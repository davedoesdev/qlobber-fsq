/*eslint brace-style: "error"*/
const path = require('path');
const child_process = require('child_process');
const crypto = require('crypto');
const os = require('os');
const { Readable } = require('stream');
const { times } = require('async');
const { QlobberFSQ } = require('..');
const { Disruptor, DisruptorWriteStream } = require('shared-memory-disruptor');
const { MPFSQBase } = require('./mpfsq_base.js');

let expect;
before(async () => {
    ({ expect } = await import('chai'));
});

class RandomStream extends Readable {
    constructor(size, i) {
        super();
        this.size = size;
        this.i = i;
        this.hash = crypto.createHash('sha256');
        this.reading = false;
    }

    _read() {
        if (this.reading) {
            return;
        }
        this.reading = true;

        if (this.size === 0) {
            return this.push(null);
        }

        crypto.randomBytes(this.size, (err, buf) => {
            this.reading = false;
            if (err) {
                return this.emit('error', err);
            }
            this.size -= buf.length;
            this.hash.update(buf);
            if (this.size === 0) {
                this.digest = this.hash.digest('hex');
            }
            if (this.push(buf)) {
                setImmediate(() => this._read());
            }
        });
    }
}

class MPFSQ extends MPFSQBase {
    constructor(options) {
        options = Object.assign({}, options, {
            direct: true,
            hash: true,
            // We have to set handler_concurrency to Infinity so all consumers are reading
            // from the disruptor. Otherwise the publisher won't write it all if there's no
            // space in the disruptor and one or more consumers aren't handling the message
            // yet because they're handling a different message.
            // i.e. It would be a deadlock because two consumers are reading different messages
            // and so both would not be fully read to the end.
            handler_concurrency: Infinity
        });

        super(child_process.fork(path.join(__dirname, 'mpfsq', 'mpfsq.js'),
                                 [Buffer.from(JSON.stringify(options)).toString('hex')]));
    }
}

describe('stream across processes', function () {
    const num_elements = 512 * 1024;

    class DirectHandler {
        constructor(options) {
            this.options = options;
            this.streams = new Map();
        }

        get_stream_for_publish(filename, unused_direct) {
            const d = new Disruptor(`/${crypto.createHash('sha256').update(filename).digest('hex')}`,
                                    num_elements,
                                    1,
                                    this.options.total,
                                    0,
                                    true,
                                    false);
            const ws = new DisruptorWriteStream(d);
            ws.filename = filename;
            this.streams.set(filename, ws);
            return ws;
        }

        get_stream_for_subscriber(unused_filename) {
            throw new Error('should not be called');
        }

        publish_stream_destroyed(filename, stream) {
            this.streams.delete(filename);
            stream.disruptor.release();
        }

        publish_stream_expired(filename) {
            const s = this.streams.get(filename);
            if (s) {
                s.destroy(new Error('expired'));
            }
        }

        subscriber_stream_destroyed(unused_filename, unused_stream) {
            throw new Error('should not be called');
        }

        subscriber_stream_ignored(unused_filename) {
            // we're not being used to subscribe, so no need to mark
        }
    }

    it('single stream', function (done) {
        const fsq = new QlobberFSQ({
            direct_handler: new DirectHandler({ total: 1 })
        });

        fsq.on('start', function () {
            const child_fsq = new MPFSQ({
                num_elements,
                total: 1,
                index: 0
            });
            const rnd_stream = new RandomStream(1024 * 1024);

            child_fsq.on('start', function () {
                this.subscribe('foo', function (hash, unused_info, unused_cb) {
                    expect(hash).to.equal(rnd_stream.digest);
                    this.stop_watching(function () {
                        fsq.stop_watching(done);
                    });
                }, function (err) {
                    if (err) {
                        return done(err);
                    }
                
                    const ws = fsq.publish('foo', { direct: true }, function (err) {
                        if (err) {
                            return done(err);
                        }
                    });
                    rnd_stream.pipe(ws);
                });
            });
        });
    });

    it('multiple streams', function (done) {
        this.timeout(60000);

        const num_streams = 4;
        const num_children = os.cpus().length;

        const fsq = new QlobberFSQ({
            direct_handler: new DirectHandler({ total: num_children })
        });

        fsq.on('start', function () {
            const rnd_streams = [];
            for (let i = 0; i < num_streams; ++i) {
                rnd_streams.push(new RandomStream(1024 * 1024, i));
            }

            let count = 0;
            let child_fsqs;
            function check(hash, i) {
                expect(hash).to.equal(rnd_streams[i].digest);
                // Note we don't subscribe one child below
                if (++count === num_streams * (num_children - 1)) {
                    times(num_children, function (i, next) {
                        child_fsqs[i].stop_watching(next);
                    }, function (err) {
                        if (err) {
                            return done(err);
                        }
                        fsq.stop_watching(done);
                    });
                }
            }

            times(num_children, function (i, next) {
                new MPFSQ({
                    num_elements,
                    total: num_children,
                    index: i
                }).on('start', function () {
                    if (i === 0) {
                        // Test we don't wait for subscribers which aren't
                        // interested in the topic
                        return next(null, this);
                    }
                    this.subscribe('foo.*', function (hash, info, cb) {
                        check(hash, parseInt(info.topic.split('.')[1]));
                        cb();
                    }, function () {
                        next(null, this);
                    });
                });
            }, function (err, fsqs) {
                if (err) {
                    return done(err);
                }
                child_fsqs = fsqs;

                times(num_streams, function (i, next) {
                    rnd_streams[i].pipe(fsq.publish(`foo.${i}`, { direct: true }, next));
                });
            });
        });
    });

    it('multiple streams without subscribers', function (done) {
        this.timeout(60000);

        const num_streams = 4;
        const num_children = os.cpus().length;

        const fsq = new QlobberFSQ({
            direct_handler: new DirectHandler({ total: num_children })
        });

        fsq.on('start', function () {
            let count = 0;
            let child_fsqs;
            function check(err) {
                expect(err.message).to.equal('no consumers');
                expect(this.destroyed).to.be.true;
                // Note we don't subscribe one child below
                if (++count === num_streams) {
                    times(num_children, function (i, next) {
                        child_fsqs[i].stop_watching(next);
                    }, function (err) {
                        if (err) {
                            return done(err);
                        }
                        fsq.stop_watching(done);
                    });
                }
            }

            times(num_children, function (i, next) {
                new MPFSQ({
                    num_elements,
                    total: num_children,
                    index: i
                }).on('start', function () {
                    next(null, this);
                });
            }, function (err, fsqs) {
                if (err) {
                    return done(err);
                }
                child_fsqs = fsqs;

                times(num_streams, function (i, next) {
                    fsq.publish(`foo.${i}`, { direct: true }, next)
                        .on('error', check)
                        .write(Buffer.alloc(1024 * 1024));
                });
            });
        });
    });
});
