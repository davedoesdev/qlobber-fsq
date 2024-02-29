/*global fsq: true,
         fs: false,
         get_message_files: false,
         expect: false,
         msg_dir: false,
         QlobberFSQ: false,
         fsq_dir: false,
         default_options: false,
         check_empty: false,
         async: false,
         wu: false,
         path: false,
         lsof: false,
         rimraf: false,
         ignore_ebusy: false,
         util: false,
         single_supported: false,
         os: false,
         argv: false*/
"use strict";

const crypto = require('crypto');

/*var orig_error = console.error;

console.error = function wup()
{
    console.log("WUP");
    console.error = orig_error;
    console.trace();
    console.error = wup;
    return orig_error.apply(this, arguments);
};*/

//process.on('uncaughtException', console.error);

function read_all(s, cb)
{
    var bufs = [];

    s.on('end', function ()
    {
        cb(Buffer.concat(bufs));
    });

    s.on('readable', function ()
    {
        while (true) // eslint-disable-line no-constant-condition
        {
            var data = this.read();
            if (data === null) { break; }
            bufs.push(data);
        }
    });
}

function test(getdents_size, use_disruptor, ephemeral)
{
describe('qlobber-fsq (getdents_size=' + getdents_size + ', use_disruptor=' + use_disruptor + ', ephemeral=' + ephemeral + ')', function ()
{
    this.timeout(60 * 1000);

    var test_options = {},
        Disruptor;

    function maybe_ephemeral(fsq)
    {
        if (!ephemeral) { return; }

        var orig_publish = fsq.publish;

        fsq.publish = function (topic, payload, options, cb)
        {
            if ((typeof payload !== 'string') && !Buffer.isBuffer(payload))
            {
                cb = options;
                options = payload;
                payload = undefined;
            }

            if (typeof options === 'function')
            {
                cb = options;
                options = undefined;
            }

            options = options || {};

            options = Object.assign({}, options);
            options.ephemeral = true;

            var r = orig_publish.call(this, topic, payload, options, cb);

            if ((typeof payload !== 'string') &&
                (!Buffer.isBuffer(payload)) &&
                !options.direct)
            {
                expect(r.ephemeral_size).to.be.above(0);
            }

            return r;
        };
    }

    function make_fsq(n, i, extra_options)
    {
        var r = new QlobberFSQ(Object.assign({},
                                             default_options,
                                             test_options,
                                             extra_options));
        r.N = n;
        r.I = i;
        maybe_ephemeral(r);

        if (use_disruptor)
        {
            const make_buckets = () =>
            {
                for (var b = 0; b < r.num_buckets; b += 1)
                {
                     new Disruptor('/test' + b,
                                   20 * 1024,
                                   2048,
                                   n,
                                   0,
                                   true,
                                   false).release();
                }
            };

            if (i === 0)
            {
                make_buckets();
            }
            else if ((i === 1) && !fsq.stopped)
            {
                var orig_on = r.on,
                    fsq_stopped = false,
                    fsq_started = false,
                    onstart = null;

                const check = () =>
                {
                    if (fsq_stopped && fsq_started && onstart)
                    {
                        process.nextTick(function ()
                        {
                            onstart.call(r);
                        });
                    }
                };

                fsq.stop_watching(function ()
                {
                    fsq_stopped = true;
                    check();
                });

                fsq = new QlobberFSQ(Object.assign({},
                                     default_options,
                                     test_options));
                fsq.N = n;
                fsq.I = 0;
                maybe_ephemeral(fsq);

                make_buckets();

                ignore_ebusy(fsq);

                fsq.on('start', function ()
                {
                    fsq_started = true;
                    check();
                });

                r.on = function (evname, f)
                {
                    if (evname !== 'start')
                    {
                        return orig_on.call(this, evname, f);
                    }

                    orig_on.call(this, 'start', function ()
                    {
                        onstart = f;
                        check();
                    });
                };
            }
        }

        return r;
    }

    if (getdents_size > 0)
    {
        require('../lib/process_all_getdents'); // account for file handle
    }

    if (use_disruptor)
    {
        Disruptor = require('shared-memory-disruptor').Disruptor;
    }

    beforeEach(function (cb)
    {
        if (getdents_size > 0)
        {
            test_options.getdents_size = getdents_size;
        }

        if (use_disruptor)
        {
            test_options.get_disruptor = function (bucket)
            {
                return new Disruptor('/test' + bucket,
                                     20 * 1024,
                                     2048,
                                     this.N,
                                     this.I,
                                     false,
                                     false);
            };
        }

        if ((getdents_size > 0) || use_disruptor)
        {
            return fsq.stop_watching(function ()
            {
                fsq = make_fsq(1, 0);

                if (getdents_size > 0)
                {
                    fsq.on('getdents_disabled', function (err)
                    {
                        cb(err);
                    });
                }

                ignore_ebusy(fsq);
                fsq.on('start', cb);
            });
        }

        cb();
    });

    var orig_ftruncate, orig_rename, orig_close, orig_flock;

    function restore()
    {
        fsq._fs.ftruncate = orig_ftruncate;
        fsq._fs.rename = orig_rename;
        fsq._fs.close = orig_close;
        fsq._fsext.flock = orig_flock;
    }

    beforeEach(function ()
    {
        orig_ftruncate = fsq._fs.ftruncate;
        orig_rename = fsq._fs.rename;
        orig_close = fsq._fs.close;
        orig_flock = fsq._fsext.flock;

        var busied_ftruncate = false,
            busied_rename = false,
            busied_close = false,
            busied_flock = false;

        fsq._fs.ftruncate = function (fd, size, cb)
        {
            if (busied_ftruncate)
            {
                busied_ftruncate = false;
                return orig_ftruncate.apply(this, arguments);
            }

            busied_ftruncate = true;
            cb({ code: 'EBUSY' });
        };

        fsq._fs.rename = function (src, dest, cb)
        {
            if (busied_rename)
            {
                busied_rename = false;
                return orig_rename.apply(this, arguments);
            }

            busied_rename = true;
            cb({ code: 'EBUSY' });
        };

        // fs.WriteStream calls fs.close when it ends so if we're not using
        // fs-ext then don't overwrite fs.close otherwise publish will error
        if (!single_supported)
        {
            return;
        }

        fsq._fs.close = function (fd, cb)
        {
            if (busied_close)
            {
                busied_close = false;
                return orig_close.apply(this, arguments);
            }

            busied_close = true;
            cb({ code: 'EBUSY' });
        };

        fsq._fsext.flock = function (fd, type, cb)
        {
            if (busied_flock)
            {
                busied_flock = false;
                return orig_flock.apply(this, arguments);
            }

            busied_flock = true;
            cb({ code: 'EBUSY' });
        };
    });

    afterEach(restore);

    it('should subscribe and publish to a simple topic', function (done)
    {
        var pub_info;

        fsq.subscribe('foo', function handler(data, info, cb)
        {
            expect(info.topic).to.equal('foo');
            expect(info.single).to.equal(false);
            expect(info.direct).to.equal(false);
            expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
            expect(info.fname.lastIndexOf(Buffer.from('foo').toString('hex') + '@', 0)).to.equal(0);
            expect(info.topic_path).to.equal(undefined);
            if (info.data !== undefined)
            {
                pub_info.data = info.data;
            }
            if (info.new !== undefined)
            {
                pub_info.new = info.new;
            }
            expect(info).to.eql(pub_info);
            expect(data.toString('utf8')).to.equal('bar');
            expect(cb.num_handlers).to.equal(1);

            fs.readFile(info.path, function (err, data)
            {
                if (ephemeral)
                {
                    expect(err.code).to.equal('ENOENT');
                }
                else
                {
                    if (err) { return done(err); }
                    expect(data.toString()).to.equal('\u0000bar');
                }
                done();
            });
        });

        fsq.publish('foo', 'bar', function (err, info)
        {
            if (err) { done(err); }
            pub_info = info;
        });
    });

    it('should construct received data only once', function (done)
    {
        var the_data = { foo: 0.435, bar: 'hello' },
            called1 = false,
            called2 = false,
            received_data;

        fsq.subscribe('test', function (data, info, cb)
        {
            expect(info.topic).to.equal('test');
            expect(JSON.parse(data)).to.eql(the_data);

            if (received_data)
            {
                expect(data === received_data).to.equal(true);
            }
            else
            {
                received_data = data;
            }

            called1 = true;

            if (called1 && called2)
            {
                cb(null, done);
            }
            else
            {
                cb();
            }
        });

        fsq.subscribe('test', function (data, info, cb)
        {
            expect(info.topic).to.equal('test');
            expect(JSON.parse(data)).to.eql(the_data);

            if (received_data)
            {
                expect(data === received_data).to.equal(true);
            }
            else
            {
                received_data = data;
            }

            called2 = true;

            if (called1 && called2)
            {
                cb(null, done);
            }
            else
            {
                cb();
            }
        });

        fsq.publish('test', JSON.stringify(the_data), function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should support more than 10 subscribers', function (done)
    {
        var the_data = { foo: 0.435, bar: 'hello' },
            counter = 11,
            received_data,
            a = [],
            i;

        function subscribe(cb)
        {
            fsq.subscribe('test', function (data, info, cb)
            {
                expect(info.topic).to.equal('test');
                expect(JSON.parse(data)).to.eql(the_data);

                if (received_data)
                {
                    expect(data === received_data).to.equal(true);
                }
                else
                {
                    received_data = data;
                }

                counter -= 1;

                if (counter === 0)
                {
                    cb(null, done);
                }
                else
                {
                    cb();
                }
            }, cb);
        }

        for (i = counter; i > 0; i -= 1)
        {
            a.push(subscribe);
        }

        async.parallel(a, function (err)
        {
            if (err) { return done(err); }

            fsq.publish('test', JSON.stringify(the_data), function (err)
            {
                if (err) { done(err); }
            });
        });
    });

    it('should support more than 10 subscribers with same handler', function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { dedup: false }),
                the_data = { foo: 0.435, bar: 'hello' },
                counter = 11,
                received_data,
                a = [],
                i;

            ignore_ebusy(fsq2);

            function handler(data, info, cb)
            {
                expect(info.topic).to.equal('test');
                expect(JSON.parse(data)).to.eql(the_data);

                if (received_data)
                {
                    expect(data === received_data).to.equal(true);
                }
                else
                {
                    received_data = data;
                }

                counter -= 1;

                if (counter === 0)
                {
                    cb(null, function (err)
                    {
                        fsq2.stop_watching(function ()
                        {
                            done(err);
                        });
                    });
                }
                else
                {
                    cb();
                }
            }

            function subscribe(cb)
            {
                fsq2.subscribe('test', handler, cb);
            }

            for (i = counter; i > 0; i -= 1)
            {
                a.push(subscribe);
            }

            fsq2.on('start', function ()
            {
                async.parallel(a, function (err)
                {
                    if (err) { return done(err); }

                    fsq2.publish('test', JSON.stringify(the_data), function (err)
                    {
                        if (err) { done(err); }
                    });
                });
            });
        });
    });

    it('should subscribe to wildcards', function (done)
    {
        var count = 0;

        function received()
        {
            count += 1;
            if (count === 2) { done(); }
        }

        fsq.subscribe('*', function (data, info)
        {
            expect(info.topic).to.equal('foo');
            expect(data.toString('utf8')).to.equal('bar');
            received();
        });

        fsq.subscribe('#', function (data, info)
        {
            expect(info.topic).to.equal('foo');
            expect(data.toString('utf8')).to.equal('bar');
            received();
        });

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should only call each handler once', function (done)
    {
        var handler = function (data, info)
        {
            expect(info.topic).to.equal('foo');
            expect(data.toString('utf8')).to.equal('bar');
            done();
        };

        fsq.subscribe('*', handler);
        fsq.subscribe('#', handler);

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should be able to disable handler dedup', function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { dedup: false }),
                count_multi = 0,
                count_single = 0;

            ignore_ebusy(fsq2);

            function handler(data, info, cb)
            {
                expect(info.topic).to.equal('foo');
                expect(data.toString('utf8')).to.equal('bar');

                cb(null, function (err)
                {
                    if (info.single)
                    {
                        count_single += 1;
                    }
                    else
                    {
                        count_multi += 1;
                    }

                    if ((count_single === (single_supported ? 1 : 0)) && 
                        (count_multi === 2))
                    {
                        fsq2.stop_watching(function ()
                        {
                            done(err);
                        });
                    }
                    else 
                    {
                        if ((count_single > 1) || (count_multi > 2))
                        {
                            throw new Error('called too many times');
                        }
                    }
                });
            }

            fsq2.on('start', function ()
            {
                fsq2.subscribe('*', handler);
                fsq2.subscribe('#', handler);

                fsq2.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });

                fsq2.publish('foo', 'bar', { single: true }, function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    });

    it('should call all handlers on a topic for pubsub', function (done)
    {
        var count = 0;

        function received()
        {
            count += 1;
            if (count === 2) { done(); }
        }

        function handler(data, info)
        {
            expect(info.topic).to.equal('foo');
            expect(data.toString('utf8')).to.equal('bar');
            received();
        }

        fsq.subscribe('foo', function () { handler.apply(this, arguments); });
        fsq.subscribe('foo', function () { handler.apply(this, arguments); });

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    if (single_supported)
    {
        it('should support a work queue', function (done)
        {
            fsq.subscribe('foo', function (data, info, cb)
            {
                expect(info.topic).to.equal('foo');
                expect(info.single).to.equal(true);
                expect(info.direct).to.equal(false);
                expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
                expect(info.fname.lastIndexOf(Buffer.from('foo').toString('hex') + '@', 0)).to.equal(0);
                expect(data.toString('utf8')).to.equal('bar');

                fs.stat(info.path, function (err)
                {
                    expect(err).to.equal(null);

                    fs.open(info.path, 'r+', function (err, fd)
                    {
                        expect(err).to.equal(null);

                        orig_flock(fd, 'exnb', function (err)
                        {
                            expect(err.code).to.be.oneOf(['EAGAIN', 'EWOULDBLOCK']);

                            orig_close(fd, function (err)
                            {
                                expect(err).to.equal(null);

                                cb(null, function ()
                                {
                                    fs.stat(info.fname, function (err)
                                    {
                                        expect(err.code).to.equal('ENOENT');
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });

            fsq.publish('foo', 'bar', { single: true }, function (err)
            {
                if (err) { done(err); }
            });
        });
    }

    it('should guard against calling subscribe callback twice', function (done)
    {
        fsq.on('warning', function (err)
        {
            if (err && (err.code !== 'EBUSY'))
            {
                throw new Error('should not be called');
            }
        });

        fsq.subscribe('foo', function (data, info, cb)
        {
            expect(info.single).to.equal(single_supported);

            cb(null, function (err)
            {
                if (err) { return done(err); }

                setTimeout(function ()
                {
                    cb(null, done);
                }, 2000);
            });
        }, function (err)
        {
            if (err) { return done(err); }

            fsq.publish('foo', 'bar', { single: single_supported }, function (err)
            {
                if (err) { done(err); }
            });
        });
    });

    if (single_supported)
    {
        it('should only give work to one worker', function (done)
        {
            this.timeout(30000);

            var fsq2 = make_fsq(2, 1),
                called = false;

            ignore_ebusy(fsq2);

            function handler (data, info, cb)
            {
                expect(called).to.equal(false);
                called = true;

                expect(info.topic).to.equal('foo');
                expect(info.single).to.equal(true);
                expect(data.toString('utf8')).to.equal('bar');

                setTimeout(function ()
                {
                    cb(null, function (err)
                    {
                        fsq2.stop_watching(function ()
                        {
                            done(err);
                        });
                    });
                }, 2000);
            }

            fsq.subscribe('foo', function () { handler.apply(this, arguments); });
            fsq.subscribe('foo', function () { handler.apply(this, arguments); });
            
            fsq2.subscribe('foo', function () { handler.apply(this, arguments); });
            fsq2.subscribe('foo', function () { handler.apply(this, arguments); });

            fsq2.on('start', function ()
            {
                fsq.publish('foo', 'bar', { single: true }, function (err)
                {
                    if (err) { done(err); }
                });
            });
        });

        it('should put work back on the queue', function (done)
        {
            var count = 0;

            fsq.subscribe('foo', function (data, info, cb)
            {
                count += 1;

                if (count === 1)
                {
                    cb('dummy failure');
                }
                else
                {
                    cb(null, done);
                }
            });

            fsq.publish('foo', 'bar', { single: true }, function (err)
            {
                if (err) { done(err); }
            });
        });
    }

    it('should allow handlers to refuse work', function (done)
    {
        fsq.stop_watching(function ()
        {
            function handler1()
            {
                throw new Error('should not be called');
            }

            var fsq2; 

            function handler2(data, info, cb)
            {
                cb(null, function (err)
                {
                    fsq2.stop_watching(function ()
                    {
                        done(err);
                    });
                });
            }

            fsq2 = make_fsq(1, 0,
            {
                filter: function (info, handlers, cb)
                {
                    expect(info.topic).to.equal('foo');
                    handlers.delete(handler1);
                    cb(null, true, handlers);
                }
            });

            ignore_ebusy(fsq2);

            fsq2.subscribe('foo', handler1);
            fsq2.subscribe('foo', handler2);

            fsq2.on('start', function ()
            {
                fsq2.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    });

    it('should not allow filters to modify qlobber matches', function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2, count = 0;

            function handler(data, info, cb)
            {
                expect(count).to.equal(2);

                cb(null, function (err)
                {
                    fsq2.stop_watching(function ()
                    {
                        done(err);
                    });
                });
            }

            fsq2 = make_fsq(1, 0,
            {
                filter: function (info, handlers, cb)
                {
                    expect(info.topic).to.equal('foo');

                    if (++count === 1)
                    {
                        handlers.delete(handler);
                        return cb(null, false);
                    }

                    cb(null, true, handlers);
                }
            });

            ignore_ebusy(fsq2);

            fsq2.subscribe('foo', handler);

            fsq2.on('start', function ()
            {
                fsq2.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    });

    it('should be able to set filter by property', function (done)
    {
        function handler1()
        {
            throw new Error('should not be called');
        }

        function handler2(data, info, cb)
        {
            cb(null, done);
        }

        fsq.filters.push(function (info, handlers, cb)
        {
            expect(info.topic).to.equal('foo');
            handlers.delete(handler1);
            cb(null, true, handlers);
        });

        fsq.subscribe('foo', handler1);
        fsq.subscribe('foo', handler2);

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should be able to pass filtered handlers as iterator (Set)', function (done)
    {
        function handler1()
        {
            throw new Error('should not be called');
        }

        function handler2(data, info, cb)
        {
            cb(null, done);
        }

        fsq.filters.push(function (info, handlers, cb)
        {
            expect(info.topic).to.equal('foo');
            cb(null, true, wu(handlers).filter(function (h)
            {
                return h !== handler1;
            }));
        });

        fsq.subscribe('foo', handler1);
        fsq.subscribe('foo', handler2);

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should be able to pass filtered handlers as iterator (Array)', function (done)
    {
        fsq.stop_watching(function ()
        {
            function handler1()
            {
                throw new Error('should not be called');
            }

            var fsq2; 

            function handler2(data, info, cb)
            {
                cb(null, function (err)
                {
                    fsq2.stop_watching(function ()
                    {
                        done(err);
                    });
                });
            }

            fsq2 = make_fsq(1, 0,
            {
                filter: function (info, handlers, cb)
                {
                    expect(info.topic).to.equal('foo');
                    cb(null, true, wu(handlers).filter(function (h)
                    {
                        return h !== handler1;
                    }));
                },
                dedup: false
            });

            ignore_ebusy(fsq2);

            fsq2.subscribe('foo', handler1);
            fsq2.subscribe('foo', handler2);

            fsq2.on('start', function ()
            {
                fsq2.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    });

    it('should support multiple filters', function (done)
    {
        function handler1()
        {
            throw new Error('should not be called');
        }

        function handler2()
        {
            throw new Error('should not be called');
        }

        function handler3(data, info, cb)
        {
            cb(null, done);
        }

        fsq.filters.push(
            function (info, handlers, cb)
            {
                expect(info.topic).to.equal('foo');
                handlers.delete(handler1);
                cb(null, true, handlers);
            },

            function (info, handlers, cb)
            {
                expect(info.topic).to.equal('foo');
                handlers.delete(handler2);
                cb(null, true, handlers);
            }
        );

        fsq.subscribe('foo', handler1);
        fsq.subscribe('foo', handler2);
        fsq.subscribe('foo', handler3);

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should not call other filters if error', function (done)
    {
        var called = false;

        fsq.filters.push(
            function (info, handlers, cb)
            {
                expect(info.topic).to.equal('foo');
                cb(new Error('dummy'));
                if (called)
                {
                    return done();
                }
                called = true;
            },

            function (unused_info, unused_handlers, unused_cb)
            {
                throw new Error('should not be called');
            }
        );

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should not call other filters if not ready', function (done)
    {
        var called = false;

        fsq.filters.push(
            function (info, handlers, cb)
            {
                expect(info.topic).to.equal('foo');
                cb(null, false);
                if (called)
                {
                    return done();
                }
                called = true;
            },

            function (unused_info, unused_handlers, unused_cb)
            {
                throw new Error('should not be called');
            }
        );

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    if (single_supported)
    {
        it('should put work back on queue for another handler', function (done)
        {
            fsq.stop_watching(function ()
            {
                function handler(data, info, cb)
                {
                    cb('dummy failure');
                }

                var filter_called = false,
                    fsq2 = make_fsq(1, 0,
                    {
                        filter: function (info, handlers, cb)
                        {
                            expect(info.topic).to.equal('foo');
                            expect(info.single).to.equal(true);

                            if (filter_called)
                            {
                                handlers.delete(handler);
                                return cb(null, true, handlers);
                            }

                            filter_called = true;
                            cb(null, true, handlers);
                        }
                    });

                ignore_ebusy(fsq2);

                fsq2.subscribe('foo', handler);

                fsq2.subscribe('foo', function (data, info, cb)
                {
                    if (filter_called)
                    {
                        cb(null, function (err)
                        {
                            fsq2.stop_watching(function ()
                            {
                                done(err);
                            });
                        });
                    }
                    else
                    {
                        cb('dummy failure2');
                    }
                });

                fsq2.on('start', function ()
                {
                    fsq2.publish('foo', 'bar', { single: true }, function (err)
                    {
                        if (err) { done(err); }
                    });
                });
            });
        });

        it('should put work back on queue for a handler on another queue', function (done)
        {
            this.timeout(30000);

            fsq.stop_watching(function ()
            {
                function handler(data, info, cb)
                {
                    cb('dummy failure');
                }

                var filter_called = false,
                    fsq2 = make_fsq(2, 0,
                    {
                        filter: function (info, handlers, cb)
                        {
                            expect(info.topic).to.equal('foo');
                            expect(info.single).to.equal(true);

                            if (filter_called)
                            {
                                handlers.delete(handler);
                            }

                            filter_called = true;
                            cb(null, true, handlers);
                        }
                    }),
                    fsq3 = make_fsq(2, 1),
                    started2 = false,
                    started3 = false;

                ignore_ebusy(fsq2);
                ignore_ebusy(fsq3);

                fsq2.subscribe('foo', handler);

                fsq3.subscribe('foo', function (data, info, cb)
                {
                    if (filter_called)
                    {
                        cb(null, function (err)
                        {
                            fsq2.stop_watching(function ()
                            {
                                fsq3.stop_watching(function ()
                                {
                                    done(err);
                                });
                            });
                        });
                    }
                    else
                    {
                        cb('dummy failure2');
                    }
                });

                function start()
                {
                    if (!(started2 && started3)) { return; }

                    fsq2.publish('foo', 'bar', { single: true }, function (err)
                    {
                        if (err) { done(err); }
                    });
                }

                fsq2.on('start', function ()
                {
                    started2 = true;
                    start();
                });

                fsq3.on('start', function ()
                {
                    started3 = true;
                    start();
                });
            });
        });
    }

    if (!ephemeral)
    {
        it('should allow handlers to delay a message', function (done)
        {
            restore();

            fsq.stop_watching(function ()
            {
                var ready_multi = false,
                    ready_single = !single_supported,
                    got_multi = false,
                    got_single = !single_supported,
                    count = 0,
                    fsq2 = make_fsq(1, 0,
                    {
                        filter: function (info, handlers, cb)
                        {
                            expect(info.topic).to.equal('foo');

                            if (info.single)
                            {
                                ready_single = true;
                            }
                            else
                            {
                                ready_multi = true;
                            }

                            if (!single_supported)
                            {
                                expect(info.single).to.equal(false);
                            }

                            count += 1;
                            cb(null, (count % 5) === 0, handlers);
                        }
                    });

                ignore_ebusy(fsq2);

                function handler(data, info, cb)
                {
                    expect(data.toString('utf8')).to.equal('bar');

                    cb(null, function (err)
                    {
                        if (info.single)
                        {
                            expect(got_single).to.equal(false);
                            got_single = true;
                        }
                        else
                        {
                            expect(got_multi).to.equal(false);
                            got_multi = true;
                        }

                        if (!single_supported)
                        {
                            expect(info.single).to.equal(false);
                        }

                        if (got_single && got_multi && ready_single && ready_multi)
                        {
                            expect(count).to.equal(single_supported ? 10 : 5);

                            fsq2.stop_watching(function ()
                            {
                                done(err);
                            });
                        }
                    });
                }

                fsq2.subscribe('foo', handler);

                fsq2.on('start', function ()
                {
                    fsq2.publish('foo', 'bar', function (err)
                    {
                        if (err) { done(err); }
                    });

                    fsq2.publish('foo', 'bar', { single: true }, function (err)
                    {
                        if (err) { done(err); }
                    });
                });
            });
        });
    }

    it('should emit start and stop events', function (done)
    {
        this.timeout(30000);

        var fsq2 = make_fsq(2, 1);
        ignore_ebusy(fsq2);

        fsq2.on('start', function ()
        {
            fsq2.stop_watching();
            fsq2.on('stop', done);
        });
    });

    it('should support per-message time-to-live', function (done)
    {
        this.timeout(20000);

        restore();

        fsq.subscribe('foo', function ()
        {
            setTimeout(function ()
            {
                fsq.force_refresh();
                setTimeout(function ()
                {
                    check_empty(msg_dir, done, done);
                }, 500);
            }, 500);
        });

        fsq.publish('foo', 'bar', { ttl: 500 }, function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should call error function', function (done)
    {
        restore();

        fsq.on('warning', function (err)
        {
            expect(err).to.equal('dummy failure');
            done();
        });

        fsq.subscribe('foo', function (data, info, cb)
        {
            cb('dummy failure');
        });

        fsq.publish('foo', 'bar', { single : single_supported }, function (err)
        {
            if (err) { done(err); }
        });
    });

    if (single_supported)
    {
        it('should support custom polling interval', function (done)
        {
            this.timeout(30000);

            restore();

            var time, count = 0, fsq2 = make_fsq(2, 1, { poll_interval: 50 });

            ignore_ebusy(fsq2);

            fsq2.subscribe('foo', function (data, info, cb)
            {
                count += 1;

                var time2 = new Date().getTime();
                expect(time2 - time).to.be.below(900);
                time = time2;

                if (count === 10)
                {
                    cb(null, function ()
                    {
                        fsq2.stop_watching(done);
                    });
                }
                else
                {
                    cb('dummy failure');
                }
            });
     
            fsq2.on('start', function ()
            {
                time = new Date().getTime();

                fsq.publish('foo', 'bar', {single : true}, function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    }

    it('should support unsubscribing', function (done)
    {
        this.timeout(5000);

        var count = 0;

        function handler(data, info, cb)
        {
            count += 1;

            if (count  > 1)
            {
                throw new Error('should not be called');
            }

            fsq.unsubscribe('foo', handler, function ()
            {
                fsq.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });

                setTimeout(function ()
                {
                    cb(null, done);
                }, 2000);
            });
        }

        fsq.subscribe('foo', handler);
        
        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should support unsubscribing to all handlers for a topic', function (done)
    {
        this.timeout(5000);

        var count = 0;

        function handler(data, info, cb)
        {
            count += 1;

            if (count > 2)
            {
                throw new Error('should not be called');
            }

            if (count === 2)
            {
                fsq.unsubscribe('foo', undefined, function ()
                {
                    fsq.publish('foo', 'bar', function (err)
                    {
                        if (err) { done(err); }
                    });

                    setTimeout(function ()
                    {
                        cb(null, done);
                    }, 2000);
                });
            }
        }

        fsq.subscribe('foo', function (data, info, cb)
        {
            handler(data, info, cb);
        });

        fsq.subscribe('foo', function (data, info, cb)
        {
            handler(data, info, cb);
        });
        
        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should support unsubscribing to all handlers', function (done)
    {
        this.timeout(5000);

        var count = 0;

        function handler(data, info, cb)
        {
            count += 1;

            if (count > 2)
            {
                throw new Error('should not be called');
            }

            if (count === 2)
            {
                fsq.subscribe('foo2', function ()
                {
                    throw new Error('should not be called');
                });

                fsq.unsubscribe(function ()
                {
                    fsq.publish('foo', 'bar', function (err)
                    {
                        if (err) { done(err); }
                    });

                    fsq.publish('foo2', 'bar2', function (err)
                    {
                        if (err) { done(err); }
                    });

                    setTimeout(function ()
                    {
                        cb(null, done);
                    }, 2000);
                });
            }
        }

        fsq.subscribe('foo', function (data, info, cb)
        {
            handler(data, info, cb);
        });

        fsq.subscribe('foo', function (data, info, cb)
        {
            handler(data, info, cb);
        });
        
        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should support changing the default time-to-live', function (done)
    {
        this.timeout(30000);

        restore();

        fsq.stop_watching(function () // stop fsq dequeuing
        {
            var got_single = !single_supported,
                got_multi = false,
                fsq2 = make_fsq(1, 0,
                {
                    multi_ttl: 1000,
                    single_ttl: 1000
                });

            ignore_ebusy(fsq2);

            fsq2.subscribe('foo', function (data, info, cb)
            {
                cb(null, function ()
                {
                    if (info.single)
                    {
                        got_single = true;
                    }
                    else
                    {
                        got_multi = true;
                    }

                    if (got_single && got_multi)
                    {
                        setTimeout(function ()
                        {
                            fsq2.force_refresh();
                            setTimeout(function ()
                            {
                                check_empty(msg_dir, done, function ()
                                {
                                    fsq2.stop_watching(done);
                                });
                            }, 1000);
                        }, 1000);
                    }
                });
            });

            fsq2.on('start', function ()
            {
                fsq2.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });

                if (single_supported)
                {
                    fsq2.publish('foo', 'bar', { single: true }, function (err)
                    {
                        if (err) { done(err); }
                    });
                }
            });
        });
    });

    it('should publish and receive twice', function (done)
    {
        var count_multi = 0,
            count_single = single_supported ? 0 : 2;

        fsq.subscribe('foo', function (data, info, cb)
        {
            cb(null, function ()
            {
                if (info.single)
                {
                    count_single += 1;
                }
                else
                {
                    count_multi += 1;
                }

                if ((count_single === 2) && (count_multi === 2))
                {
                    done();
                }
                else if ((count_single > 2) || (count_multi > 2))
                {
                    throw new Error('called too many times');
                }
            });
        });

        async.timesSeries(2, function (n, cb)
        {
            async.eachSeries([true, false], function (single, cb)
            {
                fsq.publish('foo', 'bar', { single: single }, function (err)
                {
                    cb(err);
                });
            }, cb);
        }, function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should default to putting messages in module directory', function (done)
    {
        var fsq2 = make_fsq(2, 1, { fsq_dir: undefined });
        ignore_ebusy(fsq2);

        fsq2.subscribe('foo', function (data, info)
        {
            if (use_disruptor)
            {
                // fsq and fsq2 use same disruptors
                expect(info.new).to.be.true;
                expect(info.data.toString('utf8')).to.equal('bar');
            }
            else
            {
                throw new Error('should not be called');
            }
        });

        fsq2.subscribe('foo2', function (data, info, cb)
        {
            expect(data.toString('utf8')).to.equal('bar2');
            expect(info.path.lastIndexOf(path.join(__dirname, '..', 'fsq', 'messages'), 0)).to.equal(0);
            cb(null, function ()
            {
                fsq2.stop_watching(done);
            });
        });

        fsq2.on('start', function ()
        {
            fsq.publish('foo', 'bar', function (err)
            {
                if (err) { done(err); }

                // wait for publish so EBUSY isn't retrying while fsq is being cleaned up

                fsq2.publish('foo2', 'bar2', function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    });

    it('should publish and subscribe to messages with long topics (multi)', function (done)
    {
        var arr = [], topic;
        arr.length = 64 * 1024 + 1;
        topic = arr.join('a');

        fsq.subscribe(topic, function (data, info)
        {
            expect(info.topic).to.equal(topic);
            expect(info.single).to.equal(false);
            expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
            expect(info.fname.lastIndexOf(Buffer.from(topic).toString('hex').substr(0, fsq._split_topic_at) + '@', 0)).to.equal(0);
            expect(data.toString('utf8')).to.equal('bar');

            var topic_dir = path.dirname(path.dirname(info.topic_path));

            expect(topic_dir).to.equal(path.join(msg_dir, '..', 'topics'));

            fs.readFile(info.topic_path, function (err, split)
            {
                if (err) { return done(err); }

                expect(split.toString('utf8')).to.equal(Buffer.from(topic).toString('hex').substr(fsq._split_topic_at));

                setTimeout(function ()
                {
                    fsq.force_refresh();

                    setTimeout(function ()
                    {
                        check_empty(msg_dir, done, function ()
                        {
                            check_empty(topic_dir, done, done);
                        });
                    }, 500);
                }, 1000);
            });
        });

        fsq.publish(topic, 'bar', { ttl: 1000 }, function (err)
        {
            if (ephemeral)
            {
                expect(err.code).to.equal('buffer-too-small');
                return done();
            }

            if (err) { done(err); }
        });
    });

    if (single_supported)
    {
        it('should publish and subscribe to messages with long topics (single)', function (done)
        {
            var arr = [], topic;
            arr.length = 64 * 1024 + 1;
            topic = arr.join('a');

            fsq.subscribe(topic, function (data, info, cb)
            {
                expect(info.topic).to.equal(topic);
                expect(info.single).to.equal(true);
                expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
                expect(info.fname.lastIndexOf(Buffer.from(topic).toString('hex').substr(0, fsq._split_topic_at) + '@', 0)).to.equal(0);
                expect(data.toString('utf8')).to.equal('bar');

                var topic_dir = path.dirname(path.dirname(info.topic_path));

                expect(topic_dir).to.equal(path.join(msg_dir, '..', 'topics'));

                fs.readFile(info.topic_path, function (err, split)
                {
                    if (err) { return done(err); }

                    expect(split.toString('utf8')).to.equal(Buffer.from(topic).toString('hex').substr(fsq._split_topic_at));

                    cb(null, function ()
                    {
                        setTimeout(function ()
                        {
                            fsq.force_refresh();

                            setTimeout(function ()
                            {
                                check_empty(msg_dir, done, function ()
                                {
                                    check_empty(topic_dir, done, done);
                                });
                            }, 500);
                        }, 1000);
                    });
                });
            });

            fsq.publish(topic, 'bar', { ttl: 1000, single: true }, function (err)
            {
                if (err) { done(err); }
            });
        });
    }

    it('should be able to change when a topic file is created', function (done)
    {
        fsq.stop_watching(function ()
        {
            var topic = 'hellofromfsq',
                fsq2 = make_fsq(1, 0, { split_topic_at: 5, });

            ignore_ebusy(fsq2);

            fsq2.on('start', function ()
            {
                fsq2.subscribe(topic, function (data, info)
                {
                    expect(info.topic).to.equal(topic);
                    expect(info.single).to.equal(false);
                    expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
                    expect(info.fname.lastIndexOf(Buffer.from(topic).toString('hex').substr(0, ephemeral ? undefined : 5) + '@', 0)).to.equal(0);
                    expect(data.toString('utf8')).to.equal('bar');

                    if (ephemeral)
                    {
                        expect(info.topic_path).to.be.undefined;
                        return check_empty(msg_dir, done, function ()
                        {
                            fsq2.stop_watching(done);
                        });
                    }

                    var topic_dir = path.dirname(path.dirname(info.topic_path));

                    expect(topic_dir).to.equal(path.join(msg_dir, '..', 'topics'));

                    fs.readFile(info.topic_path, function (err, split)
                    {
                        if (err) { return done(err); }
                        expect(split.toString('utf8')).to.equal(Buffer.from(topic).toString('hex').substr(5));

                        setTimeout(function ()
                        {
                            fsq2.force_refresh();

                            setTimeout(function ()
                            {
                                fsq2.stop_watching(function ()
                                {
                                    check_empty(msg_dir, done, function ()
                                    {
                                        check_empty(topic_dir, done, done);
                                    });
                                });
                            }, 5 * 1000);
                        }, 1000);
                    });
                });

                fsq2.publish(topic, 'bar', { ttl: 1000 }, function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    });

    it('should not read multi-worker messages which already exist', function (done)
    {
        this.timeout (10 * 1000);

        fsq.stop_watching(function ()
        {
            fsq.publish('foo', 'bar', function (err)
            {
                if (err) { return done(err); }

                var fsq2 = make_fsq(1, 0);
                ignore_ebusy(fsq2);

                fsq2.subscribe('foo', function ()
                {
                    done('should not be called');
                });

                fsq2.on('start', function ()
                {
                    setTimeout(function ()
                    {
                        fsq2.stop_watching(done);
                    }, 5 * 1000);
                });
            });
        });
    });

    if (single_supported)
    {
        it('should read single worker messages which already exist', function (done)
        {
            fsq.stop_watching(function ()
            {
                fsq.publish('foo', 'bar', { single: true }, function (err)
                {
                    if (err) { return done(err); }

                    var fsq2 = make_fsq(1, 0);
                    ignore_ebusy(fsq2);

                    fsq2.subscribe('foo', function (data, info, cb)
                    {
                        cb(null, function ()
                        {
                            fsq2.stop_watching(done);
                        });
                    });
                });
            });
        });

        it('should read single worker messages which already exist after subscribing', function (done)
        {
            fsq.stop_watching(function ()
            {
                fsq.publish('foo', 'bar', { single: true }, function (err)
                {
                    if (err) { return done(err); }

                    var fsq2 = make_fsq(1, 0);
                    ignore_ebusy(fsq2);

                    fsq2.on('start', function ()
                    {
                        fsq2.subscribe('foo', function (data, info, cb)
                        {
                            cb(null, function ()
                            {
                                fsq2.stop_watching(done);
                            });
                        });
                    });
                });
            });
        });
    }

    it('should support streaming interfaces', function (done)
    {
        var stream_multi,
            stream_single,
            stream_file,
            sub_multi_called = false,
            sub_single_called = !single_supported,
            pub_multi_called = false,
            pub_single_called = false;

        function handler(stream, info, cb)
        {
            var hash = crypto.createHash('sha256'),
                len = 0;

            stream.on('readable', function ()
            {
                let chunk;
                while (chunk = stream.read()) // eslint-disable-line no-cond-assign
                {
                    len += chunk.length;
                    hash.update(chunk);
                }
            });

            stream.on('end', function ()
            {
                expect(len).to.equal(1024 * 1024);
                expect(hash.digest('hex')).to.equal('268e1a23a9da868b62b12e020061c98449568c4af9cf9070c8738fe1b457ed9c');

                cb(null, function ()
                {
                    if (info.single)
                    {
                        expect(sub_single_called).to.equal(false);
                        sub_single_called = true;
                    }
                    else
                    {
                        expect(sub_multi_called).to.equal(false);
                        sub_multi_called = true;
                    }

                    if (pub_multi_called && pub_single_called &&
                        sub_multi_called && sub_single_called)
                    {
                        done();
                    }
                });
            });
        }

        handler.accept_stream = true;
        
        fsq.subscribe('foo', handler);
        
        function published(err)
        {
            if (err) { return done(err); }

            if (pub_multi_called && pub_single_called &&
                sub_multi_called && sub_single_called)
            {
                done();
            }
        }

        stream_multi = fsq.publish('foo', function (err)
        {
            expect(pub_multi_called).to.equal(false);
            pub_multi_called = true;
            published(err);
        });

        stream_single = fsq.publish('foo', { single: true }, function (err)
        {
            expect(pub_single_called).to.equal(false);
            pub_single_called = true;
            published(err);
        });

        stream_file = fs.createReadStream(path.join(__dirname, 'fixtures', 'random'));
        stream_file.pipe(stream_multi);
        stream_file.pipe(stream_single);
    });

    it('should support direct streaming', function (done)
    {
        var stream_mod = require('stream');

        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { direct_handler: new class
            {
                constructor()
                {
                    this.streams = new Map();
                    this.gsfp_called = false;
                    this.gsfs_called = false;
                    this.psd_called = false;
                    this.pse_called = false;
                    this.ssd_called = false;
                }

                get_stream_for_publish(filename, direct)
                {
                    this.gsfp_called = true;
                    expect(direct).to.equal('something truthy');
                    const r = new stream_mod.PassThrough();
                    this.streams.set(filename, r);
                    return r;
                }

                get_stream_for_subscribers(filename)
                {
                    this.gsfs_called = true;
                    return this.streams.get(filename);
                }
                
                publish_stream_destroyed(unused_filename, unused_stream)
                {
                    this.psd_called = true;
                }

                publish_stream_expired(unused_filename)
                {
                    this.pse_called = true;
                }

                subscriber_stream_destroyed(filename, stream)
                {
                    this.ssd_called = true;
                    const s = this.streams.get(filename);
                    expect(s).to.equal(stream);
                    this.streams.delete(filename);
                }

                subscriber_stream_ignored(unused_filename)
                {
                    throw new Error('should not be called');
                }
            }()});

            fsq2.on('start', function ()
            {
                var stream_direct,
                    stream_file,
                    sub_called = false,
                    pub_called = false;

                function check()
                {
                    if (pub_called && sub_called)
                    {
                        expect(fsq2._direct_handler.gsfp_called).to.be.true;
                        expect(fsq2._direct_handler.gsfs_called).to.be.true;
                        expect(fsq2._direct_handler.psd_called).to.be.false;
                        expect(fsq2._direct_handler.pse_called).to.be.false;
                        expect(fsq2._direct_handler.ssd_called).to.be.true;
                        fsq2.stop_watching(done);
                    }
                }

                function handler(stream, info, cb)
                {
                    var hash = crypto.createHash('sha256'),
                        len = 0;

                    expect(info.single).to.equal(false);
                    expect(info.direct).to.equal(true);

                    stream.on('readable', function ()
                    {
                        let chunk;
                        while (chunk = stream.read()) // eslint-disable-line no-cond-assign
                        {
                            len += chunk.length;
                            hash.update(chunk);
                        }
                    });

                    stream.on('end', function ()
                    {
                        expect(len).to.equal(1024 * 1024);
                        expect(hash.digest('hex')).to.equal('268e1a23a9da868b62b12e020061c98449568c4af9cf9070c8738fe1b457ed9c');

                        cb(null, function ()
                        {
                            expect(sub_called).to.equal(false);
                            sub_called = true;
                            check();
                        });
                    });
                }

                handler.accept_stream = true;
                
                fsq2.subscribe('foo', handler);
                
                stream_direct = fsq2.publish('foo', { direct: 'something truthy' }, function (err)
                {
                    if (err) { return done(err); }
                    expect(pub_called).to.equal(false);
                    pub_called = true;
                    check();
                });

                stream_file = fs.createReadStream(path.join(__dirname, 'fixtures', 'random'));
                stream_file.pipe(stream_direct);
            });
        });
    });

    it('should support direct streaming with readable', function (done)
    {
        var stream_mod = require('stream');

        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { direct_handler: new class
            {
                constructor()
                {
                    this.streams = new Map();
                    this.gsfp_called = false;
                    this.gsfs_called = false;
                    this.psd_called = false;
                    this.pse_called = false;
                    this.ssd_called = false;
                }

                get_stream_for_publish(filename, direct)
                {
                    this.gsfp_called = true;
                    expect(direct).to.be.an.instanceof(stream_mod.Readable);
                    this.streams.set(filename, direct);
                    return direct;
                }

                get_stream_for_subscribers(filename)
                {
                    this.gsfs_called = true;
                    return this.streams.get(filename);
                }

                publish_stream_destroyed(unused_filename, unused_stream)
                {
                    this.psd_called = true;
                }

                publish_stream_expired(unused_filename)
                {
                    this.pse_called = true;
                }

                subscriber_stream_destroyed(filename, stream)
                {
                    this.ssd_called = true;
                    const s = this.streams.get(filename);
                    expect(s).to.equal(stream);
                    this.streams.delete(filename);
                }
            }()});

            fsq2.on('start', function ()
            {
                var stream_direct,
                    stream_file,
                    sub_called = false,
                    pub_called = false;

                function check()
                {
                    if (pub_called && sub_called)
                    {
                        expect(fsq2._direct_handler.gsfp_called).to.be.true;
                        expect(fsq2._direct_handler.gsfs_called).to.be.true;
                        expect(fsq2._direct_handler.psd_called).to.be.false;
                        expect(fsq2._direct_handler.pse_called).to.be.false;
                        expect(fsq2._direct_handler.ssd_called).to.be.true;
                        fsq2.stop_watching(done);
                    }
                }

                function handler(stream, info, cb)
                {
                    var hash = crypto.createHash('sha256'),
                        len = 0;

                    expect(stream).to.equal(stream_file);
                    expect(info.single).to.equal(false);
                    expect(info.direct).to.equal(true);

                    stream.on('readable', function ()
                    {
                        let chunk;
                        while (chunk = stream.read()) // eslint-disable-line no-cond-assign
                        {
                            len += chunk.length;
                            hash.update(chunk);
                        }
                    });

                    stream.on('end', function ()
                    {
                        expect(len).to.equal(1024 * 1024);
                        expect(hash.digest('hex')).to.equal('268e1a23a9da868b62b12e020061c98449568c4af9cf9070c8738fe1b457ed9c');

                        cb(null, function ()
                        {
                            expect(sub_called).to.equal(false);
                            sub_called = true;
                            check();
                        });
                    });
                }

                handler.accept_stream = true;
                
                fsq2.subscribe('foo', handler);
                
                stream_file = fs.createReadStream(path.join(__dirname, 'fixtures', 'random'));

                stream_direct = fsq2.publish('foo', { direct: stream_file, single: true }, function (err)
                {
                    if (err) { return done(err); }
                    expect(pub_called).to.equal(false);
                    pub_called = true;
                    check();
                });

                expect(stream_direct).to.equal(stream_file);
            });
        });
    });

    it('should pipe to more than one stream', function (done)
    {
        var stream_mod = require('stream'),
            done1 = false,
            done2 = false;

        function CheckStream()
        {
            stream_mod.Writable.call(this);

            this._hash = crypto.createHash('sha256');
            this._len = 0;

            var ths = this;

            this.on('finish', function ()
            {
                ths.emit('done',
                {
                    digest: ths._hash.digest('hex'),
                    len: ths._len
                });
            });
        }

        util.inherits(CheckStream, stream_mod.Writable);

        CheckStream.prototype._write = function (chunk, encoding, callback)
        {
            this._len += chunk.length;
            this._hash.update(chunk);
            callback();
        };

        function check(obj, cb)
        {
            expect(obj.len).to.equal(1024 * 1024);
            expect(obj.digest).to.equal('268e1a23a9da868b62b12e020061c98449568c4af9cf9070c8738fe1b457ed9c');

            if (done1 && done2)
            {
                cb(null, done);
            }
            else
            {
                cb();
            }
        }

        function handler1(stream, info, cb)
        {
            var cs = new CheckStream();

            cs.on('done', function (obj)
            {
                done1 = true;
                check(obj, cb);
            });

            stream.pipe(cs);
        }

        function handler2(stream, info, cb)
        {
            var cs = new CheckStream();

            cs.on('done', function (obj)
            {
                done2 = true;
                check(obj, cb);
            });

            stream.pipe(cs);
        }

        handler1.accept_stream = true;
        fsq.subscribe('foo', handler1);

        handler2.accept_stream = true;
        fsq.subscribe('foo', handler2);

        fs.createReadStream(path.join(__dirname, 'fixtures', 'random')).pipe(
        fsq.publish('foo', function (err)
        {
            if (err) { return done(err); }
        }));
    });

    it('should not call the same handler with stream and data', function (done)
    {
        function handler(stream, info, cb)
        {
            expect(Buffer.isBuffer(stream)).to.equal(false);

            var hash = crypto.createHash('sha256'),
                len = 0;

            stream.on('readable', function ()
            {
                let chunk;
                while (chunk = stream.read()) // eslint-disable-line no-cond-assign
                {
                    len += chunk.length;
                    hash.update(chunk);
                }
            });

            stream.on('end', function ()
            {
                expect(len).to.equal(1024 * 1024);
                expect(hash.digest('hex')).to.equal('268e1a23a9da868b62b12e020061c98449568c4af9cf9070c8738fe1b457ed9c');
                cb(null, done);
            });
        }

        handler.accept_stream = true;

        fsq.subscribe('foo', handler);

        fs.createReadStream(path.join(__dirname, 'fixtures', 'random')).pipe(
        fsq.publish('foo', function (err)
        {
            if (err) { return done(err); }
        }));
    });

    it('should use inotify to process messages straight away', function (done)
    {
        var fsq2 = make_fsq(2, 1, { poll_interval: 10 * 1000 }), time;

        ignore_ebusy(fsq2);

        fsq2.subscribe('foo', function ()
        {
            expect(new Date().getTime() - time).to.be.below(fsq2._poll_interval);
            fsq2.stop_watching(done);
        });

        fsq2.on('start', function ()
        {
            time = new Date().getTime();
            fsq2.publish('foo', 'bar');
        });
    });

    it('should be able to disable inotify', function (done)
    {
        restore();

        var fsq2 = make_fsq(2, 1,
        {
            poll_interval: 10 * 1000,
            notify: false
        }), time;

        ignore_ebusy(fsq2);

        fsq2.subscribe('foo', function ()
        {
            expect(new Date().getTime() - time).to.be.at.least(fsq2._poll_interval - 1000);
            fsq2.stop_watching(done);
        });

        fsq2.on('start', function ()
        {
            // The countdown to the next poll has already started so from here
            // it may not be poll_interval until the message is received -
            // which is why we subtract a second above.
            time = new Date().getTime();
            fsq2.publish('foo', 'bar', { ttl: 30 * 1000 });
        });
    });

    it('should be able to change the size of update stamps', function (done)
    {
        fs.stat(path.join(msg_dir, '..', 'update', 'UPDATE'), function (err, stats)
        {
            if (err) { return done(err); }
            expect(stats.size).to.equal(Math.pow(16, 2) * 32);

            fsq.stop_watching(function ()
            {
                var fsq2 = make_fsq(1, 0, { bucket_stamp_size: 64 });

                ignore_ebusy(fsq2);

                fsq2.subscribe('foo', function (data)
                {
                    expect(data.toString('utf8')).to.equal('bar');
                    fsq2.stop_watching(done);
                });

                fsq2.on('start', function ()
                {
                    fs.stat(path.join(msg_dir, '..', 'update', 'UPDATE'), function (err, stats)
                    {
                        if (err) { return done(err); }
                        expect(stats.size).to.equal(Math.pow(16, 2) * 64);
                        fsq2.publish('foo', 'bar');
                    });
                });
            });
        });
    });

    it('should be able to disable the update file', function (done)
    {
        var update_file = path.join(msg_dir, '..', 'update', 'UPDATE');

        fs.stat(update_file, function (err, stats)
        {
            if (err) { return done(err); }
            expect(stats.size).to.equal(Math.pow(16, 2) * 32);

            fsq.stop_watching(function ()
            {
                fs.unlink(update_file, function (err)
                {
                    if (err) { return done(err); }

                    var fsq2 = make_fsq(1, 0, { bucket_stamp_size: 0 });

                    ignore_ebusy(fsq2);

                    fsq2.subscribe('foo', function (data)
                    {
                        expect(data.toString('utf8')).to.equal('bar');
                        fsq2.stop_watching(done);
                    });

                    fsq2.on('start', function ()
                    {
                        fs.stat(update_file, function (err)
                        {
                            expect(err.code).to.equal('ENOENT');
                            fsq2.publish('foo', 'bar');
                        });
                    });
                });
            });
        });
    });

    it('should refresh from disk once every 10 seconds by default', function (done)
    {
        this.timeout(60000);

        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0);
            ignore_ebusy(fsq2);

            var count = 0;

            process.nextTick(function ()
            {
                var orig_process_all = fsq2._process_all;

                fsq2._process_all = function ()
                {
                    count += 1;
                    return orig_process_all.apply(this, arguments);
                };
            });

            fsq2.on('start', function ()
            {
                expect(count).to.equal(256);

                setTimeout(function ()
                {
                    expect(count).to.equal(512);
                    fsq2.stop_watching(done);
                }, 11000);
            });
        });
    });

    it('should be able to change refresh from disk interval', function (done)
    {
        this.timeout(60000);

        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { refresh_ttl: 3000 });
            ignore_ebusy(fsq2);

            var count = 0;

            process.nextTick(function ()
            {
                var orig_process_all = fsq2._process_all,
                    time = new Date().getTime();

                fsq2._process_all = function ()
                {
                    count += 1;

                    var r = orig_process_all.apply(this, arguments);

                    if (count === 1024)
                    {
                        var time2 = new Date().getTime();

                        expect(time2 - time).to.be.at.least(9000);

                        fsq2.stop_watching(done);
                    }

                    return r;
                };
            });

            fsq2.on('start', function ()
            {
                expect(count).to.equal(256);
            });
        });
    });

    if (use_disruptor)
    {
        it('should be able to disable refresh from disk', function (done)
        {
            this.timeout(60000);

            fsq.stop_watching(function ()
            {
                var fsq2 = make_fsq(1, 0, { refresh_ttl: 0 });
                ignore_ebusy(fsq2);

                var count = 0;

                process.nextTick(function ()
                {
                    var orig_process_all = fsq2._process_all;

                    fsq2._process_all = function ()
                    {
                        count += 1;
                        return orig_process_all.apply(this, arguments);
                    };
                });

                fsq2.on('start', function ()
                {
                    expect(count).to.equal(0);

                    setTimeout(function ()
                    {
                        expect(count).to.equal(0);
                        fsq2.stop_watching(done);
                    }, 11000);
                });
            });
        });
    }

    it('should be able to change the number of random bytes at the end of filenames', function (done)
    {
        fsq.subscribe('foo', function (data, info)
        {
            var split = info.fname.split('+'), fsq2;
            expect(split[split.length - 1].length).to.equal(32);

            fsq.stop_watching(function ()
            {
                fsq2 = make_fsq(1, 0, { unique_bytes: 8 });
                ignore_ebusy(fsq2);

                fsq2.subscribe('foo', function (data, info)
                {
                    var split2 = info.fname.split('+');
                    expect(split2[split2.length - 1].length).to.equal(16);
                    fsq2.stop_watching(done);
                });

                fsq2.on('start', function ()
                {
                    fsq2.publish('foo', 'bar');
                });
            });
        });

        fsq.publish('foo', 'bar');
    });

    it('should read one message at a time by default', function (done)
    {
        this.timeout(5 * 60 * 1000);

        restore();

        fsq.stop_watching(function ()
        {
            var in_call = false,
                count = 0,
                fsq2 = make_fsq(1, 0,
                {
                    poll_interval: 10 * 1000,
                    notify: false
                });

            ignore_ebusy(fsq2);

            function handler (stream, info, cb)
            {
                expect(in_call).to.equal(false);

                in_call = true;
                count += 1;

                stream.on('end', function ()
                {
                    in_call = false;
                    cb(null, count === 5 ? function ()
                    {
                        fsq2.stop_watching(done);
                    } : null);
                });

                if (count === 5)
                {
                    stream.on('data', function () { return undefined; });
                }
                else
                {
                    // give time for other reads to start
                    setTimeout(function ()
                    {
                        stream.on('data', function () { return undefined; });
                    }, 5 * 1000);
                }
            }

            handler.accept_stream = true;
            
            fsq2.subscribe('foo', handler);

            fsq2.on('start', function ()
            {
                function cb(err)
                {
                    if (err) { done(err); }
                }

                for (var i = 0; i < 5; i += 1)
                {
                    fsq2.publish('foo', 'bar', { ttl: 2 * 60 * 1000 }, cb);
                }
            });
        });
    });

    it('should be able to read more than one message at a time', function (done)
    {
        this.timeout(5 * 60 * 1000);

        restore();

        fsq.stop_watching(function ()
        {
            var in_call = 0,
                count = 0,
                fsq2 = make_fsq(1, 0,
                {
                    poll_interval: 10 * 1000,
                    notify: false,
                    bucket_base: 10,
                    bucket_num_chars: 2,
                    bucket_concurrency: 5,
                    message_concurrency: 2
                });

            ignore_ebusy(fsq2);

            function handler(stream, info, cb)
            {
                expect(in_call).to.be.at.most(9);

                in_call += 1;
                count += 1;

                stream.on('end', function ()
                {
                    in_call -= 1;
                    cb(null, (count === 25) && (in_call === 0) ? function ()
                    {
                        fsq2.stop_watching(done);
                    } : null);
                });

                if (count === 25)
                {
                    stream.on('data', function () { return undefined; });
                }
                else
                {
                    // give time for other reads to start
                    setTimeout(function ()
                    {
                        stream.on('data', function () { return undefined; });
                    }, 5 * 1000);
                }
            }
            
            handler.accept_stream = true;

            fsq2.subscribe('foo', handler);

            fsq2.on('start', function ()
            {
                var i;

                function cb(err)
                {
                    if (err) { done(err); }
                }

                for (i = 0; i < 25; i += 1)
                {
                    fsq2.publish('foo', 'bar', { ttl: 2 * 60 * 1000 }, cb);
                }
            });
        });
    });
 
    it('should clear up expired messages', function (done)
    {
        var num_queues = 100,
            num_messages = 500;

        restore();

        this.timeout(10 * 60 * 1000);

        fsq.stop_watching(async function ()
        {
            const open_before = await lsof();
            async.timesSeries(num_queues, function (n, cb)
            {
                var fsq = make_fsq(num_queues, n);
                ignore_ebusy(fsq, os.platform() === 'win32' ? 'EPERM' : null);

                fsq.on('start', function ()
                {
                    cb(null, fsq);
                });
            }, function (err, fsqs)
            {
                if (err) { return done(err); }

                expect(fsqs.length).to.equal(num_queues);

                async.timesSeries(num_messages, function (n, cb)
                {
                    fsqs[0].publish('foo', 'bar', { ttl: 2 * 1000 }, cb);
                }, function (err)
                {
                    if (err) { return done(err); }

                    setTimeout(function ()
                    {
                        async.each(fsqs, function (fsq, next)
                        {
                            fsq.subscribe('foo', function ()
                            {
                                throw new Error('should not be called');
                            });
                            fsq.force_refresh();
                            next();
                        }, function ()
                        {
                            setTimeout(function ()
                            {
                                async.each(fsqs, function (fsq, cb)
                                {
                                    fsq.stop_watching(cb);
                                }, function ()
                                {
                                    check_empty(msg_dir, done, async function ()
                                    {
                                        const open_after = await lsof();
                                        try
                                        {
                                            expect(open_after[0]).to.eql(open_before[0]);
                                        }
                                        catch (ex)
                                        {
                                            console.error(open_before[1], open_after[1]);
                                            throw ex;
                                        }
                                        done();
                                    });
                                });
                            }, 60 * 1000);
                        });
                    }, 2 * 1000);
                });
            });
        });
    });

    if (single_supported)
    {
        it('should clear up expired message while worker has it locked', function (done)
        {
            this.timeout(60 * 1000);

            restore();

            fsq.subscribe('foo', function (data, info, cb)
            {
                console.log(info);
                setTimeout(function ()
                {
                    if (process.platform === 'win32')
                    {
                        get_message_files(msg_dir, function (err, files)
                        {
                            if (err) { return done(err); }
                            expect(files.length).to.equal(1);
                            cb(null, done);
                        });
                    }
                    else
                    {
                        check_empty(msg_dir, done, function ()
                        {
                            cb(null, done);
                        });
                    }
                }, 30 * 1000);
            });

            fsq.publish('foo', 'bar', { single: true, ttl: 5000 }, function (err)
            {
                if (err) { done(err); }
            });
        });
    }

    function bucket_names(base, chars)
    {
        var n = Math.pow(base, chars), i, s, r = [], arr = [];

        for (i = 0; i < n; i += 1)
        {
            s = base > 1 ? i.toString(base) : '0';
            arr.length = chars + 1;
            r.push((arr.join('0') + s).slice(-chars));
        }

        return r;
    }

    function test_buckets(base, chars)
    {
        it('should distribute messages between bucket directories (base=' + base + ', chars=' + chars + ')', function (done)
        {
            // This _could_ fail if the hash function happens not to distribute
            // at least one message into each bucket.

            var timeout = 10 * 60 * 1000,
                buckets = {},
                count = 0,
                num,
                fsq2;

            this.timeout(timeout);

            function go()
            {
                num = Math.pow(base, chars) * 15;

                fsq2.subscribe('foo', function (data, info)
                {
                    var mdir = path.join(path.dirname(info.path), '..');
                    expect(mdir).to.equal(msg_dir);

                    buckets[path.basename(path.dirname(info.path))] = true;

                    count += 1;

                    if (count === num)
                    {
                        fs.readdir(mdir, function (err, files)
                        {
                            var names = bucket_names(base, chars);
                            if (err) { return done(err); }
                            expect(files.sort()).to.eql(names);
                            expect(Object.keys(buckets).sort()).to.eql(names);
                            fsq2.stop_watching(done);
                        });
                    }
                    else if (count > num)
                    {
                        throw new Error("called too many times");
                    }
                }, function (err)
                {
                    if (err) { done(err); }
                    async.timesLimit(num, 5, function (i, next)
                    {
                        fsq2.publish('foo', 'bar', { ttl: timeout }, function (err)
                        {
                            if (err) { done(err); }
                            next();
                        });
                    });
                });
            }

            if (base === undefined)
            {
                base = 16;
                chars = 2;
                fsq2 = fsq;
                go();
            }
            else
            {
                fsq.stop_watching(function ()
                {
                    rimraf(fsq_dir, function (err)
                    {
                        if (err) { return done(err); }
                        fsq2 = make_fsq(1, 0,
                        {
                            bucket_base: base,
                            bucket_num_chars: chars,
                        });
                        ignore_ebusy(fsq2);
                        fsq2.on('start', go);
                    });
                });
            }
        });
    }

    test_buckets();
    test_buckets(1, 1);
    test_buckets(10, 2);
    test_buckets(26, 1);
    test_buckets(26, 2);
    test_buckets(8, 3);

    it('should emit an error event if an error occurs before a start event', function (done)
    {
        var orig_mkdir = fs.mkdir, fsq2;

        fs.mkdir = function (dir, cb)
        {
            cb('dummy error');
        };

        fsq2 = make_fsq(2, 1);
        ignore_ebusy(fsq2);

        fsq2.on('error', function (err)
        {
            expect(err).to.equal('dummy error');
            fs.mkdir = orig_mkdir;
            this.stop_watching(done);
        });
    });

    it('should handle read errors', function (done)
    {
        var count = 0,
            orig_createReadStream = fs.createReadStream;

        fsq._fs.createReadStream = function ()
        {
            return orig_createReadStream.call(this, '');
        };

        fsq.on('warning', function (err)
        {
            if (err && (err.code === 'ENOENT'))
            {
                count += 1;

                if (!single_supported)
                {
                    fsq._fs.createReadStream = orig_createReadStream;
                    done();
                }
                else if (count === 5) // check single repeats
                {
                    fsq._fs.createReadStream = orig_createReadStream;
                }
            }
        });

        fsq.subscribe('foo', function (data, info, cb)
        {
            if (!info.single)
            {
                expect(use_disruptor).to.equal(true);
                if (!single_supported)
                {
                    done();
                }
                return;
            }
            expect(info.single).to.equal(true); // we throw multi away on error
            cb(null, done);
        });

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });

        fsq.publish('foo', 'bar', { single: true }, function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should pass back write errors when publishing', function (done)
    {
        var orig_createWriteStream = fsq._fs.createWriteStream;

        fsq._fs.createWriteStream = function ()
        {
            return orig_createWriteStream.call(this, '');
        };

        fsq.publish('foo', 'bar', function (err)
        {
            expect(err.code).to.equal('ENOENT');
            fsq._fs.createWriteStream = orig_createWriteStream;
            done();
        });
    });

    it('should close stream if error occurs when publishing', function (done)
    {
        var finished = false;

        var s = fsq.publish('foo', function (err)
        {
            // wait for initial byte write to be done, which holds up prefinish
            setTimeout(function ()
            {
                expect(err.message).to.equal('dummy');
                expect(finished).to.equal(true);
                done();
            }, 500);
        });

        s.on('prefinish', function ()
        {
            finished = true;
        });

        s.emit('error', new Error('dummy'));
    });

    it('should close stream if error occurs when publishing after open', function (done)
    {
        var finished = false;

        var s = fsq.publish('foo', function (err)
        {
            expect(err.message).to.be.oneOf([
                'dummy',
                // Node 12: Stream is destroyed with 'dummy' but the write to
                // fs_stream is delayed until fs_stream is opened.
                'Cannot call write after a stream was destroyed'
            ]);
            expect(finished).to.equal(true);
            done();
        });

        s.on('prefinish', function ()
        {
            finished = true;
        });

        if (use_disruptor)
        {
            // Not a fs.WriteStream until content exceeds disruptor element size
            s.write('a', function ()
            {
                s.emit('error', new Error('dummy'));
            });
        }
        else
        {
            s.on('open', function ()
            {
                this.emit('error', new Error('dummy'));
            });
        }
    });

    it('should handle closed direct stream before given to subscribers', function (done)
    {
        var stream_mod = require('stream');

        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { direct_handler: new class
            {
                constructor()
                {
                    this.streams = new Map();
                    this.gsfp_called = false;
                    this.gsfs_called = false;
                    this.psd_called = false;
                    this.ssd_called = false;
                }

                get_stream_for_publish(filename, direct)
                {
                    this.gsfp_called = true;
                    expect(direct).to.equal('something truthy');
                    const r = new stream_mod.PassThrough();
                    this.streams.set(filename, r);
                    return r;
                }

                get_stream_for_subscribers(filename)
                {
                    expect(this.gsfs_called).to.be.false;
                    this.gsfs_called = true;
                    expect(this.gsfp_called).to.be.true;
                    expect(this.psd_called).to.be.true;
                    expect(this.ssd_called).to.be.false;
                    const r = this.streams.get(filename);
                    expect(r).to.be.undefined;
                    fsq2.stop_watching(done);
                    return r;
                }

                publish_stream_destroyed(filename, stream)
                {
                    this.psd_called = true;
                    const s = this.streams.get(filename);
                    expect(s).to.equal(stream);
                    this.streams.delete(filename);
                }

                subscriber_stream_destroyed(unused_filename, unused_stream)
                {
                    this.ssd_called = true;
                }
            }()});

            fsq2.on('start', function ()
            {
                var pub_called = false,
                    stream_direct;

                function handler(unused_stream, unused_info, unused_cb)
                {
                    done(new Error('should not be called'));
                }
                handler.accept_stream = true;
                fsq2.subscribe('foo', handler);
                
                stream_direct = fsq2.publish('foo', { direct: 'something truthy' }, function (err)
                {
                    if (err) { return done(err); }
                    expect(pub_called).to.equal(false);
                    pub_called = true;
                });
                stream_direct.destroy();
            });
        });

    });

    it('should handle errored direct stream before given to subscribers', function (done)
    {
        var stream_mod = require('stream'),
            warning_called = false;

        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { direct_handler: new class
            {
                constructor()
                {
                    this.streams = new Map();
                    this.gsfp_called = false;
                    this.gsfs_called = false;
                    this.psd_called = false;
                    this.ssd_called = false;
                }

                get_stream_for_publish(filename, direct)
                {
                    this.gsfp_called = true;
                    expect(direct).to.equal('something truthy');
                    const r = new stream_mod.PassThrough();
                    this.streams.set(filename, r);
                    return r;
                }

                get_stream_for_subscribers(filename)
                {
                    expect(this.gsfs_called).to.be.false;
                    this.gsfs_called = true;
                    expect(warning_called).to.be.true;
                    expect(this.gsfp_called).to.be.true;
                    expect(this.psd_called).to.be.true;
                    expect(this.ssd_called).to.be.false;
                    expect(this.streams.has(filename)).to.be.false;
                    fsq2.stop_watching(done);
                    return null;
                }

                publish_stream_destroyed(filename, stream)
                {
                    this.psd_called = true;
                    const s = this.streams.get(filename);
                    expect(s).to.equal(stream);
                    this.streams.delete(filename);
                }

                subscriber_stream_destroyed(unused_filename, unused_stream)
                {
                    this.ssd_called = true;
                }
            }()});

            fsq2.on('start', function ()
            {
                var pub_called = false,
                    stream_direct;

                function handler(unused_stream, unused_info, unused_cb)
                {
                    done(new Error('should not be called'));
                }
                handler.accept_stream = true;
                fsq2.subscribe('foo', handler);

                fsq2.on('warning', function (err)
                {
                    warning_called = true;
                    expect(err.message).to.equal('some error');
                    expect(this._direct_handler.gsfp_called).to.be.true;
                    expect(this._direct_handler.gsfs_called).to.be.false;
                    expect(this._direct_handler.psd_called).to.be.false;
                    expect(this._direct_handler.ssd_called).to.be.false;
                });
                
                stream_direct = fsq2.publish('foo', { direct: 'something truthy' }, function (err)
                {
                    if (err) { return done(err); }
                    expect(pub_called).to.equal(false);
                    pub_called = true;
                });
                stream_direct.destroy(new Error('some error'));
            });
        });
    });

    it('should support disabling work queue (single messages)', function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { single: false }), called = false;
            ignore_ebusy(fsq2);

            fsq2.subscribe('foo', function (data, info, unused_cb)
            {
                expect(info.topic).to.equal('foo');
                expect(data.toString('utf8')).to.equal('bar');
                // should never be called with 'single' messages
                expect(info.single).to.equal(false);
                called = true;
            });

            fsq2.on('start', function ()
            {
                fsq2.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });

                fsq2.publish('foo', 'bar', { single: true }, function (err)
                {
                    if (err) { done(err); }
                });
            });

            setTimeout(function ()
            {
                expect(called).to.equal(true);
                fsq2.stop_watching(done);
            }, 10000);
        });
    });

    it('should disable work queue (single messages) if fs-ext is not available', function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0);

            fsq2._require_fs = function (fs)
            {
                if (fs !== 'fs')
                {
                    throw new Error('dummy');
                }

                return require(fs);
            };

            fsq2.on('start', function ()
            {
                expect(this._do_single).to.equal(false);
                fsq2.stop_watching(done);
            });
        });
    });

    it('should emit event when fs-ext is not available', function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0);

            fsq2._require_fs = function (fs)
            {
                if (fs !== 'fs')
                {
                    throw new Error('dummy');
                }

                return require(fs);
            };

            fsq2.on('single_disabled', function (err)
            {
                expect(this._do_single).to.equal(false);
                expect(err.message).to.equal('dummy');
                fsq2.stop_watching(done);
            });
        });
    });

    it('should emit event when getdents is not available', function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { getdents_size: 1024 });

            fsq2._require_getdents = function ()
            {
                throw new Error('dummy');
            };

            fsq2.on('getdents_disabled', function (err)
            {
                expect(err.message).to.equal('dummy');
                fsq2.stop_watching(done);
            });
        });
    });

    it('should publish to a topic with an invalid file name character', function (done)
    {
        var arr = [], ltopic, rsingle = !single_supported, rmulti = false;
        arr.length = 64 * 1024 + 1;
        ltopic = arr.join('\0');

        fsq.subscribe('*', function (data, info, cb)
        {
            if (info.single)
            {
                expect(rsingle).to.equal(false);
                expect(info.topic).to.equal(ltopic);
                expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
                expect(info.fname.lastIndexOf(Buffer.from(ltopic).toString('hex').substr(0, fsq._split_topic_at) + '@', 0)).to.equal(0);
                expect(data.toString('utf8')).to.equal('test');

                var topic_dir = path.dirname(path.dirname(info.topic_path));
                expect(topic_dir).to.equal(path.join(msg_dir, '..', 'topics'));
                expect(fs.readFileSync(info.topic_path).toString('utf8')).to.equal(Buffer.from(ltopic).toString('hex').substr(fsq._split_topic_at));
            }
            else
            {
                expect(rmulti).to.equal(false);
                expect(info.topic).to.equal('\0foo');
                expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
                expect(info.fname.lastIndexOf(Buffer.from('\0foo').toString('hex') + '@', 0)).to.equal(0);
                expect(info.topic_path).to.equal(undefined);
                expect(data.toString('utf8')).to.equal('bar');
            }

            cb(null, function ()
            {
                if (info.single)
                {
                    rsingle = true;
                }
                else
                {
                    rmulti = true;
                }

                if (rsingle && rmulti)
                {
                    fsq.stop_watching(done);
                }
            });
        });

        fsq.publish('\0foo', 'bar', function (err)
        {
            if (err) { done(err); }
        });

        fsq.publish(ltopic, 'test', { single: true }, function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should error when publishing to a topic with an invalid file name character and topic encoding is disabled',  function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { encode_topics: false });

            ignore_ebusy(fsq2);

            fsq2.on('start', function ()
            {
                this.publish('\0foo', 'bar', function (err)
                {
                    if (ephemeral)
                    {
                        if (err) { return done(err); }
                    }
                    else
                    {
                        if (!err) { return done(new Error('expected an error')); }
                        if (err.code) // 0.12 doesn't set code
                        {
                            expect(err.code).to.be.oneOf(['ENOENT', 'ERR_INVALID_ARG_TYPE', 'ERR_INVALID_ARG_VALUE']);
                        }
                    }
                    this.stop_watching(done);
                });
            });
        });
    });

    it('should not error when publishing to a topic without an invalid file name character and topic encoding is disabled', function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { encode_topics: false });
            ignore_ebusy(fsq2);

            fsq2.on('start', function ()
            {
                var arr = [], ltopic, rsingle = !single_supported, rmulti = false;
                arr.length = 64 * 1024 + 1;
                ltopic = arr.join('a');

                this.subscribe('*', function (data, info, cb)
                {
                    if (info.single)
                    {
                        expect(rsingle).to.equal(false);
                        expect(info.topic).to.equal(ltopic);
                        expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
                        expect(info.fname.lastIndexOf(ltopic.substr(0, fsq._split_topic_at) + '@', 0)).to.equal(0);
                        expect(data.toString('utf8')).to.equal('test');

                        var topic_dir = path.dirname(path.dirname(info.topic_path));
                        expect(topic_dir).to.equal(path.join(msg_dir, '..', 'topics'));
                        expect(fs.readFileSync(info.topic_path).toString('utf8')).to.equal(ltopic.substr(fsq._split_topic_at));
                    }
                    else
                    {
                        expect(rmulti).to.equal(false);
                        expect(info.topic).to.equal('foo');
                        expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
                        expect(info.fname.lastIndexOf('foo' + '@', 0)).to.equal(0);
                        expect(info.topic_path).to.equal(undefined);
                        expect(data.toString('utf8')).to.equal('bar');
                    }

                    cb(null, function ()
                    {
                        if (info.single)
                        {
                            rsingle = true;
                        }
                        else
                        {
                            rmulti = true;
                        }

                        if (rsingle && rmulti)
                        {
                            fsq2.stop_watching(done);
                        }
                    });
                });

                this.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });

                this.publish(ltopic, 'test', { single: true }, function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    });

    it('should support handler concurrency', function (done)
    {
        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0, { handler_concurrency: 2 });
            ignore_ebusy(fsq2);

            fsq2.on('start', function ()
            {
                var streams = [];

                function handler(s, unused_info)
                {
                    streams.push(s);

                    if (streams.length === 1)
                    {
                        fsq2.publish('foo', 'bar2', function (err)
                        {
                            if (err) { done(err); }
                        });
                    }
                    else if (streams.length === 2)
                    {
                        read_all(streams[0], function (v)
                        {
							expect(v.toString()).to.equal('bar');
                            read_all(streams[1], function (v)
                            {
								expect(v.toString()).to.equal('bar2');
                                fsq2.stop_watching(done);
                            });
                        });
                    }
                    else
                    {
                        done(new Error('called too many times'));
                    }
                }

                handler.accept_stream = true;

                this.subscribe('foo', handler);

                this.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    });

    it('should support delivering messages in expiry order', function (done)
    {
        restore();

        fsq.stop_watching(function ()
        {
            var fsq2 = make_fsq(1, 0,
            {
                poll_interval: 60 * 60 * 1000,
                notify: false,
                order_by_expiry: true,
                multi_ttl: 24 * 60 * 60 * 1000
            });

            expect(fsq2._bucket_base).to.equal(1);
            expect(fsq2._bucket_num_chars).to.equal(1);

            ignore_ebusy(fsq2);

            fsq2.on('start', function ()
            {
                var n = 1000, ttls_out = [], ttls_in = [], expiries_in = [], i;

                // Need to leave enough time between ttls to account for
                // time increasing while publishing
                for (i = 0; i < n; i += 1)
                {
                    ttls_out.push(Math.round(

                    Math.random() * 18 * 60 // random mins up to 18 hour period
                    + 1 * 60)               // plus one hour
                    
                    * 60 * 1000);           // convert to milliseconds
                }

                function num_sort(x, y)
                {
                    return x - y;
                }

                fsq2.subscribe('foo', function (data, info)
                {
                    ttls_in.push(parseInt(data.toString()));
                    expiries_in.push(info.expires);
                    
                    if (ttls_in.length === n)
                    {
                        // check expiries are actually in ascending order
                        var sorted_expiries_in = expiries_in.concat();
                        sorted_expiries_in.sort(num_sort);
                        expect(expiries_in).to.eql(sorted_expiries_in);

                        // check messages are in expected order
                        ttls_out.sort(num_sort);
                        expect(ttls_in).to.eql(ttls_out);

                        this.stop_watching(done);
                    }
                    else if (ttls_in.length > n)
                    {
                        done(new Error('called too many times'));
                    }
                });

                async.eachSeries(ttls_out, function (ttl, cb)
                {
                    fsq2.publish('foo', ttl.toString(), { ttl: ttl }, cb);
                }, function (err)
                {
                    expect(err).to.equal(null);
                    expect(ttls_in.length).to.equal(0);
                    fsq2.refresh_now();
                });
            });
        });
    });

    it('should support error on stream and calling back', function (done)
    {
        var msg;

        fsq.on('warning', function (err)
        {
            if (err.code !== 'EBUSY')
            {
                msg = err.message;
            }
        });

        var handler = function (stream, info, cb)
        {
            stream.emit('error', new Error('dummy'));
            cb(new Error('dummy'), function (err)
            {
                expect(err).to.equal(null);
                expect(msg).to.equal('dummy');
                done();
            });
        };
        handler.accept_stream = true;

        fsq.subscribe('foo', handler);
        fsq.publish('foo', { single: single_supported }).end('bar');
    });

    it('should support calling back before stream has ended', function (done)
    {
        var count = 0;

        function handler(stream, info, cb)
        {
            count += 1;

            if (count === 2)
            {
                cb(null, done);
            }
            else
            {
                cb();
            }
        }
        handler.accept_stream = true;

        fsq.subscribe('foo', handler);
        fsq.publish('foo').end('bar');
        fsq.publish('foo', { single: single_supported }).end('bar');
    });

    it('should end/error stream after called back before stream has ended',
    function (done)
    {
        var count = 0;

        function handler(stream, info, cb)
        {
            stream.on('readable', function ()
            {
                var data = this.read();
                if (use_disruptor && !info.single)
                {
                    expect(data.toString()).to.equal('bar');
                }
                else
                {
                    expect(data).to.equal(null);
                }
            });

            count += 1;

            if (count === 2)
            {
                stream.on('end', done);
                cb();
            }
            else
            {
                var ended = false, msg;

                stream.on('end', function ()
                {
                    ended = true;
                });

                stream.on('error', function (err)
                {
                    msg = err.message;
                });

                cb(new Error('dummy'), function (err)
                {
                    expect(err).not.to.exist;
                    expect(msg).to.equal('dummy');

                    setImmediate(function ()
                    {
                        expect(ended).to.equal(true);
                        fsq.publish('foo', { single: single_supported }).end('bar');
                    });
                });
            }
        }
        handler.accept_stream = true;

        fsq.subscribe('foo', handler);
        fsq.publish('foo').end('bar');
    });

    function existing_messages(dedup)
    {
        describe('dedup=' + dedup, function ()
        {
            it('should support delivering existing messages to subscribers',
            function(done)
            {
                restore();

                fsq.stop_watching(function ()
                {
                    var fsq2 = make_fsq(1, 0,
                    {
                        ttl: 10000,
                        dedup: dedup
                    });

                    fsq2.on('start', function ()
                    {
                        fsq2.subscribe('foo', function (data, info)
                        {
                            expect(info.existing).to.equal(undefined);
                            expect(data.toString()).to.equal('bar');
                            expect(info.topic).to.equal('foo');

                            setTimeout(function ()
                            {
                                fsq2.subscribe('foo', function (data2, info2)
                                {
                                    expect(info.existing).to.equal(undefined);
                                    expect(info2.existing).to.equal(true);
                                    expect(data2.toString()).to.equal('bar');
                                    expect(info2.topic).to.equal('foo');
                                    expect(info2.path).to.equal(info.path);
                                    this.stop_watching(done);
                                },
                                {
                                    subscribe_to_existing: true
                                });
                            }, 500);
                        });

                        fsq2.publish('foo').end('bar');
                    });
                });
            });

            it('should support delaying existing messages with filter',
            function (done)
            {
                restore();

                fsq.stop_watching(function ()
                {
                    var count = 0;

                    function filter(info, handlers, cb)
                    {
                        count += 1;
                        if (count === 1)
                        {
                            // allow through initial pub and sub
                            cb(null, true, handlers);
                        }
                        else if (count === 2)
                        {
                            // delay first existing
                            cb(null, false);
                        }
                        else if (count === 3)
                        {
                            // subscribe again to existing messages
                            fsq2.subscribe('foo', function (data, info)
                            {
                                expect(info.existing).to.equal(true);
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');
                            },
                            {
                                subscribe_to_existing: true
                            });

                            // and delay again
                            cb(null, false);
                        }
                        else
                        {
                            // allow through existing
                            cb(null, true, handlers);
                        }
                    }

                    var fsq2 = make_fsq(1, 0,
                    {
                        ttl: 10000,
                        dedup: dedup,
                        filter: filter
                    });

                    fsq2.on('start', function ()
                    {
                        fsq2.subscribe('foo', function (data, info)
                        {
                            expect(info.existing).to.equal(undefined);
                            expect(data.toString()).to.equal('bar');
                            expect(info.topic).to.equal('foo');

                            setTimeout(function ()
                            {
                                fsq2.subscribe('foo', function (data2, info2)
                                {
                                    expect(info.existing).to.equal(undefined);
                                    expect(info2.existing).to.equal(true);
                                    expect(data2.toString()).to.equal('bar');
                                    expect(info2.topic).to.equal('foo');
                                    expect(info2.path).to.equal(info.path);
                                    this.stop_watching(done);
                                },
                                {
                                    subscribe_to_existing: true
                                });
                            }, 500);
                        });

                        fsq2.publish('foo').end('bar');
                    });
                });
            });

            it('should support delaying existing messages twice',
            function (done)
            {
                restore();

                fsq.stop_watching(function ()
                {
                    var count = 0;

                    function filter(info, handlers, cb)
                    {
                        count += 1;
                        if (count === 1)
                        {
                            // allow through initial pub and sub
                            cb(null, true, handlers);
                        }
                        else if (count === 2)
                        {
                            // delay first existing
                            cb(null, false);
                        }
                        else if (count === 3)
                        {
                            // subscribe again to existing messages
                            fsq2.subscribe('foo', function (data, info)
                            {
                                expect(info.existing).to.equal(true);
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');
                            },
                            {
                                subscribe_to_existing: true
                            });

                            // for old method which checks if has unsubscribed
                            fsq2.unsubscribe('dummy');

                            // and delay again
                            cb(null, false);
                        }
                        else if (count === 4)
                        {
                            // and delay again
                            cb(null, false);
                        }
                        else
                        {
                            // allow through existing
                            cb(null, true, handlers);
                        }
                    }

                    var fsq2 = make_fsq(1, 0,
                    {
                        ttl: 10000,
                        dedup: dedup,
                        filter: filter
                    });

                    fsq2.on('start', function ()
                    {
                        fsq2.subscribe('foo', function (data, info)
                        {
                            expect(info.existing).to.equal(undefined);
                            expect(data.toString()).to.equal('bar');
                            expect(info.topic).to.equal('foo');

                            setTimeout(function ()
                            {
                                fsq2.subscribe('foo', function (data2, info2)
                                {
                                    expect(info.existing).to.equal(undefined);
                                    expect(info2.existing).to.equal(true);
                                    expect(data2.toString()).to.equal('bar');
                                    expect(info2.topic).to.equal('foo');
                                    expect(info2.path).to.equal(info.path);
                                    this.stop_watching(done);
                                },
                                {
                                    subscribe_to_existing: true
                                });
                            }, 500);
                        });

                        fsq2.publish('foo').end('bar');
                    });
                });
            });

            it('should subscribe to new messages too',
            function (done)
            {
                restore();

                fsq.stop_watching(function ()
                {
                    var fsq2 = make_fsq(1, 0,
                    {
                        ttl: 10000,
                        dedup: dedup
                    });

                    fsq2.on('start', function ()
                    {
                        fsq2.subscribe('foo', function (data, info)
                        {
                            expect(info.existing).to.equal(undefined);
                            expect(data.toString()).to.equal('bar');
                            expect(info.topic).to.equal('foo');
                            this.stop_watching(done);
                        },
                        {
                            subscribe_to_existing: true
                        });

                        // When using Disruptor, there is no cache so we don't
                        // know if we've seen a message. So we regard all as
                        // existing until we've done a poll. Thus, start a
                        // refresh (poll) now and then wait a bit for it to
                        // finish.
                        fsq2.refresh_now();
                        setTimeout(function ()
                        {
                            fsq2.publish('foo').end('bar');
                        }, 500);
                    });
                });
            });

            it('should support unsubscribing from existing messages',
            function(done)
            {
                restore();

                fsq.stop_watching(function ()
                {
                    var fsq2 = make_fsq(1, 0,
                    {
                        ttl: 10000,
                        dedup: dedup
                    });

                    fsq2.on('start', function ()
                    {
                        fsq2.subscribe('foo', function (data, info)
                        {
                            expect(info.existing).to.equal(undefined);
                            expect(data.toString()).to.equal('bar');
                            expect(info.topic).to.equal('foo');

                            setTimeout(function ()
                            {
                                function handler()
                                {
                                    done(new Error('should not be called'));
                                }

                                fsq2.subscribe('foo', handler,
                                {
                                    subscribe_to_existing: true
                                });

                                fsq2.unsubscribe('foo', handler);

                                setTimeout(function ()
                                {
                                    fsq2.stop_watching(done);
                                }, 1500);
                            }, 500);
                        });

                        fsq2.publish('foo').end('bar');
                    });
                });
            });

            function unsub_delayed_existing(unsub, done)
            {
                restore();

                fsq.stop_watching(function ()
                {
                    var count = 0;

                    function handler()
                    {
                        done(new Error('should not be called'));
                    }

                    function handler2()
                    {
                        done(new Error('should not be called'));
                    }

                    function filter(info, handlers, cb)
                    {
                        count += 1;
                        if (count === 1)
                        {
                            // allow through initial pub and sub
                            cb(null, true, handlers);
                        }
                        else if (count === 2)
                        {
                            // delay first existing
                            cb(null, false);
                        }
                        else if (count === 3)
                        {
                            // subscribe again to existing messages
                            fsq2.subscribe('foo', handler2,
                            {
                                subscribe_to_existing: true
                            });

                            // unsubscribe
                            unsub(fsq2, handler, handler2);

                            // and delay again
                            cb(null, false);

                            setTimeout(function ()
                            {
                                fsq2.stop_watching(done);
                            }, 1500);
                        }
                        else
                        {
                            // existing message doesn't call filter if there
                            // are no handlers
                            done(new Error('called too many times'));
                        }
                    }

                    var fsq2 = make_fsq(1, 0,
                    {
                        ttl: 10000,
                        dedup: dedup,
                        filter: filter
                    });

                    fsq2.on('start', function ()
                    {
                        fsq2.subscribe('foo', function (data, info)
                        {
                            expect(info.existing).to.equal(undefined);
                            expect(data.toString()).to.equal('bar');
                            expect(info.topic).to.equal('foo');

                            setTimeout(function ()
                            {
                                fsq2.subscribe('foo', handler,
                                {
                                    subscribe_to_existing: true
                                });
                            }, 500);
                        });

                        fsq2.publish('foo').end('bar');
                    });
                });
            }

            it('should support unsubscribing from delayed existing messages (by handler)',
            function (done)
            {
                unsub_delayed_existing(function (fsq2, handler, handler2)
                {
                    fsq2.unsubscribe('foo', handler);
                    fsq2.unsubscribe('foo', handler2);
                }, done);
            });

            it('should support unsubscribing from delayed existing messages (by topic)',
            function (done)
            {
                unsub_delayed_existing(function (fsq2, unused_handler, unused_handler2)
                {
                    fsq2.unsubscribe('foo');
                }, done);
            });

            it('should support unsubscribing from delayed existing messages (all)',
            function (done)
            {
                unsub_delayed_existing(function (fsq2, unused_handler, unused_handler2)
                {
                    fsq2.unsubscribe();
                }, done);
            });
        });
    }

    if (!ephemeral)
    {
        describe('existing messages', function ()
        {
            existing_messages(true);
            existing_messages(false);
        });
    }

    if (!use_disruptor)
    {
        it('should be able to unsubscribe while file being opened', function (done)
        {
            var orig_open = fsq._fs.open;

            function handler()
            {
                done(new Error('should not be called'));
            }

            fsq._fs.open = function ()
            {
                fsq._fs.open = orig_open;
                var ths = this,
                    args = Array.from(arguments); 
                fsq.unsubscribe('foo', handler, function (err)
                {
                    if (err) { return done(err); }
                    setImmediate(done);
                    orig_open.apply(ths, args);
                });
            };

            fsq.subscribe('foo', handler, function (err)
            {
                if (err) { return done(err); }
                fsq.publish('foo', 'bar', function (err, unused_info)
                {
                    if (err) { done(err); }
                });
            });
        });
    }

    it('should publish empty message', function (done)
    {
        fsq.subscribe('foo', function handler(data, info)
        {
            expect(info.topic).to.equal('foo');
            expect(data.length).to.equal(0);
            done();
        });

        fsq.publish('foo', function (err)
        {
            if (err) { done(err); }
        }).end();
    });
});
}

test(0, false);

if (argv.disruptor)
{
    test(0, true, false);
    test(0, true, true);
}

if (argv.getdents_size > 0)
{
    test(argv.getdents_size, false);

    if (argv.disruptor)
    {
        test(argv.getdents_size, true, false);
        test(argv.getdents_size, true, true);
    }
}
