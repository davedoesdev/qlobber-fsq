/*global describe: false,
         it: false,
         fsq: false,
         expect: false,
         msg_dir: false,
         fs: false,
         QlobberFSQ: false,
         fsq_dir: false,
         flags: false,
         check_empty: false,
         async: false,
         path: false,
         crypto: false,
         lsof: false,
         rimraf: false,
         beforeEach: false,
         afterEach: false,
         ignore_ebusy: false,
         retry_interval: false,
         util: false */
/*jslint node: true, nomen: true, bitwise: true, todo: true */
"use strict";

describe('qlobber-fsq', function ()
{
    this.timeout(60 * 1000);

    var orig_close = fs.close,
        orig_flock = fs.flock,
        orig_ftruncate = fs.ftruncate,
        orig_rename = fs.rename;

    function restore()
    {
        fs.close = orig_close;
        fs.flock = orig_flock;
        fs.ftruncate = orig_ftruncate;
        fs.rename = orig_rename;
    }

    /*jslint unparam: true */
    beforeEach(function ()
    {
        var busied_close = false,
            busied_flock = false,
            busied_ftruncate = false,
            busied_rename = false;

        fs.close = function (fd, cb)
        {
            if (busied_close)
            {
                busied_close = false;
                return orig_close.apply(this, arguments);
            }

            busied_close = true;
            cb({ code: 'EBUSY' });
        };

        fs.flock = function (fd, type, cb)
        {
            if (busied_flock)
            {
                busied_flock = false;
                return orig_flock.apply(this, arguments);
            }

            busied_flock = true;
            cb({ code: 'EBUSY' });
        };

        fs.ftruncate = function (fd, size, cb)
        {
            if (busied_ftruncate)
            {
                busied_ftruncate = false;
                return orig_ftruncate.apply(this, arguments);
            }

            busied_ftruncate = true;
            cb({ code: 'EBUSY' });
        };

        fs.rename = function (src, dest, cb)
        {
            if (busied_rename)
            {
                busied_rename = false;
                return orig_rename.apply(this, arguments);
            }

            busied_rename = true;
            cb({ code: 'EBUSY' });
        };
    });
    /*jslint unparam: false */

    afterEach(restore);

    it('should subscribe and publish to a simple topic', function (done)
    {
        fsq.subscribe('foo', function (data, info)
        {
            expect(info.topic).to.equal('foo');
            expect(info.single).to.equal(false);
            expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
            expect(info.fname.lastIndexOf('foo@', 0)).to.equal(0);
            expect(info.topic_path).to.equal(undefined);
            expect(data.toString('utf8')).to.equal('bar');
            done();
        });

        fsq.publish('foo', 'bar', function (err)
        {
            if (err) { done(err); }
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
            var fsq2 = new QlobberFSQ(
            {
                fsq_dir: fsq_dir,
                flags: flags,
                dedup: false
            }),
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
            var fsq2 = new QlobberFSQ(
            {
                fsq_dir: fsq_dir,
                flags: flags,
                dedup: false
            }), count_multi = 0, count_single = 0;

            ignore_ebusy(fsq2);

            function handler(data, info, cb)
            {
                expect(info.topic).to.equal('foo');
                expect(data.toString('utf8')).to.equal('bar');

                if (info.single)
                {
                    count_single += 1;
                }
                else
                {
                    count_multi += 1;
                }

                if ((count_single === 1) && (count_multi === 2))
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
                    if ((count_single > 1) || (count_multi > 2))
                    {
                        throw new Error('called too many times');
                    }

                    cb();
                }
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

    it('should support a work queue', function (done)
    {
        fsq.subscribe('foo', function (data, info, cb)
        {
            expect(info.topic).to.equal('foo');
            expect(info.single).to.equal(true);
            expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
            expect(info.fname.lastIndexOf('foo@', 0)).to.equal(0);
            expect(data.toString('utf8')).to.equal('bar');

            fs.stat(info.path, function (err)
            {
                expect(err).to.equal(null);

                fs.open(info.path, 'r+', function (err, fd)
                {
                    expect(err).to.equal(null);

                    orig_flock(fd, 'exnb', function (err)
                    {
                        expect(err.code).to.equal('EAGAIN');

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

    it('should guard against calling subscribe callback twice', function (done)
    {
        fsq.on('warning', function (err)
        {
            if (err && (err.code !== 'EBUSY'))
            {
                throw new Error('should not be called');
            }
        });

        /*jslint unparam: true */
        fsq.subscribe('foo', function (data, info, cb)
        {
            expect(info.single).to.equal(true);

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

            fsq.publish('foo', 'bar', { single: true }, function (err)
            {
                if (err) { done(err); }
            });
        });
        /*jslint unparam: false */
    });

    it('should only give work to one worker', function (done)
    {
        this.timeout(30000);

        var fsq2 = new QlobberFSQ({ fsq_dir: fsq_dir, flags: flags }),
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

        /*jslint unparam: true */
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
        /*jslint unparam: false */

        fsq.publish('foo', 'bar', { single: true }, function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should allow handlers to refuse work', function (done)
    {
        fsq.stop_watching(function ()
        {
            function handler1()
            {
                throw new Error('should not be called');
            }

            var fsq2; 

            /*jslint unparam: true */
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
            /*jslint unparam: false */

            fsq2 = new QlobberFSQ(
            {
                fsq_dir: fsq_dir,
                flags: flags,
                filter: function (info, handlers, cb)
                {
                    expect(info.topic).to.equal('foo');

                    cb(null, true, handlers.filter(function (h)
                    {
                        return h !== handler1;
                    }));
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

    it('should put work back on queue for another handler', function (done)
    {
        fsq.stop_watching(function ()
        {
            /*jslint unparam: true */
            function handler(data, info, cb)
            {
                cb('dummy failure');
            }
            /*jslint unparam: false */

            var filter_called = false,
                fsq2 = new QlobberFSQ(
                {
                    fsq_dir: fsq_dir,
                    flags: flags,
                    filter: function (info, handlers, cb)
                    {
                        expect(info.topic).to.equal('foo');
                        expect(info.single).to.equal(true);

                        if (filter_called)
                        {
                            return cb(null, true, handlers.filter(function (h)
                            {
                                return h !== handler;
                            }));
                        }

                        filter_called = true;
                        cb(null, true, handlers);
                    }
                });

            ignore_ebusy(fsq2);

            fsq2.subscribe('foo', handler);

            /*jslint unparam: true */
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
            /*jslint unparam: false */

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
            /*jslint unparam: true */
            function handler(data, info, cb)
            {
                cb('dummy failure');
            }
            /*jslint unparam: false */

            var filter_called = false,
                fsq2 = new QlobberFSQ(
                {
                    fsq_dir: fsq_dir,
                    flags: flags,
                    filter: function (info, handlers, cb)
                    {
                        expect(info.topic).to.equal('foo');
                        expect(info.single).to.equal(true);

                        if (filter_called)
                        {
                            return cb(null, true, handlers.filter(function (h)
                            {
                                return h !== handler;
                            }));
                        }

                        filter_called = true;
                        cb(null, true, handlers);
                    }
                }),
                fsq3 = new QlobberFSQ({ fsq_dir: fsq_dir, flags: flags }),
                started2 = false,
                started3 = false;

            ignore_ebusy(fsq2);
            ignore_ebusy(fsq3);

            fsq2.subscribe('foo', handler);

            /*jslint unparam: true */
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
            /*jslint unparam: false */

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

    it('should allow handlers to delay a message', function (done)
    {
        restore();

        fsq.stop_watching(function ()
        {
            var ready_multi = false,
                ready_single = false,
                got_multi = false,
                got_single = false,
                count = 0,
                fsq2 = new QlobberFSQ(
                {
                    fsq_dir: fsq_dir,
                    flags: flags,
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

                        count += 1;
                        cb(null, (count % 5) === 0, handlers);
                    }
                });

            function handler(data, info, cb)
            {
                expect(data.toString('utf8')).to.equal('bar');

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

                if (got_single && got_multi && ready_single && ready_multi)
                {
                    expect(count).to.equal(10);

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

    it('should emit start and stop events', function (done)
    {
        this.timeout(30000);

        var fsq2 = new QlobberFSQ({ fsq_dir: fsq_dir, flags: flags });
        ignore_ebusy(fsq2);

        fsq2.on('start', function ()
        {
            fsq2.stop_watching();
            fsq2.on('stop', done);
        });
    });

    it('should support per-message time-to-live', function (done)
    {
        this.timeout(10000);

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

        /*jslint unparam: true */
        fsq.subscribe('foo', function (data, info, cb)
        {
            cb('dummy failure');
        });
        /*jslint unparam: false */

        fsq.publish('foo', 'bar', { single : true }, function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should support custom polling interval', function (done)
    {
        this.timeout(30000);

        restore();

        var time, count = 0, fsq2 = new QlobberFSQ(
        {
            fsq_dir: fsq_dir,
            flags: flags,
            poll_interval: 50
        });

        ignore_ebusy(fsq2);

        /*jslint unparam: true */
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
        /*jslint unparam: false */
 
        fsq2.on('start', function ()
        {
            time = new Date().getTime();

            fsq.publish('foo', 'bar', {single : true}, function (err)
            {
                if (err) { done(err); }
            });
        });
    });

    it('should support unsubscribing', function (done)
    {
        this.timeout(5000);

        var count = 0;

        /*jslint unparam: true */
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
        /*jslint unparam: false */

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

        /*jslint unparam: true */
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
        /*jslint unparam: false */

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

        /*jslint unparam: true */
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
        /*jslint unparam: false */

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
            var got_single = false,
                got_multi = false,
                fsq2 = new QlobberFSQ(
                {
                    fsq_dir: fsq_dir,
                    flags: flags,
                    multi_ttl: 1000,
                    single_ttl: 1000
                });

            ignore_ebusy(fsq2);

            /*jslint unparam: true */
            fsq2.subscribe('foo', function (data, info, cb)
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

                cb();
            });
            /*jslint unparam: false */

            fsq2.on('start', function ()
            {
                fsq2.publish('foo', 'bar', function (err)
                {
                    if (err) { done(err); }
                });

                fsq.publish('foo', 'bar', { single: true }, function (err)
                {
                    if (err) { done(err); }
                });
            });
        });
    });

    it('should publish and receive twice', function (done)
    {
        var count_multi = 0,
            count_single = 0;

        /*jslint unparam: true */
        fsq.subscribe('foo', function (data, info, cb)
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
                cb(null, done);
            }
            else
            {
                if ((count_single > 2) || (count_multi > 2))
                {
                    throw new Error('called too many times');
                }

                cb();
            }
        });
        /*jslint unparam: false */

        /*jslint unparam: true */
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
        /*jslint unparam: false */
    });

    it('should default to putting messages in module directory', function (done)
    {
        var fsq2 = new QlobberFSQ({ flags: flags });
        ignore_ebusy(fsq2);

        fsq2.subscribe('foo', function ()
        {
            throw new Error('should not be called');
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
            arr.length = fsq._split_topic_at + 1;
            expect(info.fname.lastIndexOf(arr.join('a') + '@', 0)).to.equal(0);
            expect(data.toString('utf8')).to.equal('bar');

            var topic_dir = path.dirname(path.dirname(info.topic_path));

            expect(topic_dir).to.equal(path.join(msg_dir, '..', 'topics'));

            fs.readFile(info.topic_path, function (err, split)
            {
                if (err) { return done(err); }

                arr.length = topic.length - fsq._split_topic_at + 1;
                expect(split.toString('utf8')).to.equal(arr.join('a'));

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
            if (err) { done(err); }
        });
    });

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
            arr.length = fsq._split_topic_at + 1;
            expect(info.fname.lastIndexOf(arr.join('a') + '@', 0)).to.equal(0);
            expect(data.toString('utf8')).to.equal('bar');

            var topic_dir = path.dirname(path.dirname(info.topic_path));

            expect(topic_dir).to.equal(path.join(msg_dir, '..', 'topics'));

            fs.readFile(info.topic_path, function (err, split)
            {
                if (err) { return done(err); }

                arr.length = topic.length - fsq._split_topic_at + 1;
                expect(split.toString('utf8')).to.equal(arr.join('a'));

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

                cb();
            });
        });

        fsq.publish(topic, 'bar', { ttl: 1000, single: true }, function (err)
        {
            if (err) { done(err); }
        });
    });

    it('should be able to change when a topic file is created', function (done)
    {
        fsq.stop_watching(function ()
        {
            var topic = 'hellofromfsq',
                fsq2 = new QlobberFSQ(
                {
                    fsq_dir: fsq_dir,
                    flags: flags,
                    split_topic_at: 5,
                    retry_interval: retry_interval
                });

            ignore_ebusy(fsq2);

            fsq2.on('start', function ()
            {
                fsq2.subscribe(topic, function (data, info)
                {
                    expect(info.topic).to.equal(topic);
                    expect(info.single).to.equal(false);
                    expect(info.path.lastIndexOf(msg_dir, 0)).to.equal(0);
                    expect(info.fname.lastIndexOf('hello@', 0)).to.equal(0);
                    expect(data.toString('utf8')).to.equal('bar');

                    var topic_dir = path.dirname(path.dirname(info.topic_path));

                    expect(topic_dir).to.equal(path.join(msg_dir, '..', 'topics'));

                    fs.readFile(info.topic_path, function (err, split)
                    {
                        if (err) { return done(err); }
                        expect(split.toString('utf8')).to.equal('fromfsq');

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

                var fsq2 = new QlobberFSQ({ fsq_dir: fsq_dir, flags: flags });
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

    it('should read single worker messages which already exist', function (done)
    {
        fsq.stop_watching(function ()
        {
            fsq.publish('foo', 'bar', { single: true }, function (err)
            {
                if (err) { return done(err); }

                var fsq2 = new QlobberFSQ({ fsq_dir: fsq_dir, flags: flags });
                ignore_ebusy(fsq2);

                /*jslint unparam: true */
                fsq2.subscribe('foo', function (data, info, cb)
                {
                    cb(null, function ()
                    {
                        fsq2.stop_watching(done);
                    });
                });
                /*jslint unparam: false */
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

                var fsq2 = new QlobberFSQ({ fsq_dir: fsq_dir, flags: flags });
                ignore_ebusy(fsq2);

                fsq2.on('start', function ()
                {
                    /*jslint unparam: true */
                    fsq2.subscribe('foo', function (data, info, cb)
                    {
                        cb(null, function ()
                        {
                            fsq2.stop_watching(done);
                        });
                    });
                    /*jslint unparam: false */
                });
            });
        });
    });

    it('should support streaming interfaces', function (done)
    {
        var stream_multi,
            stream_single,
            stream_file,
            sub_multi_called = false,
            sub_single_called = false,
            pub_multi_called = false,
            pub_single_called = false;

        function handler(stream, info, cb)
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

            var hash = crypto.createHash('sha256'),
                len = 0;

            stream.on('readable', function ()
            {
                var chunk = stream.read();

                if (chunk)
                {
                    len += chunk.length;
                    hash.update(chunk);
                }
            });

            stream.on('end', function ()
            {
                expect(len).to.equal(1024 * 1024);
                expect(hash.digest('hex')).to.equal('268e1a23a9da868b62b12e020061c98449568c4af9cf9070c8738fe1b457ed9c');

                if (pub_multi_called && pub_single_called &&
                    sub_multi_called && sub_single_called)
                {
                    cb(null, done);
                }
                else
                {
                    cb();
                }
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

        /*jslint unparam: true */
        CheckStream.prototype._write = function (chunk, encoding, callback)
        {
            this._len += chunk.length;
            this._hash.update(chunk);
            callback();
        };
        /*jslint unparam: false */

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

        /*jslint unparam: true */
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
        /*jslint unparam: false */

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
        /*jslint unparam: true */
        function handler(stream, info, cb)
        {
            expect(Buffer.isBuffer(stream)).to.equal(false);

            var hash = crypto.createHash('sha256'),
                len = 0;

            stream.on('readable', function ()
            {
                var chunk = stream.read();

                if (chunk)
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
        /*jslint unparam: false */

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
        var fsq2 = new QlobberFSQ(
        {
            fsq_dir: fsq_dir,
            flags: flags,
            poll_interval: 10 * 1000
        }), time;

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

        var fsq2 = new QlobberFSQ(
        {
            fsq_dir: fsq_dir,
            flags: flags,
            poll_interval: 10 * 1000,
            notify: false
        }), time;

        ignore_ebusy(fsq2);

        fsq2.subscribe('foo', function ()
        {
            expect(new Date().getTime() - time).to.be.at.least(fsq2._poll_interval);
            fsq2.stop_watching(done);
        });

        fsq2.on('start', function ()
        {
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
                var fsq2 = new QlobberFSQ(
                {
                    fsq_dir: fsq_dir,
                    flags: flags,
                    bucket_stamp_size: 64
                });

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

    it('should be able to change the number of random bytes at the end of filenames', function (done)
    {
        /*jslint unparam: true */
        fsq.subscribe('foo', function (data, info)
        {
            var split = info.fname.split('+'), fsq2;
            expect(split[split.length - 1].length).to.equal(32);

            fsq.stop_watching(function ()
            {
                fsq2 = new QlobberFSQ(
                {
                    fsq_dir: fsq_dir,
                    flags: flags,
                    unique_bytes: 8
                });

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
        /*jslint unparam: false */

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
                fsq2 = new QlobberFSQ(
                {
                    fsq_dir: fsq_dir,
                    flags: flags,
                    poll_interval: 10 * 1000,
                    notify: false
                });

            ignore_ebusy(fsq2);

            /*jslint unparam: true */
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
            /*jslint unparam: false */

            handler.accept_stream = true;
            
            fsq2.subscribe('foo', handler);

            fsq2.on('start', function ()
            {
                var i;

                function cb(err)
                {
                    if (err) { done(err); }
                }

                for (i = 0; i < 5; i += 1)
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
                fsq2 = new QlobberFSQ(
                {
                    fsq_dir: fsq_dir,
                    flags: flags,
                    poll_interval: 10 * 1000,
                    notify: false,
                    bucket_base: 10,
                    bucket_num_chars: 2,
                    bucket_concurrency: 5,
                    message_concurrency: 2
                });

            ignore_ebusy(fsq2);

            /*jslint unparam: true */
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
            /*jslint unparam: false */
            
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
        var num_queues = 100, //6000
            num_messages = 500;

        restore();

        this.timeout(10 * 60 * 1000);

        fsq.stop_watching(function ()
        {
            lsof.counters(function (open_before)
            {
                /*jslint unparam: true */
                async.times(num_queues, function (n, cb)
                {
                    var fsq = new QlobberFSQ({ fsq_dir: fsq_dir, flags: flags });

                    ignore_ebusy(fsq);
                    
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
                        fsq.publish('foo', 'bar', { ttl: 2 * 1000 }, cb);
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
                                        check_empty(msg_dir, done, function ()
                                        {
                                            lsof.counters(function (open_after)
                                            {
                                                expect(open_after.open).to.equal(open_before.open);
                                                done();
                                            });
                                        });
                                    });
                                }, 60 * 1000);
                            });
                        }, 2 * 1000);
                    });
                });
                /*jslint unparam: false */
            });
        });
    });

    it('should clear up expired message while worker has it locked', function (done)
    {
        this.timeout(60 * 1000);

        restore();

        /*jslint unparam: true */
        fsq.subscribe('foo', function (data, info, cb)
        {
            setTimeout(function ()
            {
                check_empty(msg_dir, done, function ()
                {
                    cb(null, done);
                });
            }, 30 * 1000);
        });
        /*jslint unparam: false */

        fsq.publish('foo', 'bar', { single: true, ttl: 500 }, function (err)
        {
            if (err) { done(err); }
        });
    });

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

                /*jslint unparam: true */
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
                        throw "called too many times";
                    }
                });
                /*jslint unparam: false */
                
                /*jslint unparam: true */
                var q = async.queue(function (task, cb)
                {
                    fsq2.publish('foo', 'bar', { ttl: timeout }, function (err)
                    {
                        if (err) { done(err); }
                        cb();
                    });
                }, 5), i;
                /*jslint unparam: false */

                for (i = 0; i < num; i += 1)
                {
                    q.push(i);
                }
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
                        fsq2 = new QlobberFSQ(
                        {
                            fsq_dir: fsq_dir,
                            flags: flags,
                            bucket_base: base,
                            bucket_num_chars: chars,
                            retry_interval: retry_interval
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
        var orig_readdir = fs.readdir, fsq2;

        /*jslint unparam: true */
        fs.readdir = function (dir, cb)
        {
            cb('dummy error');
        };
        /*jslint unparam: false */

        fsq2 = new QlobberFSQ({ fsq_dir: fsq_dir, flags: flags });
        ignore_ebusy(fsq2);

        fsq2.on('error', function (err)
        {
            expect(err).to.equal('dummy error');
            fs.readdir = orig_readdir;
            done();
        });
    });

    it('should handle read errors', function (done)
    {
        var count = 0,
            orig_createReadStream = fs.createReadStream;

        fs.createReadStream = function ()
        {
            return orig_createReadStream.call(this, '');
        };

        fsq.on('warning', function (err)
        {
            if (err && (err.code === 'ENOENT'))
            {
                count += 1;

                if (count === 5) // check single repeats
                {
                    fs.createReadStream = orig_createReadStream;
                }
            }
        });

        /*jslint unparam: true */
        fsq.subscribe('foo', function (data, info, cb)
        {
            cb(null, done);
        });
        /*jslint unparam: false */

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
        var orig_createWriteStream = fs.createWriteStream;

        fs.createWriteStream = function ()
        {
            return orig_createWriteStream.call(this, '');
        };

        fsq.publish('foo', 'bar', function (err)
        {
            expect(err.code).to.equal('ENOENT');
            fs.createWriteStream = orig_createWriteStream;
            done();
        });
    });

    // TODO:
    // - Test on CephFS when Firefly and Ubuntu 14.04 are released.
});

