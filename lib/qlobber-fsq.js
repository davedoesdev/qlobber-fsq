/**
# qlobber-fsq&nbsp;&nbsp;&nbsp;[![Build Status](https://travis-ci.org/davedoesdev/qlobber-fsq.png)](https://travis-ci.org/davedoesdev/qlobber-fsq) [![Coverage Status](https://coveralls.io/repos/davedoesdev/qlobber-fsq/badge.png?branch=master)](https://coveralls.io/r/davedoesdev/qlobber-fsq?branch=master) [![NPM version](https://badge.fury.io/js/qlobber-fsq.png)](http://badge.fury.io/js/qlobber-fsq)

Shared file system queue for Node.js.

- Supports pub-sub and work queues.
- Supports local file system for multi-core use.
- Tested with [FraunhoferFS (BeeGFS)](http://www.fhgfs.com/) and [CephFS](http://ceph.com/ceph-storage/file-system/) for distributed use.
- Highly configurable.
- Full set of unit tests, including stress tests.
- Use as a backend-less alternative to [RabbitMQ](http://www.rabbitmq.com/), [Redis pub-sub](http://redis.io/topics/pubsub) etc.
- Supports AMQP-like topics with single- and multi-level wildcards.

Example:

```javascript
var QlobberFSQ = require('qlobber-fsq').QlobberFSQ;
var fsq = new QlobberFSQ({ fsq_dir: '/shared/fsq' });
fsq.subscribe('foo.*', function (data, info)
{
    console.log(info.topic, data.toString('utf8'));
    var assert = require('assert');
    assert.equal(info.topic, 'foo.bar');
    assert.equal(data, 'hello');
});
fsq.on('start', function ()
{
    this.publish('foo.bar', 'hello');
});
```

You can publish messages using a separate process if you like:

```javascript
var QlobberFSQ = require('qlobber-fsq').QlobberFSQ;
var fsq = new QlobberFSQ({ fsq_dir: '/shared/fsq' });
fsq.stop_watching();
fsq.on('stop', function ()
{
    this.publish('foo.bar', 'hello');
});
```

Or use the streaming interface to read and write messages:

```javascript
var QlobberFSQ = require('qlobber-fsq').QlobberFSQ;
var fsq = new QlobberFSQ({ fsq_dir: '/shared/fsq' });
function handler(stream, info)
{
    var data = [];

    stream.on('readable', function ()
    {
        var chunk = stream.read();
        if (chunk)
        {
            data.push(chunk);
        }
    });

    stream.on('end', function ()
    {
        var str = Buffer.concat(data).toString('utf8');
        console.log(info.topic, str);
        var assert = require('assert');
        assert.equal(info.topic, 'foo.bar');
        assert.equal(str, 'hello');
    });
}
handler.accept_stream = true;
fsq.subscribe('foo.*', handler);
fsq.on('start', function ()
{
    fsq.publish('foo.bar').end('hello');
});
```

The API is described [here](#tableofcontents).

## Installation

```shell
npm install qlobber-fsq
```

## Limitations

- `qlobber-fsq` provides no guarantee that the order messages are given to subscribers is the same as the order in which the messages were written. If you want to maintain message order between readers and writers then you'll need to do it in your application (using ACKs, sliding windows etc).

- `qlobber-fsq` does its best not to lose messages but in exceptional circumstances (e.g. process crash, file system corruption) messages may get dropped. You should design your application to be resilient against dropped messages.

- `qlobber-fsq` makes no assurances about the security or privacy of messages in transit or at rest. It's up to your application to encrypt messages if required.

- `qlobber-fsq` supports Node 0.10 onwards.

## Distributed filesystems

Note: When using a distributed file system with `qlobber-fsq`, ensure that you synchronize the time and date on all the computers you're using.

### FraunhoferFS (BeeGFS)

When using the FraunhoferFS distributed file system, set the following options in `fhgfs-client.conf`:

```
tuneFileCacheType             = none
tuneUseGlobalFileLocks        = true
```

`qlobber-fsq` has been tested with FraunhoferFS 2014.01 on Ubuntu 14.04 and FraunhoferFS 2012.10 on Ubuntu 13.10.

### CephFS

`qlobber-fsq` has been tested with CephFS 0.80 on Ubuntu 14.04. Note that you'll need to [upgrade your kernel](http://www.yourownlinux.com/2014/04/install-upgrade-to-linux-kernel-3-14-1-in-linux.html) to at least 3.14.1 in order to get the fix for [a bug](http://tracker.ceph.com/issues/7371) in CephFS.

## How it works

![How it works](http://rawgit.davedoesdev.com/davedoesdev/qlobber-fsq/master/diagrams/how_it_works.svg)

Under the directory you specify for `fsq_dir`, `qlobber-fsq` creates the following sub-directories:

- `staging` Whilst it's being published, each message is written to a file in the staging area. The filename itself contains the message's topic, when it expires, whether it should be read by one subscriber or many and a random sequence of characters to make it unique.
- `messages` Once published to the staging area, each message is moved into this directory. `qlobber-fsq` actually creates a number of sub-directories (called buckets) under `messages` and distributes message between buckets according to the hash of their filenames. This helps to reduce the number of directory entries that have to be read when a single message is written. 
- `topics` If a message's topic is long, a separate topic file is created for it in this directory.
- `update` This contains one file, `UPDATE`, which is updated with a random sequence of bytes (called a stamp) every time a message is moved into the `messages` directory. `UPDATE` contains a separate stamp for each bucket.

`qlobber-fsq` reads `UPDATE` at regular intervals to determine whether a new message has been written to a bucket. If it has then it processes each filename in the bucket's directory listing.

If the expiry time in the filename has passed then it deletes the message.

If the filename indicates the message can be read by many subscribers:

- If it's processed this filename before then stop processing this filename.
- If the topic in the filename matches any subscribers then call each subscriber with the file's content. It uses [`qlobber`](https://github.com/davedoesdev/qlobber) to pattern match topics to subscribers.
- Remember that we've processed the filename.

If the filename indicates the message can be read by only one subscriber (i.e. work queue semantics):

- Try to lock the file using `flock`. If it fails to lock the file then stop processing this filename.
- If the topic in the filename matches any subscribers then call one subscriber with the file's content.
- Truncate and delete the file before unlocking it. We truncate the file in case of directory caching.

## Licence

[MIT](LICENCE)

## Test

To run the default tests:

```shell
grunt test [--fsq-dir <path>]
```

If you don't specify `--fsq-dir` then the default will be used (a directory named `fsq` in the `qlobber-fsq` module directory).

To run the stress tests (multiple queues in a single Node process):

```shell
grunt test-stress [--fsq-dir <path>]
```

To run the multi-process tests (each process publishing and subscribing to different messages):

```shell
grunt test-multi [--fsq-dir <path>] [--queues <number of queues>]
```

If you omit `--queues` then one process will be created per core (detected with [`os.cpus()`](http://nodejs.org/api/os.html#os_os_cpus)).

To run the distributed tests (one Node process per remote host, each one publishing and subscribing to different messages);

```shell
grunt test-multi --fsq-dir <path> --remote <host1> --remote <host2>
```

You can specify as many remote hosts as you like. The test uses [`cp-remote`](https://github.com/davedoesdev/cp-remote) to run a module on each remote host. Make sure on each host:

- The `qlobber-fsq` module is installed at the same location.
- Mount the same distributed file system on the directory you specify for `--fsq-dir`. FraunhoferFS is the only distributed file system currently supported.

## Lint

```shell
grunt lint
```

## Code Coverage

```shell
grunt coverage [--fsq-dir <path>]
```

[Instanbul](http://gotwarlost.github.io/istanbul/) results are available [here](http://rawgit.davedoes.com/davedoesdev/qlobber-fsq/master/coverage/lcov-report/index.html).

Coveralls page is [here](https://coveralls.io/r/davedoesdev/qlobber-fsq).

## Benchmarks

To run the benchmark:

```shell
grunt bench [--fsq-dir <path>] \
            --rounds <number of rounds> \
            --size <message size> \
            --ttl <message time-to-live in seconds> \
            (--queues <number of queues> | \
             --remote <host1> --remote <host2> ...)
```

If you don't specify `--fsq-dir` then the default will be used (a directory named `fsq` in the `qlobber-fsq` module directory).

If you provide at least one `--remote <host>` argument then the benchmark will be distributed across multiple hosts using [`cp-remote`](https://github.com/davedoesdev/cp-remote). Make sure on each host:

- The `qlobber-fsq` module is installed at the same location.
- Mount the same distributed file system on the directory you specify for `--fsq-dir`. FraunhoferFS is the only distributed file system currently supported.

# API
*/
/*jslint node: true, nomen: true, bitwise: true, unparam: true */
"use strict";

require('graceful-fs');

var fs = require('fs-ext'),
    stream = require('stream'),
    path = require('path'),
    crypto = require('crypto'),
    util = require('util'),
    events = require('events'),
    constants = require('constants'),
    async = require('async'),
    Qlobber = require('qlobber').Qlobber;

function CollectStream()
{
    stream.Writable.call(this);
    
    this.setMaxListeners(0);

    this._chunks = [];
    this._len = 0;

    var ths = this;

    this.on('finish', function ()
    {
        ths.emit('buffer', Buffer.concat(ths._chunks, ths._len));
    });
}

util.inherits(CollectStream, stream.Writable);

CollectStream.prototype._write = function (chunk, encoding, callback)
{
    this._chunks.push(chunk);
    this._len += chunk.length;
    callback();
};

/*function sum(buf, start, end)
{
    var i, r = 0;

    for (i = start; i < end; i += 1)
    {
        r += buf[i];
    }

    return r;
}*/

/**
Creates a new `QlobberFSQ` object for publishing and subscribing to a file system queue.

@constructor

@param {Object} [options] Configures the file system queue. Valid properties are listed below:

  - `{String} fsq_dir` The path to the file system queue directory. Note that the following sub-directories will be created under this directory if they don't exist: `messages`, `staging`, `topics` and `update`. Defaults to a directory named `fsq` in the `qlobber-fsq` module directory.

  - `{Integer} split_topic_at` Maximum number of characters in a short topic. Short topics are contained entirely in a message's filename. Long topics are split so the first `split_topic_at` characters go in the filename and the rest are written to a separate file in the `topics` sub-directory. Obviously long topics are less efficient. Defaults to 200, which is the maximum for most common file systems. Note: if your `fsq_dir` is on an [`ecryptfs`](http://ecryptfs.org/) file system then you should set `split_topic_at` to 100.

  - `{Integer} bucket_base`, `{Integer} bucket_num_chars` Messages are distributed across different _buckets_ for efficiency. Each bucket is a sub-directory of the `messages` directory. The number of buckets is determined by the `bucket_base` and `bucket_num_chars` options. `bucket_base` is the radix to use for bucket names and `bucket_num_chars` is the number of digits in each name. For example, `bucket_base: 26` and `bucket_num_chars: 4` results in buckets `00` through `pppp`. Defaults to `base_base: 16` and `bucket_num_chars: 2` (i.e. buckets `00` through `ff`).

  - `{Integer} bucket_stamp_size` The number of bytes to write to the `UPDATE` file when a message is published. The `UPDATE` file (in the `update` directory) is used to determine whether any messages have been published without having to scan all the bucket directories. Each bucket has a section in the `UPDATE` file, `bucket_stamp_size` bytes long. When a message is written to a bucket, its section is filled with random bytes. Defaults to 32.

  - `{Integer} flags` Extra flags to use when reading and writing files. You shouldn't need to use this option but if you do then it should be a bitwise-or of values in the (undocumented) Node `constants` module (e.g. `constants.O_DIRECT | constants.O_SYNC`). Defaults to 0.

  - `{Integer} unique_bytes` Number of random bytes to append to each message's filename (encoded in hex), in order to avoid name clashes. Defaults to 16. If you increase it (or change the algorithm to add some extra information like the hostname), be sure to reduce `split_topic_at` accordingly.

  - `{Integer} single_ttl` Default time-to-live (in milliseconds) for messages which should be read by at most one subscriber. This value is added to the current time and the resulting expiry time is put into the message's filename. After the expiry time, the message is ignored and deleted when convenient. Defaults to 1 hour. 

  - `{Integer} multi_ttl` Default time-to-live (in milliseconds) for messages which can be read by many subscribers. This value is added to the current time and the resulting expiry time is put into the message's filename. After the expiry time, the message is ignored and deleted when convenient. Defaults to 5 seconds.

  - `{Integer} poll_interval` `qlobber-fsq` reads the `UPDATE` file at regular intervals to check whether any messages have been written. `poll_interval` is the time (in milliseconds) between each check. Defaults to 1 second.

  - `{Boolean} notify` Whether to use [`fs.watch`](http://nodejs.org/api/fs.html#fs_fs_watch_filename_options_listener) to watch for changes to the `UPDATE` file. Note that this will be done in addition to reading it every `poll_interval` milliseconds because `fs.watch` (`inotify` underneath) can be unreliable, especially under high load. Defaults to `true`.

  - `{Integer} retry_interval` Some I/O operations can fail with an error indicating they should be retried. `retry_interval` is the time (in milliseconds) to wait before retrying. Dfaults to 1 second.

  - `{Integer} message_concurrency` The number of messages in each bucket to process at once. Defaults to 1.

  - `{Integer} bucket_concurrency` The number of buckets to process at once. Defaults to 1.

  - `{Boolean} dedup` Whether to ensure each handler function is called at most once when a message is received. Defaults to `true`.

  - `{String} separator` The character to use for separating words in message topics. Defaults to `.`.

  - `{String} wildcard_one` The character to use for matching exactly one word in a message topic to a subscriber. Defaults to `*`.

  - `{String} wildcard_some` The character to use for matching zero or more words in a message topic to a subscriber. Defaults to `#`.

  - `{Function (info, handlers, cb(err, ready, filtered_handlers))} filter` Function called before each message is processed. You can use this to filter the subscribed handler functions to be called for the message (by passing the filtered list as the third argument to `cb`). If you want to ignore the message _at this time_ then pass `false` as the second argument to `cb`. `filter` will be called again later with the same message. Defaults to a function which calls `cb(null, true, handlers)`.
*/
function QlobberFSQ(options)
{
    events.EventEmitter.call(this);

    options = options || {};

    this._fsq_dir = options.fsq_dir || path.join(__dirname, '..', 'fsq');
    this._msg_dir = this._fsq_dir + path.sep + 'messages';
    this._topic_dir = this._fsq_dir + path.sep + 'topics';
    this._staging_dir = this._fsq_dir + path.sep + 'staging';
    this._update_dir = this._fsq_dir + path.sep + 'update';
    this._update_fname = this._update_dir + path.sep + 'UPDATE';
    
    this._split_topic_at = options.split_topic_at || 200;

    this._bucket_base = options.bucket_base || 16;
    this._bucket_num_chars = options.bucket_num_chars || 2;
    this._bucket_stamp_size = options.bucket_stamp_size || 32;

    this._flags = options.flags || 0;

    this._unique_bytes = options.unique_bytes || 16;

    this._single_ttl = options.single_ttl || (60 * 60 * 1000); // 1 hour
    this._multi_ttl = options.multi_ttl || (5 * 1000); // 5 seconds

    this._poll_interval = options.poll_interval || 1000; // 1 second
    this._retry_interval = options.retry_interval || 1000; // 1 second

    this._message_concurrency = options.message_concurrency || 1;
    this._bucket_concurrency = options.bucket_concurrency || 1;

    this._do_dedup = options.dedup === undefined ? true : options.dedup;

    this._filter = options.filter || function (info, handlers, cb)
    {
        cb(null, true, handlers);
    };

    this._matcher = new Qlobber(options);
    this._dedup = '__fsq_dedup' + crypto.randomBytes(16).toString('base64');
    this._destr = '__fsq_destr' + crypto.randomBytes(16).toString('base64');

    this._leading_byte = new Buffer(1);

    var ths = this,
        caches = {},
        pending = {},
        initialized = false,
        count,
        num_buckets,
        buckets = [],
        dirs = [this._fsq_dir,
                this._staging_dir,
                this._update_dir,
                this._msg_dir,
                this._topic_dir];

    this._error = function (err)
    {
        if (err)
        {
            var i, silent = false;

            for (i = 1; i < arguments.length; i += 1)
            {
                if (err.code === arguments[i])
                {
                    silent = true;
                    break;
                }
            }

            if ((!silent) && !ths.emit('warning', err))
            {
                console.error(err);
            }
        }

        return err;
    };

    this._stopped = false;
    this._active = true;

    this._chkstop = function ()
    {
        if (this._stopped && this._active)
        {
            this._active = false;
            this.emit('stop');
        }

        return this._stopped;
    };

    this._try_again = function (err)
    {
        // graceful-fs takes care of EAGAIN
        return err && (err.code === 'EBUSY'); 
    };

    function emit_error(err)
    {
        ths._active = false;
        ths.emit('error', err);
    }

    function parse_fname(bucket, fname, cb)
    {
        var at_pos = fname.lastIndexOf('@'), metadata, info;

        if (at_pos < 0) { return cb(); }

        metadata = fname.substr(at_pos + 1).split('+');
        if (metadata.length !== 4) { return cb(); }

        info = {
            fname: fname,
            path: ths._msg_dir + path.sep + bucket + path.sep + fname,
            topic: fname.substr(0, at_pos),
            expires: parseInt(metadata[1], 16),
            single: metadata[2] === 's'
        };

        if (metadata[0] === 's')
        {
            return cb(info);
        }

        info.topic_path = ths._topic_dir + path.sep + bucket + path.sep + fname;

        fs.readFile(info.topic_path, { flag: constants.O_RDONLY | ths._flags },
        function (err, split)
        {
            if (ths._error(err)) { return cb(); }
            info.topic += split.toString('utf8');
            cb(info);
        });
    }

    function close(fd, err, cb)
    {
        fs.close(fd, function (err2)
        {
            ths._error(err2);

            if (ths._try_again(err2))
            {
                return setTimeout(function ()
                {
                    close(fd, err, cb);
                }, ths._retry_interval);
            }

            if (cb) { cb(err || err2); }
        });
    }

    function unlock_and_close(fd, err, cb)
    {
        // close should be enough but just in case
        fs.flock(fd, 'un', function (err2)
        {
            ths._error(err2);

            if (ths._try_again(err2))
            {
                return setTimeout(function ()
                {
                    unlock_and_close(fd, err, cb);
                }, ths._retry_interval);
            }

            close(fd, err || err2, cb);
        });
    }

    function unlink_unlock_and_close(info, fd, err, cb)
    {
        fs.unlink(info.path, function (err2)
        {
            // Ignore EBUSY, rely on truncate having happened.
            // When we see it again and can't read a byte, we'll unlink again.

            ths._error(err2, 'ENOENT');

            if (err2 && (err2.code === 'ENOENT'))
            {
                err2 = null;
            }

            if (!info.topic_path)
            {
                return unlock_and_close(fd, err || err2, cb);
            }

            fs.unlink(info.topic_path, function (err3)
            {
                ths._error(err3, 'ENOENT');

                if (err3 && (err3.code === 'ENOENT'))
                {
                    err3 = null;
                }

                unlock_and_close(fd, err || err2 || err3, cb);
            });
        });
    }

    function collect(handler, s, info, cb)
    {
        var cs = s[ths._destr];

        if (!cs)
        {
            cs = new CollectStream();
            s[ths._destr] = cs;
            s.pipe(cs);
        }

        cs.on('buffer', function (buf)
        {
            if (ths._chkstop()) { return; }
            handler(buf, info, cb);
        });
    }

    function call_handlers(handlers, info, cb)
    {
        //console.log('call_handlers', require('os').hostname(), info.topic, handlers.length);

        if ((handlers.length === 0) || ths._chkstop()) { return cb(); }

        fs.open(info.path,
                (info.single ? constants.O_RDWR : constants.O_RDONLY) | ths._flags,
        function (err, fd)
        {
            function multi_callback(err, cb)
            {
                ths._error(err);
                if (cb) { cb(); }
                ths._chkstop();
            }

            var called = false;

            function single_callback(err, cb)
            {
                if (called) { return multi_callback(err, cb); }
                called = true;

                function cb2(err)
                {
                    if (cb) { cb(err); }
                    ths._chkstop();
                }

                function truncate()
                {
                    fs.ftruncate(fd, 0, function (err)
                    {
                        ths._error(err);

                        if (ths._try_again(err))
                        {
                            return setTimeout(truncate, ths._retry_interval);
                        }

                        //console.log('truncated', info.fname);

                        unlink_unlock_and_close(info, fd, err, cb2);
                    });
                }

                if (ths._error(err))
                {
                    unlock_and_close(fd, null, cb2);
                }
                else
                {
                    truncate();
                }
            }

            function read()
            {
                var stream = fs.createReadStream(null,
                    {
                        fd: fd,
                        autoClose: false,
                        start: 1
                    }),
                    hcb = info.single ? single_callback : multi_callback,
                    i,
                    handler;

                stream.setMaxListeners(0);

                for (i = 0; i < handlers.length; i += 1)
                {
                    handler = handlers[i];

                    if (handler.accept_stream)
                    {
                        handler(stream, info, hcb);
                    }
                    else
                    {
                        collect(handler, stream, info, hcb);
                    }
                }

                stream.once('end', function ()
                {
                    if (info.single)
                    {
                        cb();
                    }
                    else
                    {
                        close(fd, null, cb);
                    }
                });

                stream.once('error', function (err)
                {
                    ths._error(err);

                    if (info.single)
                    {
                        unlock_and_close(fd, null, cb);
                    }
                    else
                    {
                        close(fd, null, cb);
                    }
                });
            }

            if (ths._error(err, 'ENOENT'))
            {
                if (ths._chkstop()) { return cb(); }
                return cb(null, err.code === 'ENOENT');
            }

            if (ths._chkstop()) { return close(fd, null, cb); }

            if (info.single)
            {
                fs.flock(fd, 'exnb', function (err)
                {
                    if (ths._error(err, 'EAGAIN') || ths._chkstop())
                    {
                        return close(fd, null, cb);
                    }

                    //console.log('locked', info.fname);

                    var stream = fs.createReadStream(null,
                        {
                            fd: fd,
                            autoClose: false,
                            start: 0,
                            end: 1
                        }),
                        got_data = false;

                    stream.on('readable', function ()
                    {
                        var data = stream.read();
                        if (data)
                        {
                            got_data = data.length > 0;
                        }
                    });

                    stream.once('end', function ()
                    {
                        if (got_data)
                        {
                            read();
                        }
                        else
                        {
                            unlink_unlock_and_close(info, fd, null, cb);
                        }
                    });

                    stream.once('error', function (err)
                    {
                        ths._error(err);
                        unlock_and_close(fd, null, cb);
                    });
                });
            }
            else
            {
                read();
            }
        });
    }

    function make_info_handler(cache, cache2, pending2)
    {
        return function (info, next)
        {
            if (ths._chkstop()) { return next('stopped'); }

            var now = Date.now(),
                prev_delay = ths._delay,
                handlers,
                handlers2,
                not_seen,
                i,
                h,
                dedup = {};

            function cb(err, not_found, is_pending)
            {
                ths._error(err);

                if (not_found || is_pending)
                {
                    delete cache2[info.fname];
                }
                else
                {
                    cache2[info.fname] = info;
                }

                if ((info.single || is_pending) && !not_found)
                {
                    pending2.push(info);
                }

                setImmediate(next);
            }

            if (info.expires <= now)
            {
                return fs.unlink(info.path, function (err)
                {
                    ths._error(err, 'ENOENT');

                    // Assume file has been removed. If it hasn't and we see it
                    // again then it will be expired and we'll unlink again.

                    if (!info.topic_path)
                    {
                        return cb(null, true);
                    }

                    fs.unlink(info.topic_path, function (err)
                    {
                        ths._error(err, 'ENOENT');
                        cb(null, true);
                    });
                });
            }

            handlers = ths._matcher.match(info.topic);
            not_seen = initialized && !cache[info.fname];

            if (not_seen)
            {
                //console.log('not_seen', require('os').hostname(), info.fname, handlers.length);
                ths._delay = 0;
            }
            /*else
            {
                console.log('seen', require('os').hostname(), info.fname, handlers.length);
            }*/

            if (ths._do_dedup)
            {
                handlers2 = [];

                for (i = 0; i < handlers.length; i += 1)
                {
                    h = handlers[i];

                    if (h[ths._dedup] !== dedup)
                    {
                        h[ths._dedup] = dedup;
                        handlers2.push(h);
                    }
                }

                handlers = handlers2;
            }

            if (info.single)
            {
                ths._filter(info, handlers, function (err, ready, handlers)
                {
                    ths._error(err);

                    if (!ready)
                    {
                        ths._delay = prev_delay;
                    }
                    else if (handlers.length > 0)
                    {
                        return call_handlers([handlers[0]], info, cb);
                    }

                    cb();
                });
            }
            else if (not_seen)
            {
                ths._filter(info, handlers, function (err, ready, handlers)
                {
                    ths._error(err);

                    if (!ready)
                    {
                        ths._delay = prev_delay;
                        cb(null, false, true);
                    }
                    else
                    {
                        call_handlers(handlers, info, cb);
                    }
                });
            }
            else
            {
                cb();
            }
        };
    }

    function make_fname_handler(cache, cache2, pending2)
    {
        var info_handler = make_info_handler(cache, cache2, pending2);

        return function (fname, next)
        {
            if (ths._chkstop()) { return next('stopped'); }

            function cb(info)
            {
                if (ths._chkstop()) { return next('stopped'); }
                if (!info) { return setImmediate(next); }
                info_handler(info, next);
            }

            var info = cache && cache[fname];

            if (info)
            {
                return cb(info);
            }

            parse_fname(cache2.bucket, fname, cb);
        };
    }

    function handle(files, cache, cache2, make_handler, cb)
    {
        var pending2 = [];

        function cb2()
        {
            if (ths._chkstop()) { return; }
            cb(cache2, pending2);
        }

        if (files.length === 0)
        {
            return setImmediate(cb2);
        }

        async.eachLimit(files,
                        ths._message_concurrency,
                        make_handler(cache, cache2, pending2),
                        cb2);
    }

    this._poll = function ()
    {
        ths._timeout = null;
        if (ths._chkstop()) { return; }

        ths._delay = ths._poll_interval;

        fs.readFile(ths._update_fname,
                    { flag: constants.O_RDONLY | ths._flags },
        function (err, update)
        {
            ths._error(err);
            if (ths._chkstop()) { return; }

            if (err || (update.length < ths._update_size))
            {
                ths._timeout = setTimeout(ths._poll, ths._poll_interval);
                return;
            }

            async.eachLimit(buckets, ths._bucket_concurrency,
            function (bucket, next)
            {
                var start = bucket * ths._bucket_stamp_size,
                    end = (bucket + 1) * ths._bucket_stamp_size,
                    cache,
                    bucket_fmt;

                //console.log('upsum', require('os').hostname(), sum(update, start, end), bucket);                

                if (update.slice(start, end).equals(
                        ths._last_update.slice(start, end)))
                {
                    //console.log('no update', require('os').hostname(), bucket);

                    cache = caches[bucket];

                    handle(pending[bucket], cache, cache, make_info_handler,
                    function (cache2, pending2)
                    {
                        pending[bucket] = pending2;
                        next();
                    });
                }
                else
                {
                    //console.log('update', require('os').hostname(), bucket);

                    bucket_fmt = ths._format_bucket(bucket);

                    fs.readdir(ths._msg_dir + path.sep + bucket_fmt,
                    function (err, files)
                    {
                        ths._error(err);
                        if (ths._chkstop()) { return; }
                        if (err) { return next(); }

                        handle(files, caches[bucket], { bucket: bucket_fmt}, make_fname_handler,
                        function (cache2, pending2)
                        {
                            update.copy(ths._last_update, start, start, end);
                            caches[bucket] = cache2;
                            pending[bucket] = pending2;
                            next();
                        });
                    });
                }
            }, function ()
            {
                if (ths._chkstop()) { return; }

                if (!initialized)
                {
                    initialized = true;
                    ths.emit('start');
                }

                if (!ths._chkstop())
                {
                    ths._timeout = setTimeout(ths._poll, ths._delay);
                }
            });
        });
    };

    this._format_bucket = function (b)
    {
        var bs = ths._bucket_base > 1 ? b.toString(ths._bucket_base) : '0',
            arr = [];
        arr.length = ths._bucket_num_chars + 1;
        return (arr.join('0') + bs).slice(-ths._bucket_num_chars);
    };

    num_buckets = Math.pow(this._bucket_base, this._bucket_num_chars);

    for (count = 0; count < num_buckets; count += 1)
    {
        buckets[count] = count;
    }

    this._update_size = num_buckets * this._bucket_stamp_size;
    this._last_update = crypto.randomBytes(this._update_size);

    process.nextTick(function ()
    {
        fs.readdir(ths._msg_dir, function (err, msg_dirs)
        {
            if (err && (err.code !== 'ENOENT')) { return emit_error(err); }

            var existing_msg_dirs = {};

            if (msg_dirs)
            {
                msg_dirs.forEach(function (d)
                {
                    existing_msg_dirs[d] = true;
                });
            }

            fs.readdir(ths._topic_dir, function (err, topic_dirs)
            {
                if (err && (err.code !== 'ENOENT')) { return emit_error(err); }

                var existing_topic_dirs = {}, i, b;

                if (topic_dirs)
                {
                    topic_dirs.forEach(function (d)
                    {
                        existing_topic_dirs[d] = true;
                    });
                }

                for (i = 0; i < num_buckets; i += 1)
                {
                    b = ths._format_bucket(i);

                    if (!existing_msg_dirs[b])
                    {
                        dirs.push(ths._msg_dir + path.sep + b);
                    }

                    if (!existing_topic_dirs[b])
                    {
                        dirs.push(ths._topic_dir + path.sep + b);
                    }
                }

                async.eachSeries(dirs, function (dir, next)
                {
                    fs.mkdir(dir, function (err)
                    {
                        next(err && (err.code !== 'EEXIST') ? err : null);
                    });
                }, function (err)
                {
                    if (err) { return emit_error(err); }

                    fs.writeFile(ths._update_fname,
                                 crypto.randomBytes(ths._update_size),
                                 { flag: constants.O_CREAT |
                                         constants.O_WRONLY | 
                                         ths._flags },
                    function (err)
                    {
                        //console.log('wrote update file', require('os').hostname());

                        if (err) { return emit_error(err); }
                        if (ths._chkstop()) { return; }

                        ths._timeout = setTimeout(ths._poll, 0);

                        if (options.notify !== false)
                        {
                            try
                            {
                                ths._watcher = fs.watch(ths._update_dir, function (event)
                                {
                                    if (event === 'change')
                                    {
                                        ths.refresh_now();
                                    }
                                });
                            }
                            catch (err2)
                            {
                                ths._stop_timeout();
                                emit_error(err2);
                            }
                        }
                    });
                });
            });
        });
    });
}

util.inherits(QlobberFSQ, events.EventEmitter);

/**
Subscribe to messages in the file system queue.

@param {String} topic Which messages you're interested in receiving. Message topics are split into words using `.` as the separator. You can use `*` to match exactly one word in a topic or `#` to match zero or more words. For example, `foo.*` would match `foo.bar` whereas `foo.#` would match `foo`, `foo.bar` and `foo.bar.wup`. Note you can change the separator and wildcard characters by specifying the `separator`, `wildcard_one` and `wildcard_some` options when [constructing `QlobberFSQ` objects](#qlobberfsqoptions). See the [`qlobber` documentation](https://github.com/davedoesdev/qlobber#qlobberoptions) for more information.

@param {Function} handler Function to call when a new message is received on the file system queue and its topic matches against `topic`. `handler` will be passed the following arguments:

  - `{Readable|Buffer} data` [Readable](http://nodejs.org/api/stream.html#stream_class_stream_readable) stream or message content as a [Buffer](http://nodejs.org/api/buffer.html#buffer_class_buffer). By default you'll receive the message content. If `handler` has a property `accept_stream` set to a truthy value then you'll receive a stream. Note that _all_ subscribers will receive the same stream or content for each message. You should take this into account when reading from the stream. The stream can be piped into multiple [Writable](http://nodejs.org/api/stream.html#stream_class_stream_writable) streams but bear in mind it will go at the rate of the slowest one.

  - `{Object} info` Metadata for the message, with the following properties:

    - `{String} fname` Name of the file in which the message is stored.
    - `{String} path` Full path to the file in which the message is stored.
    - `{String} topic` Topic the message was published with.
    - `{String} [topic_path]` Full path to the file in which the topic overspill is stored (only present if the topic is too long to fit in the file name).
    - `{Integer} expires` When the message expires (number of milliseconds after 1 January 1970 00:00:00 UTC).
    - `{Boolean} single` Whether this message is being given to at most one subscriber (across all `QlobberFSQ` objects).

  - `{Function} done` Function to call once you've handled the message. Note that calling this function is only mandatory if `info.single === true`, in order to delete and unlock the file. `done` takes two arguments:

    - `{Object} err` If an error occurred then pass details of the error, otherwise pass `null` or `undefined`.
    - `{Function} [finish]` Optional function to call once the message has been deleted and unlocked, in the case of `info.single === true`, or straight away otherwise. It will be passed the following argument:
      - `{Object} err` If an error occurred then details of the error, otherwise `null`.

@param {Function} cb Function to call once the subscription has been registered. This will be passed the following argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`.
*/
QlobberFSQ.prototype.subscribe = function (topic, handler, cb)
{
    this._matcher.add(topic, handler);
    if (cb) { cb(); }
};

/**
Unsubscribe from messages in the file system queue.

@param {String} [topic] Which messages you're no longer interested in receiving via the `handler` function. This should be a topic you've previously passed to [`subscribe`](#qlobberfsqprototypesubscribetopic-handler-cb). If topic is `undefined` then all handlers for all topics are unsubscribed.

@param {Function} [handler] The function you no longer want to be called with messages published to the topic `topic`. This should be a function you've previously passed to [`subscribe`](#qlobberfsqprototypesubscribetopic-handler-cb). If you subscribed `handler` to a different topic then it will still be called for messages which match that topic. If `handler` is undefined, all handlers for the topic `topic` are unsubscribed.

@param {Function} cb Function to call once `handler` has been unsubscribed from `topic`. This will be passed the following argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`.
*/
QlobberFSQ.prototype.unsubscribe = function (topic, handler, cb)
{
    if (typeof topic === 'function')
    {
        cb = topic;
        topic = undefined;
        handler = undefined;
    }

    if (topic === undefined)
    {
        this._matcher.clear();
    }
    else if (handler === undefined)
    {
        this._matcher.remove(topic);
    }
    else
    {
        this._matcher.remove(topic, handler);
    }

    if (cb) { cb(); }
};

function default_hasher(fname)
{
    var h = crypto.createHash('md5'); // not for security, just mapping!
    h.update(fname);
    return h.digest();
}

/**
Publish a message to the file system queue.

@param {String} topic Message topic. The topic should be a series of words separated by `.` (or the `separator` character you provided to the [`QlobberFSQ constructor`](#qlobberfsqoptions)). Since the unencoded topic string is used as part of the message's filename, topic words can contain any valid file name character for your file system. However, it's probably sensible to limit it to alphanumeric characters, `-`, `_` and `.`.

@param {String|Buffer} [payload] Message payload. If you don't pass a payload then `publish` will return a [Writable stream](http://nodejs.org/api/stream.html#stream_class_stream_writable) for you to write the payload into.

@param {Object} [options] Optional settings for this publication:

  - `{Boolean} single` If `true` then the message will be given to _at most_ one interested subscriber, across all `QlobberFSQ` objects scanning the file system queue. Otherwise all interested subscribers will receive the message.

  - `{Integer} ttl` Time-to-live (in milliseconds) for this message. If you don't specify anything then `single_ttl` or `multi_ttl` (provided to the [`QlobberFSQ constructor`](#qlobberfsqoptions)) will be used, depending on the value of `single`. After the time-to-live for the message has passed, the message is ignored and deleted when convenient.

  - `{String} encoding` If `payload` is a string, the encoding to use when writing it out to the message file. Defaults to `utf8`.

  - `{Integer} mode` The file mode (permissions) to set on the message file. Defaults to octal `0666` (readable and writable to everyone).

  - `{Function} hasher` A hash function to use for deciding into which bucket the message should be placed. The hash function should return a `Buffer` at least 4 bytes long. It defaults to running `md5` on the message file name. If you supply a `hasher` function it will be passed the following arguments:

    - `{String} fname` Message file name.
    - `{Integer} expires` When the message expires (number of milliseconds after 1 January 1970 00:00:00 UTC).
    - `{String} topic` Message topic.
    - `{String|Buffer} payload` Message payload.
    - `{Object} options` The optional settings for this publication.

@param {Function} [cb] Optional function to call once the message has been written to the file system queue. This will be called after the message has been moved into its bucket and is therefore available to subscribers in any `QlobberFSQ` object scanning the queue. It will be passed the following argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`.

@return {Stream|undefined} A [Writable stream](http://nodejs.org/api/stream.html#stream_class_stream_writable) if no `payload` was passed, otherwise `undefined`.
*/
QlobberFSQ.prototype.publish = function (topic, payload, options, cb)
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

    var ths = this,
        write_options = { flags: constants.O_TRUNC |
                                 constants.O_CREAT |
                                 constants.O_WRONLY |
                                 this._flags },
        now = Date.now(),
        expires = now + (options.ttl || (options.single ? this._single_ttl :
                                                          this._multi_ttl)),
        split = topic.substr(this._split_topic_at),
        fname = topic.substr(0, this._split_topic_at) + '@' +
                (split ? 'l' : 's') + '+' +
                expires.toString(16) + '+' +
                (options.single ? 's' : 'm') + '+' +
                crypto.randomBytes(this._unique_bytes).toString('hex'),
        staging_fname = this._staging_dir + path.sep + fname,
        bucket,
        msg_fname,
        topic_fname,
        stream,
        was_error = false,
        hasher = options.hasher || default_hasher;

    write_options.flag = write_options.flags;
    write_options.encoding = options.encoding || 'utf8';
    write_options.mode = options.mode || 438; // 0666

    bucket = this._format_bucket(hasher(
            fname, expires, topic, payload, options).readUInt32BE(0));
    msg_fname = this._msg_dir + path.sep + bucket + path.sep + fname;
    topic_fname = this._topic_dir + path.sep + bucket + path.sep + fname;
    bucket = this._bucket_base > 1 ? parseInt(bucket, this._bucket_base) : 0;

    function errored(err)
    {
        if (err)
        {
            ths._error(err);
            fs.unlink(staging_fname, function (err2)
            {
                ths._error(err2, 'ENOENT');
                fs.unlink(topic_fname, function (err3)
                {
                    ths._error(err3, 'ENOENT');
                    if (cb) { cb(err || err2 || err3); }
                });
            });
            return true;
        }

        return false;
    }

    function rename_and_update()
    {
        fs.rename(staging_fname, msg_fname, function (err)
        {
            if (ths._try_again(err))
            {
                ths._error(err);
                return setTimeout(rename_and_update, ths._retry_interval);
            }

            if (errored(err)) { return; }

            var update_stream = fs.createWriteStream(ths._update_fname,
            {
                flags: constants.O_CREAT |
                       constants.O_WRONLY |
                       ths._flags,
                start: bucket * ths._bucket_stamp_size
            }), update_was_error = false;

            update_stream.once('error', function (err)
            {
                update_was_error = true;
                errored(err);
            });

            update_stream.once('close', function ()
            {
                if (cb && !update_was_error) { cb(null, msg_fname); }
            });
            
            update_stream.once('open', function ()
            {
                update_stream.end(crypto.randomBytes(ths._bucket_stamp_size));
            });
        });
    }

    function maybe_split()
    {
        if (!split)
        {
            return rename_and_update();
        }

        write_options.encoding = 'utf8';

        fs.writeFile(topic_fname, split, write_options, function (err)
        {
            if (errored(err)) { return; }
            rename_and_update();
        });
    }

    stream = fs.createWriteStream(staging_fname, write_options);

    stream.once('error', function (err)
    {
        was_error = true;
        errored(err);
    });

    stream.once('close', function ()
    {
        if (!was_error)
        {
            maybe_split();
        }
    });

    stream.write(ths._leading_byte);

    if ((typeof payload !== 'string') && !Buffer.isBuffer(payload))
    {
        return stream;
    }

    stream.once('open', function ()
    {
        stream.end(payload);
    });
};

QlobberFSQ.prototype._stop_timeout = function ()
{
    if (this._timeout)
    {
        clearTimeout(this._timeout);
        this._timeout = null;
        return true;
    }

    return false;
};

/**
Stop scanning for new messages.

@param {Function] [cb] Optional function to call once scanning has stopped. Alternatively, you can listen for the [`stop` event](#qlobberfsqeventsstop).
*/
QlobberFSQ.prototype.stop_watching = function (cb)
{
    if (this._stopped)
    {
        if (cb)
        {
            cb.call(this);
        }
        return;
    }
    this._stopped = true;

    if (this._watcher)
    {
        this._watcher.close();
        this._watcher = null;
    }

    if (this._stop_timeout())
    {
        var ths = this;
        setImmediate(function () { ths._chkstop(); });
    }

    if (cb)
    {
        if (this._active)
        {
            this.once('stop', cb);
        }
        else
        {
            cb.call(this);
        }
    }
};

/**
Check the `UPDATE` file now rather than waiting for the next periodic check to occur
*/
QlobberFSQ.prototype.refresh_now = function ()
{
    if (this._stopped) { return; }

    if (this._stop_timeout())
    {
        this._poll();
    }
    else
    {
        this._delay = 0;
    }
};

/**
Scan for new messages in the `messages` sub-directory without checking whether the `UPDATE` file has changed.
*/
QlobberFSQ.prototype.force_refresh = function ()
{
   this._last_update = crypto.randomBytes(this._update_size);
   this.refresh_now();
};

exports.QlobberFSQ = QlobberFSQ;

