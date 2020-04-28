/**
# qlobber-fsq&nbsp;&nbsp;&nbsp;[![Build Status](https://travis-ci.org/davedoesdev/qlobber-fsq.png)](https://travis-ci.org/davedoesdev/qlobber-fsq) [![Build status](https://ci.appveyor.com/api/projects/status/k7x8kh1m64vu52ej?svg=true)](https://ci.appveyor.com/project/davedoesdev/qlobber-fsq) [![Coverage Status](https://coveralls.io/repos/davedoesdev/qlobber-fsq/badge.png?branch=master)](https://coveralls.io/r/davedoesdev/qlobber-fsq?branch=master) [![NPM version](https://badge.fury.io/js/qlobber-fsq.png)](http://badge.fury.io/js/qlobber-fsq)

Shared file system queue for Node.js.

- **Note:** Version 9 can use [shared memory LMAX Disruptors](https://github.com/davedoesdev/shared-memory-disruptor) to speed things up on a single multi-core server.
- Supports pub-sub and work queues.
- Supports local file system for multi-core use.
- Tested with [FraunhoferFS (BeeGFS)](http://www.fhgfs.com/) and [CephFS](http://ceph.com/ceph-storage/file-system/) for distributed use.
- **Note:** An alternative module, [`qlobber-pg`](https://github.com/davedoesdev/qlobber-pg), can be used when you need access from multiple hosts. It's API-compatible with `qlobber-fsq` and requires a PostgreSQL database.
- Highly configurable.
- Full set of unit tests, including stress tests.
- Use as a backend-less alternative to [RabbitMQ](http://www.rabbitmq.com/), [Redis pub-sub](http://redis.io/topics/pubsub) etc.
- Supports AMQP-like topics with single- and multi-level wildcards.
- Tested on Linux and Windows.

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
        var chunk = this.read();
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

- `qlobber-fsq` provides no guarantee that the order messages are given to subscribers is the same as the order in which the messages were written. If you want to maintain message order between readers and writers then you'll need to do it in your application (using ACKs, sliding windows etc). Alternatively, use the `order_by_expiry` [constructor](#qlobberfsqoptions) option to have messages delivered in order of the time they expire.

- `qlobber-fsq` does its best not to lose messages but in exceptional circumstances (e.g. process crash, file system corruption) messages may get dropped. You should design your application to be resilient against dropped messages.

- `qlobber-fsq` makes no assurances about the security or privacy of messages in transit or at rest. It's up to your application to encrypt messages if required.

- `qlobber-fsq` supports Node 6 onwards.

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
grunt test [--fsq-dir=<path>] [--getdents_size=<buffer size>] [--disruptor]
```

If you don't specify `--fsq-dir` then the default will be used (a directory named `fsq` in the `test` directory).

If you specify `--getdents_size` then use of [`getdents`](https://github.com/davedoesdev/getdents) will be included in the tests.

If you specify `--disruptor` then use of [shared memory LMAX Disruptors](https://github.com/davedoesdev/shared-memory-disruptor) will be included in the tests.

To run the stress tests (multiple queues in a single Node process):

```shell
grunt test-stress [--fsq-dir=<path>] [--disruptor]
```

To run the multi-process tests (each process publishing and subscribing to different messages):

```shell
grunt test-multi [--fsq-dir=<path>] [--queues=<number of queues>] [--disruptor]
```

If you omit `--queues` then one process will be created per core (detected with [`os.cpus()`](http://nodejs.org/api/os.html#os_os_cpus)).

To run the distributed tests (one process per remote host, each one publishing and subscribing to different messages):

```shell
grunt test-multi --fsq-dir=<path> --remote=<host1> --remote=<host2>
```

You can specify as many remote hosts as you like. The test uses [`cp-remote`](https://github.com/davedoesdev/cp-remote) to run a module on each remote host. Make sure on each host:

- The `qlobber-fsq` module is installed at the same location.
- Mount the same distributed file system on the directory you specify for `--fsq-dir`. FraunhoferFS and CephFS are the only distributed file systems currently supported.

Please note the distributed tests don't run on Windows.

## Lint

```shell
grunt lint
```

## Code Coverage

```shell
grunt coverage [--fsq-dir=<path>]
```

[Instanbul](http://gotwarlost.github.io/istanbul/) results are available [here](http://rawgit.davedoesdev.com/davedoesdev/qlobber-fsq/master/coverage/lcov-report/index.html).

Coveralls page is [here](https://coveralls.io/r/davedoesdev/qlobber-fsq).

## Benchmarks

To run the benchmark:

```shell
grunt bench [--fsq-dir=<path>] \
            --rounds=<number of rounds> \
            --size=<message size> \
            --ttl=<message time-to-live in seconds> \
            [--disruptor] \
            [--num_elements=<number of disruptor elements>] \
            [--element_size=<disruptor element size>] \
            [--bucket_stamp_size=<number of bytes to write to UPDATE file] \
            [--getdents_size=<buffer size>] \
            [--ephemeral] \
            [--refresh_ttl=<period between expiration check in seconds>] \
            (--queues=<number of queues> | \
             --remote=<host1> --remote=<host2> ...)
```

If you don't specify `--fsq-dir` then the default will be used (a directory named `fsq` in the `bench` directory).

If you provide at least one `--remote=<host>` argument then the benchmark will be distributed across multiple hosts using [`cp-remote`](https://github.com/davedoesdev/cp-remote). Make sure on each host:

- The `qlobber-fsq` module is installed at the same location.
- Mount the same distributed file system on the directory you specify for `--fsq-dir`. FraunhoferFS and CephFS are the only distributed file systems currently supported.

# API
*/
/*jslint node: true, nomen: true, bitwise: true, unparam: true */
"use strict";

var stream = require('stream'),
    path = require('path'),
    crypto = require('crypto'),
    util = require('util'),
    events = require('events'),
    constants = require('constants'),
    async = require('async'),
    wu = require('wu'),
    semver = require('semver'),
    qlobber = require('qlobber'),
    Qlobber = qlobber.Qlobber,
    QlobberDedup = qlobber.QlobberDedup;

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

- `{Boolean} encode_topics` Whether to hex-encode message topics. Because topic strings form part of message filenames, they're first hex-encoded. If you can ensure that your message topics contain only valid filename characters, set this to `false` to skip encoding.

- `{Integer} split_topic_at` Maximum number of characters in a short topic. Short topics are contained entirely in a message's filename. Long topics are split so the first `split_topic_at` characters go in the filename and the rest are written to a separate file in the `topics` sub-directory. Obviously long topics are less efficient. Defaults to 200, which is the maximum for most common file systems. Note: if your `fsq_dir` is on an [`ecryptfs`](http://ecryptfs.org/) file system then you should set `split_topic_at` to 100.

- `{Integer} bucket_base`, `{Integer} bucket_num_chars` Messages are distributed across different _buckets_ for efficiency. Each bucket is a sub-directory of the `messages` directory. The number of buckets is determined by the `bucket_base` and `bucket_num_chars` options. `bucket_base` is the radix to use for bucket names and `bucket_num_chars` is the number of digits in each name. For example, `bucket_base: 26` and `bucket_num_chars: 4` results in buckets `0000` through `pppp`. Defaults to `base_base: 16` and `bucket_num_chars: 2` (i.e. buckets `00` through `ff`). The number of buckets is available as the `num_buckets` property of the `QlobberFSQ` object.

- `{Integer} bucket_stamp_size` The number of bytes to write to the `UPDATE` file when a message is published. The `UPDATE` file (in the `update` directory) is used to determine whether any messages have been published without having to scan all the bucket directories. Each bucket has a section in the `UPDATE` file, `bucket_stamp_size` bytes long. When a message is written to a bucket, its section is filled with random bytes. Defaults to 32. If you set this to 0, the `UPDATE` file won't be written to and all the bucket directories will be scanned even if no messages have been published.

- `{Integer} flags` Extra flags to use when reading and writing files. You shouldn't need to use this option but if you do then it should be a bitwise-or of values in the (undocumented) Node `constants` module (e.g. `constants.O_DIRECT | constants.O_SYNC`). Defaults to 0.

- `{Integer} unique_bytes` Number of random bytes to append to each message's filename (encoded in hex), in order to avoid name clashes. Defaults to 16. If you increase it (or change the algorithm to add some extra information like the hostname), be sure to reduce `split_topic_at` accordingly.

- `{Integer} single_ttl` Default time-to-live (in milliseconds) for messages which should be read by at most one subscriber. This value is added to the current time and the resulting expiry time is put into the message's filename. After the expiry time, the message is ignored and deleted when convenient. Defaults to 1 hour.

- `{Integer} multi_ttl` Default time-to-live (in milliseconds) for messages which can be read by many subscribers. This value is added to the current time and the resulting expiry time is put into the message's filename. After the expiry time, the message is ignored and deleted when convenient. Defaults to 1 minute.

- `{Integer} poll_interval` `qlobber-fsq` reads the `UPDATE` file at regular intervals to check whether any messages have been written. `poll_interval` is the time (in milliseconds) between each check. Defaults to 1 second.

- `{Boolean} notify` Whether to use [`fs.watch`](http://nodejs.org/api/fs.html#fs_fs_watch_filename_options_listener) to watch for changes to the `UPDATE` file. Note that this will be done in addition to reading it every `poll_interval` milliseconds because `fs.watch` (`inotify` underneath) can be unreliable, especially under high load. Defaults to `true`.

- `{Integer} retry_interval` Some I/O operations can fail with an error indicating they should be retried. `retry_interval` is the time (in milliseconds) to wait before retrying. Defaults to 1 second.

- `{Integer} message_concurrency` The number of messages in each bucket to process at once. Defaults to 1.

- `{Integer} bucket_concurrency` The number of buckets to process at once. Defaults to 1.

- `{Integer} handler_concurrency` By default, a message is considered handled by a subscriber only when all its data has been read. If you set `handler_concurrency` to non-zero, a message is considered handled as soon as a subscriber receives it. The next message will then be processed straight away. The value of `handler-concurrency` limits the number of messages being handled by subscribers at any one time. Defaults to 0 (waits for all message data to be read).

- `{Boolean} order_by_expiry` Pass messages to subscribers in order of their expiry time. If `true` then `bucket_base` and `bucket_num_chars` are forced to 1 so messages are written to a single bucket. Defaults to `false`.

- `{Boolean} dedup` Whether to ensure each handler function is called at most once when a message is received. Defaults to `true`.

- `{Boolean} single` Whether to process messages meant for _at most_ one subscriber (across all `QlobberFSQ` objects), i.e. work queues. This relies on the optional dependency [`fs-ext`](https://github.com/baudehlo/node-fs-ext). Defaults to `true` if `fs-ext` is installed, otherwise `false` (in which case a [`single_disabled`](#qlobberfsqeventssingle_disablederr) event will be emitted).

- `{String} separator` The character to use for separating words in message topics. Defaults to `.`.

- `{String} wildcard_one` The character to use for matching exactly one word in a message topic to a subscriber. Defaults to `*`.

- `{String} wildcard_some` The character to use for matching zero or more words in a message topic to a subscriber. Defaults to `#`.

- `{Integer} getdents_size` If positive, use [`getdents`](https://github.com/davedoesdev/getdents) to enumerate messages in bucket directories. `getdents_size` is the buffer size to use with `getdents`. Otherwise, use [`fs.readdir`](https://nodejs.org/api/fs.html#fs_fs_readdir_path_options_callback) (which is the default). If `getdents` is requested but unavailable, a [`getdents_disabled`](#qlobberfsqeventsgetdents_disablederr) event will be emitted.

- `{Function (info, handlers, cb(err, ready, filtered_handlers)) | Array} filter` Function called before each message is processed.

  - You can use this to filter the subscribed handler functions to be called for the message (by passing the filtered list as the third argument to `cb`).

  - If you want to ignore the message _at this time_ then pass `false` as the second argument to `cb`. `filter` will be called again later with the same message.
  - Defaults to a function which calls `cb(null, true, handlers)`.

  - `handlers` is an ES6 Set, or array if `options.dedup` is falsey.

  - `filtered_handlers` should be an ES6 Set, or array if `options.dedup` is falsey. If not, `new Set(filtered_handlers)` or `Array.from(filtered_handlers)` will be used to convert it.

  - You can supply an array of filter functions - each will be called in turn with the `filtered_handlers` from the previous one.

  - An array containing the filter functions is also available as the `filters` property of the `QlobberFSQ` object and can be modified at any time.

- `{Function (bucket)} get_disruptor` You can speed up message processing on a single multi-core server by using [shared memory LMAX Disruptors](https://github.com/davedoesdev/shared-memory-disruptor). Message metadata and (if it fits) payload will be send through the Disruptor. `get_disruptor` will be called for each bucket number and should return the Disruptor to use for that bucket or `null`. The same bucket can be used for more than one bucket if you wish.

- `{Integer} refresh_ttl` If you use a shared memory LMAX Disruptor for a bucket (see `get_disruptor` above), notification of new messages in the bucket is received through the Disruptor. However, checking for expired messages still needs to read the filesystem. `refresh_ttl` is the time (in milliseconds) between checking for expired messages when a Disruptor is in use. Defaults to 10 seconds.

- `{Integer} disruptor_spin_interval` If a Disruptor is shared across multiple buckets or multiple `QlobberFSQ` instances, contention can occur when publishing a message. In this case [`publish`](#qlobberfsqprototypepublishtopic-payload-options-cb) will try again until it succeeds. `disruptor_spin_interval` is the time (in milliseconds) to wait before retrying. Defaults to 0.
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

    this._encode_topics = options.encode_topics === undefined ? true : options.encode_topics;
    this._split_topic_at = options.split_topic_at || 200;

    this._bucket_base = options.bucket_base || 16;
    this._bucket_num_chars = options.bucket_num_chars || 2;
    this._bucket_stamp_size = options.bucket_stamp_size === undefined ? 32 : options.bucket_stamp_size;

    this._flags = options.flags || 0;

    this._unique_bytes = options.unique_bytes || 16;

    this._single_ttl = options.single_ttl || (60 * 60 * 1000); // 1 hour
    this._multi_ttl = options.multi_ttl || (60 * 1000); // 1 minute

    this._poll_interval = options.poll_interval || 1000; // 1 second
    this._retry_interval = options.retry_interval || 1000; // 1 second

    this._disruptor_spin_interval = options.disruptor_spin_interval === undefined ? 0 : options.disruptor_spin_interval;

    this._message_concurrency = options.message_concurrency || 1;
    this._bucket_concurrency = options.bucket_concurrency || 1;

    this._order_by_expiry = options.order_by_expiry;
    if (this._order_by_expiry)
    {
        this._bucket_base = 1;
        this._bucket_num_chars = 1;
    }

    this._do_dedup = options.dedup === undefined ? true : options.dedup;
    this._do_single = options.single === undefined ? true : options.single;

    if (this._do_dedup)
    {
        this._matcher = new QlobberDedup(options);
    }
    else
    {
        this._matcher = new Qlobber(options);
    }

    this._matcher_marker = {};

    this._extra_matcher = null;

    this._leading_byte = Buffer.from([0]);

    this._disruptors = [];

    this.num_buckets = QlobberFSQ.get_num_buckets(this._bucket_base,
                                                  this._bucket_num_chars);

    this._last_refreshed = new Map();

    var ths = this,
        delivered = new Map(),
        pending = new Map(),
        bucket_formats = [],
        handler_queue,
        refresh_ttl = options.refresh_ttl === undefined ?
                            10 * 1000 : options.refresh_ttl,
        dirs = [this._fsq_dir,
                this._staging_dir,
                this._update_dir,
                this._msg_dir,
                this._topic_dir];

    this._ensure_extra_matcher = function ()
    {
        if (!ths._extra_matcher)
        {
            if (ths._do_dedup)
            {
                ths._extra_matcher = new QlobberDedup(options);
            }
            else
            {
                ths._extra_matcher = new Qlobber(options);
            }

            ths._extra_matcher._extra_handlers = new Map();
            ths._extra_matcher._matcher_markers = new Map();
            ths._extra_matcher._not_seen = new Map();
        }

        return ths._extra_matcher;
    };

    this.filters = options.filter || [];

    if (typeof this.filters[Symbol.iterator] !== 'function')
    {
        this.filters = [this.filters];
    }

    function filter(info, handlers, cb)
    {
        function next(i)
        {
            return function (err, ready, handlers)
            {
                if (handlers)
                {
                    if (ths._do_dedup)
                    {
                        if (!(handlers instanceof Set))
                        {
                            handlers = new Set(handlers);
                        }
                    }
                    else if (!Array.isArray(handlers))
                    {
                        handlers = Array.from(handlers);
                    }
                }

                if (err || !ready || (i === ths.filters.length))
                {
                    return cb(err, ready, handlers);
                }

                ths.filters[i].call(ths, info, handlers, next(i + 1));
            };
        }

        next(0)(null, true, handlers);
    }

    function copy(handlers)
    {
        return ths._do_dedup ? new Set(handlers) : Array.from(handlers);
    }

    this._error = function (err)
    {
        if (err)
        {
            var i, silent = false;

            for (i = 1; i < arguments.length; i += 1)
            {
                if (arguments[i] && (err.code === arguments[i]))
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

    this.stopped = false;
    this.active = true;
    this.initialized = false;

    this._chkstop = function ()
    {
        if (ths.stopped && ths.active)
        {
            ths.active = false;
            ths.emit('stop');
        }

        return ths.stopped;
    };

    this._try_again = function (err)
    {
        // graceful-fs takes care of EAGAIN
        return err && (err.code === 'EBUSY');
    };

    function emit_error(err)
    {
        ths.active = false;
        ths.emit('error', err);
    }

    function decode_topic(info)
    {
        if (ths._encode_topics)
        {
            info.topic = Buffer.from(info.topic, 'hex').toString();
        }
    }

    function parse_fname(bucket_fmt, fname, cb)
    {
        var at_pos = fname.lastIndexOf('@'), metadata, info;

        if (at_pos < 0) { return cb(); }

        metadata = fname.substr(at_pos + 1).split('+');
        if (metadata.length !== 4) { return cb(); }

        info = {
            fname: fname,
            path: ths._msg_dir + path.sep + bucket_fmt + path.sep + fname,
            topic: fname.substr(0, at_pos),
            expires: parseInt(metadata[1], 16),
            single: metadata[2] === 's'
        };

        if (metadata[0] === 's')
        {
            decode_topic(info);
            return cb(info);
        }

        info.topic_path = ths._topic_dir + path.sep + bucket_fmt + path.sep + fname;

        ths._fs.readFile(info.topic_path,
                         { flag: constants.O_RDONLY | ths._flags },
        function (err, split)
        {
            if (ths._error(err, 'ENOENT')) { return cb(); }
            info.topic += split.toString('utf8');
            decode_topic(info);
            cb(info);
        });
    }

    function close(fd, err, cb)
    {
        ths._fs.close(fd, function (err2)
        {
            ths._error(err2);

            if (ths._try_again(err2))
            {
                return setTimeout(close, ths._retry_interval, fd, err, cb);
            }

            if (cb) { cb(err || err2); }
        });
    }

    function unlock_and_close(fd, err, cb)
    {
        // close should be enough but just in case
        ths._fsext.flock(fd, 'un', function (err2)
        {
            if (ths._error(err2, 'EAGAIN') &&
                (ths._try_again(err2) || (err2.code === 'EAGAIN')))
            {
                return setTimeout(unlock_and_close,
                                  ths._retry_interval,
                                  fd,
                                  err,
                                  cb);
            }

            close(fd, err || err2, cb);
        });
    }

    function unlink(info, err, cb)
    {
        ths._fs.unlink(info.path, function (err2)
        {
            // Ignore EBUSY, rely on truncate having happened.
            // When we see it again and it's expired or we can't read a byte,
            // we'll unlink again.

            ths._error(err2,
                       'ENOENT',
                       (process.platform === 'win32') ? 'EPERM' : null);

            if (err2 &&
                ((err2.code === 'ENOENT') ||
                 ((process.platform === 'win32') &&
                  (err2.code === 'EPERM'))))
            {
                err2 = null;
            }

            if (!info.topic_path)
            {
                return cb(err || err2);
            }

            ths._fs.unlink(info.topic_path, function (err3)
            {
                ths._error(err3,
                           'ENOENT',
                           (process.platform === 'win32') ? 'EPERM' : null);

                if (err3 &&
                    ((err3.code === 'ENOENT') ||
                     ((process.platform === 'win32') &&
                      (err3.code === 'EPERM'))))
                {
                    err3 = null;
                }

                cb(err || err2 || err3);
            });
        });
    }

    function unlink_unlock_and_close(info, fd, err, cb)
    {
        if (process.platform === 'win32')
        {
            // Windows can't unlink while the file is open.
            // Another reader opening the file between us closing and
            // unlinking it will find it empty.

            return unlock_and_close(fd, err, function (err2)
            {
                unlink(info, err2, cb);
            });
        }

        unlink(info, err, function (err2)
        {
            unlock_and_close(fd, err || err2, cb);
        });
    }

    function collected(handler, info, cb)
    {
        return function (buf)
        {
            if (ths._chkstop()) { return; }
            handler.call(ths, buf, info, cb);
        };
    }

    function call_handlers2(handlers, info, cb)
    {
        //console.log('call_handlers', require('os').hostname(), info.topic, handlers.length);

        var called = false,
            done_err = null,
            waiting = [],
            len = ths._do_dedup ? handlers.size : handlers.length;

        if ((len === 0) || ths._chkstop()) { return done(); }

        function done_ne(err)
        {
            var was_waiting = waiting;
            waiting = [];

            if (was_waiting.length > 0)
            {
                process.nextTick(function ()
                {
                    for (var f of was_waiting)
                    {
                        f(err);
                    }
                });
            }

            var was_called = called;
            called = true;
            done_err = err;

            if (ths._chkstop() || was_called) { return; }

            cb();
        }

        function done(err)
        {
            ths._error(err);
            done_ne(err);
        }

        function wait_for_done(err, cb)
        {
            ths._error(err);

            if (cb)
            {
                if (called)
                {
                    cb(done_err);
                }
                else
                {
                    waiting.push(cb);
                }
            }
        }

        function deliver_message(make_stream, data, fd)
        {
            var stream,
                cstream,
                hcb,
                delivered_stream = false;

            function deliver_stream()
            {
                var destroyed = false;

                function destroy(err)
                {
                    // We can't call stream.destroy() because it closes the
                    // file descriptor and we want to close it ourselves
                    // (we might want to truncate and unlock it first).

                    destroyed = stream.destroyed = true;

                    if (err)
                    {
                        stream.emit('error', err);
                    }
                }

                function common_callback(err, cb)
                {
                    if (destroyed)
                    {
                        wait_for_done(err, cb);
                        return null;
                    }

                    destroy(err);
                    
                    function cb2(err)
                    {
                        if (cb)
                        {
                            process.nextTick(cb, err);
                        }
                        stream.push(null);
                        // From Node 10, 'readable' not emitted after destroyed.
                        // 'end' is emitted though (at least for now); if this
                        // changes then we could emit 'end' here instead.
                        stream.emit('readable');
                        done(err);
                    }

                    if (fd < 0)
                    {
                        cb2();
                        return null;
                    }

                    return cb2;
                }

                function multi_callback(err, cb)
                {
                    var cb2 = common_callback(err, cb);
                    if (!cb2) { return; }

                    close(fd, null, cb2);
                }

                function single_callback(err, cb)
                {
                    var cb2 = common_callback(err, cb);
                    if (!cb2) { return; }

                    function truncate()
                    {
                        ths._fs.ftruncate(fd, 0, function (err)
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

                    if (err)
                    {
                        return unlock_and_close(fd, null, cb2);
                    }

                    truncate();
                }

                stream.setMaxListeners(0);

                stream.once('end', function ()
                {
                    if (info.single)
                    {
                        return done();
                    }

                    if (destroyed) { return; }
                    destroyed = stream.destroyed = true;

                    if (fd < 0)
                    {
                        return done();
                    }

                    close(fd, null, done);
                });

                stream.on('error', function (err)
                {
                    ths._error(err);

                    if (destroyed) { return; }
                    destroyed = stream.destroyed = true;

                    function cb(err)
                    {
                        stream.push(null);
                        done(err);
                    }

                    if (fd < 0)
                    {
                        return cb();
                    }

                    if (info.single)
                    {
                        return unlock_and_close(fd, null, cb);
                    }

                    close(fd, null, cb);
                });

                var hcb = info.single ? single_callback : multi_callback;
                hcb.num_handlers = len;
                return hcb;
            }

            function ensure_stream()
            {
                if (!stream)
                {
                    stream = make_stream();
                    hcb = deliver_stream();
                }

                return stream;
            }

            function unpipe()
            {
                stream.unpipe(cstream);
            }

            for (var handler of handlers)
            {
                if (handler.accept_stream)
                {
                    handler.call(ths, ensure_stream(), info, hcb);
                    delivered_stream = true;
                }
                else if (data)
                {
                    wait_for_done.num_handlers = len;
                    handler.call(ths, data, info, wait_for_done);
                }
                else
                {
                    if (!cstream)
                    {
                        cstream = new CollectStream();
                        ensure_stream().pipe(cstream);
                        stream.on('error', unpipe);
                    }

                    cstream.on('buffer', collected(handler, info, hcb));

                    delivered_stream = true;
                }
            }

            if (!delivered_stream)
            {
                // an unsubscribe might have modified handlers
                // or we don't have a stream to wait for
                if (fd < 0)
                {
                    return done();
                }
                close(fd, null, done);
            }
        }

        if (info.data)
        {
            return process.nextTick(function ()
            {
                if (ths._chkstop()) { return done(); }

                deliver_message(function ()
                {
                    var s = new stream.PassThrough();            
                    s.end(info.data);
                    return s;
                }, info.data, -1);
            });
        }

        ths._fs.open(info.path,
                     (info.single ? constants.O_RDWR : constants.O_RDONLY) | ths._flags,
        function (err, fd)
        {
            function read2()
            {
                deliver_message(function ()
                {
                    return ths._fs.createReadStream(null,
                    {
                        fd: fd,
                        autoClose: false,
                        start: 1
                    });
                }, null, fd);
            }

            function read()
            {
                if (info.size !== undefined)
                {
                    return read2();
                }

                ths._fs.fstat(fd, function (err, stats)
                {
                    if (ths._error(err) || ths._chkstop())
                    {
                        return close(fd, null, done);
                    }

                    info.size = Math.max(stats.size - 1, 0);
                    read2();
                });
            }

            if ((info.single &&
                 err && (err.code === 'EPERM') &&
                 (process.platform === 'win32')) ||
                ths._error(err, 'ENOENT'))
            {
                return done_ne(err);
            }

            if (ths._chkstop()) { return close(fd, null, done); }

            if (info.single)
            {
                ths._fsext.flock(fd, 'exnb', function (err)
                {
                    if (ths._error(err, 'EAGAIN', 'EWOULDBLOCK') ||
                        ths._chkstop())
                    {
                        return close(fd, null, done);
                    }

                    //console.log('locked', info.fname);

                    var stream = ths._fs.createReadStream(null,
                        {
                            fd: fd,
                            autoClose: false,
                            start: 0,
                            end: 0
                        }),
                        got_data = 0;

                    stream.on('readable', function ()
                    {
                        var data = this.read();
                        if (data)
                        {
                            got_data += data.length;
                        }
                    });

                    stream.once('end', function ()
                    {
                        if (ths._chkstop())
                        {
                            unlock_and_close(fd, null, done);
                        }
                        else if (got_data > 0)
                        {
                            read();
                        }
                        else
                        {
                            unlink_unlock_and_close(info, fd, null, done);
                        }
                    });

                    stream.once('error', function (err)
                    {
                        ths._error(err);
                        unlock_and_close(fd, null, done);
                    });
                });
            }
            else
            {
                read();
            }
        });
    }

    if (options.handler_concurrency)
    {
        handler_queue = async.queue(function (task, cb)
        {
            setImmediate(task.cb);
            call_handlers2(task.handlers, task.info, cb);
        }, options.handler_concurrency);
    }

    function call_handlers(handlers, info, cb)
    {
        if (handler_queue)
        {
            handler_queue.push(
            {
                handlers: handlers,
                info: info,
                cb: cb
            });
        }
        else
        {
            call_handlers2(handlers, info, cb);
        }
    }

    function handle_info(delivered,
                         delivered2,
                         pending2,
                         extra_matcher,
                         only_check_expired,
                         info,
                         next)
    {
        if (ths._chkstop() || !info) { return setImmediate(next); }

        var now = Date.now(),
            reset_delay = false,
            extra_handlers,
            has_extra_handlers = false,
            prev_extra_handlers,
            not_seen,
            handlers,
            matcher_marker,
            h,
            info2;

        function cb(not_found, is_pending)
        {
            if (reset_delay)
            {
                ths._delay = 0;
            }

            if (delivered2)
            {
                if (not_found || is_pending)
                {
                    delivered2.delete(info.fname);
                }
                else
                {
                    delivered2.add(info.fname);
                }
            }

            if ((info.single || is_pending) && !not_found)
            {
                pending2.push(info.data ? info : info.fname);
            }

            setImmediate(next);
        }

        if (info.expires <= now)
        {
            return unlink(info, null, function ()
            {
                cb(true);
            });
        }

        if (only_check_expired)
        {
            return cb();
        }

        if (delivered)
        {
            // New message if not in cache. If we're intializing, we just want
            // to store the message in the cache so we know what's existing.
            not_seen = ths.initialized && !delivered.has(info.fname);
        }
        else
        {
            // No cache. We're either called with file on disk to get existing
            // (extra_matcher true) or new (extra_matcher false).
            // Multi messages are always new, single messages are only new
            // the first time we process them.
            not_seen = ths.initialized &&
                       (info.new || !info.single) &&
                       !extra_matcher;
        }

        if (not_seen)
        {
            //console.log('not_seen', require('os').hostname(), info.fname, handlers.length);
            reset_delay = true;
        }
        /*else
        {
            console.log('seen', require('os').hostname(), info.fname, handlers.length);
        }*/

        if (info.single)
        {
            if (!ths._do_single)
            {
                return cb();
            }

            handlers = copy(ths._matcher.match(info.topic));

            info2 = not_seen ? info : Object.assign(
            {
                existing: true
            }, info);

            return filter(info2, handlers, function (err, ready, handlers)
            {
                ths._error(err);

                if (!ready)
                {
                    reset_delay = false;
                }
                else if (ths._do_dedup)
                {
                    if (handlers.size > 0)
                    {
                        return call_handlers(new Set([handlers.values().next().value]),
                                             info2,
                                             cb);
                    }
                }
                else if (handlers.length > 0)
                {
                    return call_handlers([handlers[0]], info2, cb);
                }

                cb();
            });
        }

        if (not_seen)
        {
            handlers = copy(ths._matcher.match(info.topic));
            info2 = info;
            if (ths._extra_matcher)
            {
                ths._extra_matcher._not_seen.set(info.path, copy(handlers));
            }
        }
        else if (extra_matcher)
        {
            // Need to get existing messages
            extra_handlers = extra_matcher.match(info.topic);
            has_extra_handlers = (ths._do_dedup ?
                    extra_handlers.size : extra_handlers.length) > 0;
            prev_extra_handlers =
                    extra_matcher._extra_handlers.get(info.path);
            if (prev_extra_handlers)
            {
                // We had handlers to receive this as an existing message
                if ((extra_matcher._matcher_markers.get(info.path) ===
                     ths._matcher_marker) &&
                    !has_extra_handlers)
                {
                    // No unsubscribe happened and no new handlers
                    extra_handlers = prev_extra_handlers;
                }
                else
                {
                    // Add previous handlers to new ones
                    extra_handlers = copy(extra_handlers);
                    handlers = ths._matcher.match(info.topic);
                    for (h of prev_extra_handlers)
                    {
                        if (ths._do_dedup)
                        {
                            if (handlers.has(h))
                            {
                                extra_handlers.add(h);
                            }
                        }
                        else if (handlers.indexOf(h) >= 0)
                        {
                            extra_handlers.push(h);
                        }
                    }
                }
                extra_matcher._extra_handlers.delete(info.path);
                extra_matcher._matcher_markers.delete(info.path);
            }

            // Make a copy of handlers, filtering out any we've already
            // delivered as new while extra_matcher was waiting to be processed
            handlers = ths._do_dedup ? new Set() : [];
            has_extra_handlers = false;

            for (h of extra_handlers)
            {
                const not_seen = extra_matcher._not_seen.get(info.path);
                if (not_seen && not_seen.has(h))
                {
                    continue;
                }

                if (ths._do_dedup)
                {
                    handlers.add(h);
                }
                else
                {
                    handlers.push(h);
                }

                has_extra_handlers = true;
            }

            if (!has_extra_handlers)
            {
                return cb();
            }

            // Handlers may be modified by filter functions (below) but we
            // may also need to store them in _extra_handlers (also below)
            // so make another copy
            extra_handlers = copy(handlers);

            // If message is delayed by filter, remember marker so we know
            // if any unsubscribe happened
            matcher_marker = ths._matcher_marker;

            info2 = Object.assign(
            {
                existing: true
            }, info);
        }
        else
        {
            return cb();
        }

        filter(info2, handlers, function (err, ready, handlers)
        {
            ths._error(err);

            if (ready)
            {
                return call_handlers(handlers, info2, cb);
            }

            reset_delay = false;

            if (has_extra_handlers)
            {
                // Remember handlers for existing messages
                var em = ths._ensure_extra_matcher();
                em._extra_handlers.set(info.path, extra_handlers);
                em._matcher_markers.set(info.path, matcher_marker);
            }

            cb(false, not_seen);
        });
    }

    function handle_fname(delivered,
                          delivered2,
                          pending2,
                          bucket_fmt,
                          extra_matcher,
                          only_check_expired,
                          fname,
                          next)
    {
        if (ths._chkstop()) { return setImmediate(next); }

        parse_fname(bucket_fmt, fname, function (info)
        {
            handle_info(delivered,
                        delivered2,
                        pending2,
                        extra_matcher,
                        only_check_expired,
                        info,
                        next);
        });
    }

    function make_info_handler(delivered,
                               delivered2,
                               pending2,
                               bucket_fmt,
                               extra_matcher,
                               only_check_expired)
    {
        return function (info, next)
        {
            handle_info(delivered,
                        delivered2,
                        pending2,
                        extra_matcher,
                        only_check_expired,
                        info,
                        next);
        };
    }

    function make_fname_handler(delivered,
                                delivered2,
                                pending2,
                                bucket_fmt,
                                extra_matcher,
                                only_check_expired)
    {
        return function (fname, next)
        {
            handle_fname(delivered,
                         delivered2,
                         pending2,
                         bucket_fmt,
                         extra_matcher,
                         only_check_expired,
                         fname,
                         next);
        };
    }

    function make_mixed_handler(delivered,
                                delivered2,
                                pending2,
                                bucket_fmt,
                                extra_matcher,
                                only_check_expired)
    {
        return function (info, next)
        {
            if (info.fname)
            {
                return handle_info(delivered,
                                   delivered2,
                                   pending2,
                                   extra_matcher,
                                   only_check_expired,
                                   info,
                                   next);
            }

            return handle_fname(delivered,
                                delivered2,
                                pending2,
                                bucket_fmt,
                                extra_matcher,
                                only_check_expired,
                                info,
                                next);
        };
    }

    function handle(infos,
                    delivered,
                    delivered2,
                    pending2,
                    bucket_fmt,
                    extra_matcher,
                    only_check_expired,
                    make_handler,
                    cb)
    {
        function cb2()
        {
            if (ths._chkstop()) { return; }
            process.nextTick(cb);
        }

        function cb3()
        {
            async.eachLimit(infos,
                            ths._message_concurrency,
                            make_handler(delivered,
                                         delivered2,
                                         pending2,
                                         bucket_fmt,
                                         extra_matcher,
                                         only_check_expired),
                            cb2);
        }

        if (ths._order_by_expiry)
        {
            return async.mapLimit(
                infos,
                ths._message_concurrency,
                function (info, cb)
                {
                    if (info.data)
                    {
                        return process.nextTick(cb, null, info);
                    }

                    parse_fname(bucket_fmt, info, function (pinfo)
                    {
                        process.nextTick(cb, null, pinfo);
                    });
                },
                function (err, pinfos)
                {
                    async.sortBy(pinfos, function (info, next)
                    {
                        process.nextTick(next, null, info ? info.expires : 0);
                    }, function (err, sinfos)
                    {
                        infos = sinfos;
                        make_handler = make_info_handler;
                        cb3();
                    });
                });
        }

        cb3();
    }

    function process_pending(bucket, bucket_fmt, extra_matcher, cache, cb)
    {
        // Note we never call with extra_matcher true and cache false
        var d = cache ? delivered.get(bucket) : null,
            infos = extra_matcher ?
                        // 1. Ensure pending picked up as existing
                        // 2. Ensure pending are processed (otherwise
                        //    any pending not delivered will be lost)
                        // This supports adding to pending outside polling but
                        // we don't do that currently
                        wu.chain(d, pending.get(bucket)) :
                        pending.get(bucket),
            pending2 = [];

        pending.set(bucket, pending2);

        handle(infos,
               d,
               d,
               pending2,
               bucket_fmt,
               extra_matcher,
               false,
               make_mixed_handler,
               cb);
    }

    function process_all_readdir(bucket,
                                 bucket_fmt,
                                 extra_matcher,
                                 cache,
                                 only_check_expired,
                                 cb)
    {
        ths._fs.readdir(ths._msg_dir + path.sep + bucket_fmt,
        function (err, files)
        {
            ths._error(err);
            if (ths._chkstop()) { return; }
            if (err) { return cb(err); }

            var d = cache ? delivered.get(bucket) : null,
                d2 = cache ? new Set() : null,
                pending2 = [];

            if (cache)
            {
                delivered.set(bucket, d2);
            }

            pending.set(bucket, pending2);

            handle(files,
                   d,
                   d2,
                   pending2,
                   bucket_fmt,
                   extra_matcher,
                   only_check_expired,
                   make_fname_handler,
                   cb);
        });
    }

    function polled()
    {
        if (ths._chkstop()) { return; }

        if (!ths.initialized)
        {
            ths.initialized = true;
            ths.emit('start');
        }

        if (!ths._chkstop())
        {
            ths._timeout = setTimeout(ths._poll, ths._delay);
        }
    }

    function read_update(cb)
    {
        if (ths._update_size === 0)
        {
            return cb();
        }

        ths._fs.readFile(ths._update_fname,
                         { flag: constants.O_RDONLY | ths._flags },
        function (err, update)
        {
            ths._error(err);
            if (ths._chkstop()) { return; }

            if (err || (update.length < ths._update_size))
            {
                ths._timeout = setTimeout(ths._poll, ths._retry_interval);
                return;
            }

            cb(update);
        });
    }

    function check_update(update, bucket, extra_matcher, cb)
    {
        if (ths._chkstop()) { return; }

        var bucket_fmt = ths._format_bucket(bucket),
            start = bucket * ths._bucket_stamp_size,
            end = (bucket + 1) * ths._bucket_stamp_size,
            disruptor = ths._disruptors[bucket];       

        function save_update(err)
        {
            if (update && !err)
            {
                update.copy(ths._last_update, start, start, end);
            }

            cb();
        }

        function refresh(expired)
        {
            //console.log('update', require('os').hostname(), bucket);
            ths._process_all(
                    bucket,
                    bucket_fmt,
                    extra_matcher,
                    !disruptor,
                    disruptor && !extra_matcher,
                    function (err)
                    {
                        if (refresh_ttl > 0)
                        {
                            var now = Date.now();
                            // Use negative to stop a constantly timed out
                            // bucket never processing pending messages
                            ths._last_refreshed.set(bucket, expired ? -now : now);
                        }

                        if (disruptor)
                        {
                            return cb();
                        }

                        save_update(err);
                    });
        }

        if (refresh_ttl > 0)
        {
            var last_refresh = ths._last_refreshed.get(bucket);

            if (last_refresh < 0)
            {
                // We timed out last time, refresh next time
                ths._last_refreshed.set(bucket, -last_refresh);
            }
            else if ((Date.now() - last_refresh) > refresh_ttl)
            {
                return refresh(true);
            }
        }

        //console.log('upsum', require('os').hostname(), sum(update, start, end), bucket);

        if (update &&
            update.slice(start, end).equals(ths._last_update.slice(start, end)) &&
            !(extra_matcher && disruptor))
        {
            //console.log('no update', require('os').hostname(), bucket);
            return process_pending(bucket,
                                   bucket_fmt,
                                   extra_matcher,
                                   !disruptor,
                                   cb);
        }

        if (disruptor && !extra_matcher)
        {
            var element_size = disruptor.elementSize;
            if (element_size >= 6)
            {
                var bucket_pending = pending.get(bucket),
                    bufs = disruptor.consumeNewSync();

                return async.eachSeries(bufs, function (buf, next)
                {
                    var n = buf.length / element_size;

                    async.timesSeries(n, function (i, next)
                    {
                        var start = i * element_size,
                            end = start + element_size,
                            len = buf.readUInt16LE(end - 2);

                        if (len === 0)
                        {
                            return process.nextTick(next);
                        }

                        var fname_end = start + len,
                            fname = buf.toString('utf8', start, fname_end);

                        parse_fname(bucket_fmt, fname, function (info)
                        {
                            if (info)
                            {
                                info.size = buf.readUInt32LE(end - 6);

                                if ((info.size <= element_size - 6 - len) &&
                                    !info.single)
                                {
                                    info.data = Buffer.alloc(info.size);
                                    buf.copy(info.data,
                                             0,
                                             fname_end,
                                             fname_end + info.size);
                                }

                                info.new = true;

                                bucket_pending.push(info);
                            }

                            process.nextTick(next);
                        });
                    }, next);
                }, function ()
                {
                    if (ths._chkstop()) { return; }
                    disruptor.consumeCommit();
                    process_pending(bucket,
                                    bucket_fmt,
                                    extra_matcher,
                                    false,
                                    save_update);
                });
            }
        }

        refresh(false);
    }

    function process_messages(update)
    {
        var extra_matcher = ths._extra_matcher;
        ths._extra_matcher = null;

        async.timesLimit(ths.num_buckets, ths._bucket_concurrency,
        function (bucket, next)
        {
            check_update(update, bucket, extra_matcher, next);
        }, polled);
    }

    this._poll = function ()
    {
        ths._timeout = null;
        if (ths._chkstop()) { return; }

        ths._delay = ths._poll_interval;

        read_update(process_messages);
    };

    function format_bucket(b)
    {
        var bs = ths._bucket_base > 1 ? b.toString(ths._bucket_base) : '0',
            arr = [];
        arr.length = ths._bucket_num_chars + 1;
        return (arr.join('0') + bs).slice(-ths._bucket_num_chars);
    };

    this._format_bucket = function (b)
    {
        return bucket_formats[b];
    };

    this._update_size = this.num_buckets * this._bucket_stamp_size;
    this._last_update = crypto.randomBytes(this._update_size);

    this._require_fs = require;
    this._require_getdents = require;

    function create_watcher()
    {
        ths._timeout = setTimeout(ths._poll, 0);

        if ((ths._update_size > 0) && (options.notify !== false))
        {
            try
            {
                ths._watcher = ths._fs.watch(ths._update_dir, function (event)
                {
                    if (event === 'change')
                    {
                        ths.refresh_now();
                    }
                });

                ths._watcher.on('error', ths._error);
            }
            catch (err)
            {
                ths._stop_timeout();
                emit_error(err);
            }
        }
    }

    function start_notifiers(err)
    {
        //console.log('wrote update file', require('os').hostname());

        if (err) { return emit_error(err); }
        if (ths._chkstop()) { return; }

        create_watcher();
    }

    for (var i = 0; i < this.num_buckets; i += 1)
    {
        pending.set(i, []);
        ths._last_refreshed.set(i, 0);
        bucket_formats[i] = format_bucket(i);
    }

    process.nextTick(function ()
    {
        if (options.get_disruptor)
        {
            for (var i = 0; i < ths.num_buckets; i += 1)
            {
                ths._disruptors[i] = options.get_disruptor.call(ths, i);
            }
        }

        if (ths._do_single)
        {
            try
            {
                ths._fsext = ths._require_fs('fs-ext');
            }
            catch (err)
            {
                ths._do_single = false;

                if (ths.emit('single_disabled', err))
                {
                    if (ths._chkstop()) { return; }
                }
                else
                {
                    console.warn("single messages won't be processed", err);
                }
            }
        }

        ths._fs = Object.assign({}, ths._require_fs('fs'));

        // prevent graceful-fs readdir file sort overhead
        var orig_readdir = ths._fs.readdir;
        ths._fs.readdir = function (path, options, cb)
        {
            var args = [path];
            if (typeof options !== 'function')
            {
                args.push(options);
            }
            else
            {
                cb = options;
            }
            args.push(function (err, files)
            {
                if (files)
                {
                    files.sort = null;
                }

                if (typeof cb === 'function')
                {
                    cb.apply(this, arguments);
                }
            });
            return orig_readdir.apply(this, args);
        };

        require('graceful-fs').gracefulify(ths._fs);

        ths._process_all = process_all_readdir;
        if (options.getdents_size > 0)
        {
            var err = null;
            if (ths._order_by_expiry)
            {
                err = new Error("getdents can't be used with order-by-expiry");
            }
            else
            {
                try
                {
                    ths._process_all =
                        ths._require_getdents('./process_all_getdents').call(ths,
                              options.getdents_size,
                              ths.num_buckets,
                              delivered,
                              pending,
                              make_fname_handler);
                }
                catch (ex)
                {
                    err = ex;
                }
            }
            if (err)
            {
                if (ths.emit('getdents_disabled', err))
                {
                    if (ths._chkstop()) { return; }
                }
                else
                {
                    console.warn("getdents won't be used", err);
                }
            }
        }

        for (var i = 0; i < ths.num_buckets; i += 1)
        {
            var b = ths._format_bucket(i);
            dirs.push(ths._msg_dir + path.sep + b);
            dirs.push(ths._topic_dir + path.sep + b);
        }

        async.eachSeries(dirs, function (dir, next)
        {
            ths._fs.mkdir(dir, function (err)
            {
                next(err && (err.code !== 'EEXIST') ? err : null);
            });
        }, function (err)
        {
            if (err) { return emit_error(err); }

            if (ths._update_size === 0)
            {
                return start_notifiers();
            }

            ths._fs.writeFile(ths._update_fname,
                              crypto.randomBytes(ths._update_size),
                              { flag: constants.O_CREAT |
                                      constants.O_WRONLY |
                                      ths._flags },
                              start_notifiers);
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
  - `{Integer} size` Message size in bytes.

- `{Function} done` Function to call once you've handled the message. Note that calling this function is only mandatory if `info.single === true`, in order to delete and unlock the file. `done` takes two arguments:

  - `{Object} err` If an error occurred then pass details of the error, otherwise pass `null` or `undefined`.
  - `{Function} [finish]` Optional function to call once the message has been deleted and unlocked, in the case of `info.single === true`, or straight away otherwise. It will be passed the following argument:
    - `{Object} err` If an error occurred then details of the error, otherwise `null`.

@param {Object} [options] Optional settings for this subscription:
- `{Boolean} subscribe_to_existing` If `true` then `handler` will be called with any existing, unexpired messages that match `topic`, as well as new ones. Defaults to `false` (only new messages).

@param {Function} [cb] Optional function to call once the subscription has been registered. This will be passed the following argument:
- `{Object} err` If an error occurred then details of the error, otherwise `null`.
*/
QlobberFSQ.prototype.subscribe = function (topic, handler, options, cb)
{
    if (typeof options === 'function')
    {
        cb = options;
        options = undefined;
    }

    options = options || {};

    this._matcher.add(topic, handler);

    if (options.subscribe_to_existing)
    {
        this._ensure_extra_matcher().add(topic, handler);
    }

    if (cb) { cb.call(this); }
};

/**
Unsubscribe from messages in the file system queue.

@param {String} [topic] Which messages you're no longer interested in receiving via the `handler` function. This should be a topic you've previously passed to [`subscribe`](#qlobberfsqprototypesubscribetopic-handler-options-cb). If topic is `undefined` then all handlers for all topics are unsubscribed.

@param {Function} [handler] The function you no longer want to be called with messages published to the topic `topic`. This should be a function you've previously passed to [`subscribe`](#qlobberfsqprototypesubscribetopic-handler-options-cb). If you subscribed `handler` to a different topic then it will still be called for messages which match that topic. If `handler` is `undefined`, all handlers for the topic `topic` are unsubscribed.

@param {Function} [cb] Optional function to call once `handler` has been unsubscribed from `topic`. This will be passed the following argument:
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
        if (this._extra_matcher)
        {
            this._extra_matcher.clear();
        }
    }
    else if (handler === undefined)
    {
        this._matcher.remove(topic);
        if (this._extra_matcher)
        {
            this._extra_matcher.remove(topic);
        }
    }
    else
    {
        this._matcher.remove(topic, handler);
        if (this._extra_matcher)
        {
            this._extra_matcher.remove(topic, handler);
        }
    }

    this._matcher_marker = {};

    if (cb) { cb.call(this); }
};

function default_hasher(fname)
{
    var h = crypto.createHash('md5'); // not for security, just mapping!
    h.update(fname);
    return h.digest();
}

/**
Publish a message to the file system queue.

@param {String} topic Message topic. The topic should be a series of words separated by `.` (or the `separator` character you provided to the [`QlobberFSQ constructor`](#qlobberfsqoptions)). Topic words can contain any character, unless you set `encode_topics` to `false` in the [`QlobberFSQ constructor`](#qlobberfsqoptions). In that case they can contain any valid filename character for your file system, although it's probably sensible to limit it to alphanumeric characters, `-`, `_` and `.`.

@param {String|Buffer} [payload] Message payload. If you don't pass a payload then `publish` will return a [Writable stream](http://nodejs.org/api/stream.html#stream_class_stream_writable) for you to write the payload into.

@param {Object} [options] Optional settings for this publication:
- `{Boolean} single` If `true` then the message will be given to _at most_ one interested subscriber, across all `QlobberFSQ` objects scanning the file system queue. Otherwise all interested subscribers will receive the message (the default).

- `{Integer} ttl` Time-to-live (in milliseconds) for this message. If you don't specify anything then `single_ttl` or `multi_ttl` (provided to the [`QlobberFSQ constructor`](#qlobberfsqoptions)) will be used, depending on the value of `single`. After the time-to-live for the message has passed, the message is ignored and deleted when convenient.

- `{String} encoding` If `payload` is a string, the encoding to use when writing it out to the message file. Defaults to `utf8`.

- `{Integer} mode` The file mode (permissions) to set on the message file. Defaults to octal `0666` (readable and writable to everyone).

- `{Function} hasher` A hash function to use for deciding into which bucket the message should be placed. The hash function should return a `Buffer` at least 4 bytes long. It defaults to running `md5` on the message file name. If you supply a `hasher` function it will be passed the following arguments:

  - `{String} fname` Message file name.
  - `{Integer} expires` When the message expires (number of milliseconds after 1 January 1970 00:00:00 UTC).
  - `{String} topic` Message topic.
  - `{String|Buffer} payload` Message payload.
  - `{Object} options` The optional settings for this publication.

- `{Integer} bucket` Which bucket to write the message into, instead of using `hasher` to calculate it.

- `{Boolean} ephemeral` This applies only if a shared memory LMAX Disruptor is being used for the message's bucket (see the `get_disruptor` option to the [`QlobberFSQ constructor`](#qlobberfsqoptions)). By default, the message is written both to the Disruptor and the filesystem. If `ephemeral` is truthy, the message is written only to the Disruptor.
  
  - If the Disruptor's elements aren't large enough to contain the message's metadata, the message won't be written  to the Disruptor and `cb` (below) will receive an error with a property `code` equal to the string `buffer-too-small`.
    
  - However, if the Disruptor's elements aren't large enough for the message's payload, the message _will_ be written to disk. The amount of space available in the Disruptor for the payload can be found via the `ephemeral_size` property on the stream returned by this function. If your message won't fit and you don't want to write it to disk, emit an `error` event on the stream without ending it.
    
@param {Function} [cb] Optional function to call once the message has been written to the file system queue. This will be called after the message has been moved into its bucket and is therefore available to subscribers in any `QlobberFSQ` object scanning the queue. It will be passed the following arguments:
- `{Object} err` If an error occurred then details of the error, otherwise `null`.

- `{Object} info` Metadata for the message. See [`subscribe`](#qlobberfsqprototypesubscribetopic-handler-options-cb) for a description of `info`'s properties.

@return {Stream|undefined} A [Writable stream](http://nodejs.org/api/stream.html#stream_class_stream_writable) if no `payload` was passed, otherwise `undefined`.
*/
QlobberFSQ.prototype.publish = function (topic, payload, options, cb)
{
    if ((typeof payload !== 'string') &&
        !Buffer.isBuffer(payload) &&
        (payload !== undefined))
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

    var unencoded_topic = topic;

    if (this._encode_topics)
    {
        topic = Buffer.from(topic).toString('hex');
    }

    var ths = this,
        write_options = { flags: constants.O_TRUNC |
                                 constants.O_CREAT |
                                 constants.O_WRONLY |
                                 this._flags },
        now = Date.now(),
        expires = now + (options.ttl || (options.single ? this._single_ttl :
                                                          this._multi_ttl)),
        split = topic.substr(this._split_topic_at),
        unique_bytes = crypto.randomBytes(this._unique_bytes).toString('hex'),
        fname = topic.substr(0, this._split_topic_at) + '@' +
                (split ? 'l' : 's') + '+' +
                expires.toString(16) + '+' +
                (options.single ? 's' : 'm') + '+' +
                unique_bytes,
        staging_fname = this._staging_dir + path.sep + fname,
        bucket,
        bucket_fmt,
        msg_fname,
        topic_fname,
        out_stream,
        hasher = options.hasher || default_hasher,
        disruptor,
        element_size,
        element,
        count = 0;

    write_options.flag = write_options.flags;
    write_options.encoding = options.encoding || 'utf8';
    write_options.mode = options.mode || 438; // 0666

    bucket = (options.bucket === undefined ?
            hasher(fname, expires, topic, payload, options).readUInt32BE(0) :
            options.bucket) % this.num_buckets;
    bucket_fmt = this._format_bucket(bucket);
    msg_fname = this._msg_dir + path.sep + bucket_fmt + path.sep + fname;
    topic_fname = this._topic_dir + path.sep + bucket_fmt + path.sep + fname;

    function errored(err)
    {
        if (err)
        {
            ths._error(err);

            function unlink_topic(err2)
            {
                ths._error(err2, 'ENOENT');

                function callback(err3)
                {
                    ths._error(err3, 'ENOENT');
                    if (cb) { cb.call(ths, err); }
                }

                try
                {
                    ths._fs.unlink(topic_fname, callback);
                }
                catch (err3)
                {
                    process.nextTick(callback, err3);
                }
            }

            try
            {
                ths._fs.unlink(staging_fname, unlink_topic);
            }
            catch (err2)
            {
                process.nextTick(unlink_topic, err2);
            }

            return true;
        }

        return false;
    }

    function onerror(err)
    {
        /*jshint validthis: true */
        this.end();
        errored(err);
    }

    function derror(err)
    {
        this.removeListener('finish', claim);

        this.once('finish', function ()
        {
            errored(err);
        });

        this.end();
    }

    function register(s, onclose)
    {
        s.once('close', onclose);

        s.once('error', function ()
        {
            this.removeListener('close', onclose);
        });

        s.once('error', onerror);

        s.once('open', function ()
        {
            this.removeListener('error', onerror);

            this.once('error', function (err)
            {
                this.once('close', function ()
                {
                    errored(err);
                });

                this.end();
            });
        });
    }

    function done()
    {
        if (!cb) { return; }

        var info = {
            fname: fname,
            path: msg_fname,
            topic: unencoded_topic,
            expires: expires,
            single: !!options.single,
            size: count
        };

        if (split)
        {
            info.topic_path = topic_fname;
        }

        cb.call(ths, null, info);
    }

    function update()
    {
        if (ths._update_size === 0)
        {
            return done();
        }

        var update_stream = ths._fs.createWriteStream(ths._update_fname,
        {
            flags: constants.O_CREAT |
                   constants.O_WRONLY |
                   ths._flags,
            start: bucket * ths._bucket_stamp_size
        });

        register(update_stream, done);

        update_stream.once('open', function ()
        {
            this.end(crypto.randomBytes(ths._bucket_stamp_size));
        });
    }

    function commit(start, end)
    {
        if (ths.stopped) { return errored(new Error('stopped')); }

        if (!disruptor.produceCommitSync(start, end))
        {
            return setTimeout(commit, ths._disruptor_spin_interval, start, end);
        }

        update();
    }

    function claim()
    {
        if (ths.stopped) { return errored(new Error('stopped')); }

        var b = disruptor.produceClaimSync();

        if (b.length == 0)
        {
            return setTimeout(claim, ths._disruptor_spin_interval);
        }

        element.copy(b);
        b.writeUInt32LE(count, element_size);

        commit(disruptor.prevClaimStart, disruptor.prevClaimEnd);
    }

    function rename()
    {
        ths._fs.rename(staging_fname, msg_fname, function (err)
        {
            if (ths._try_again(err))
            {
                ths._error(err);
                return setTimeout(rename, ths._retry_interval);
            }

            if (errored(err)) { return; }

            if (!disruptor)
            {
                return update();
            }

            claim();
        });
    }

    function maybe_split()
    {
        count = Math.max(out_stream.bytesWritten - 1, 0);

        if (!split)
        {
            return rename();
        }

        write_options.encoding = 'utf8';

        ths._fs.writeFile(topic_fname, split, write_options, function (err)
        {
            if (errored(err)) { return; }
            rename();
        });
    }

    disruptor = ths._disruptors[bucket];

    if (disruptor)
    {
        element_size = disruptor.elementSize;
        element = Buffer.alloc(element_size);
        element_size -= 6; // space for data length and filename length

        var ephemeral = options.ephemeral && !options.single,
            dfname = ephemeral ?
                (topic + '@s+' +
                 expires.toString(16) + '+' +
                 (options.single ? 's' : 'm') + '+' +
                 unique_bytes) :
                fname;

        var start = element.write(dfname);

        if (start > element_size)
        {
            var err = new Error('buffer too small');
            err.code = 'buffer-too-small';
            return errored(err);
        }
        else
        {
            element.writeUInt16LE(start, element_size + 4);

            var space = element_size - start,
                pos = start,
                opened = false;

            out_stream = new class extends stream.PassThrough {
                _write(chunk, encoding, cb) {
                    const len = chunk.length;
                    count += len;

                    if (count <= space) {
                        chunk.copy(element, pos);
                        pos += len;

                        if (ephemeral) {
                            return cb();
                        }
                    }

                    if (!opened) {
                        opened = true;

                        let fs_stream;
                        try {
                            fs_stream = ths._fs.createWriteStream(staging_fname, write_options);
                        } catch (ex) {
                            out_stream.emit('error', ex);
                            return cb();
                        }

                        out_stream.removeListener('finish', claim);
                        out_stream.removeListener('error', derror);

                        register(fs_stream, maybe_split);
                        out_stream.pipe(fs_stream);
                        out_stream.on('error', function (err) {
                            fs_stream.emit('error', err);
                            this.end();
                        });
                        out_stream = fs_stream;

                        if (ephemeral) {
                            chunk = Buffer.concat([
                                ths._leading_byte,
                                element.slice(start, pos),
                                chunk
                            ]);
                        } else {
                            chunk = Buffer.concat([
                                ths._leading_byte,
                                chunk
                            ]);
                        }
                    }

                    return super._write(chunk, encoding, cb);
                }
            }();

            out_stream.ephemeral_size = space;

            out_stream.once('finish', claim);
            out_stream.once('error', derror);
        }
    }

    if (!out_stream)
    {
        try
        {
            out_stream = this._fs.createWriteStream(staging_fname, write_options);
        }
        catch (ex)
        {
            ths._error(ex);
            if (cb) { cb.call(ths, ex); }
            return;
        }
        register(out_stream, maybe_split);
        out_stream.write(ths._leading_byte);
    }

    if ((typeof payload !== 'string') && !Buffer.isBuffer(payload))
    {
        return out_stream;
    }

    out_stream.end(payload);
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

QlobberFSQ.prototype.___stop_watching = function (err, cb)
{
    if (cb)
    {
        return cb.call(this, err);
    }

    if (err)
    {
        this.emit('error', err);
    }
};

QlobberFSQ.prototype.__stop_watching = function (err, cb)
{
    this._error(err);

    if (this._stop_timeout())
    {
        setImmediate(this._chkstop);
    }

    if (this.active)
    {
        return this.once('stop', function ()
        {
            this.___stop_watching(err, cb);
        });
    }

    this.___stop_watching(err, cb);
};

QlobberFSQ.prototype._stop_watching = function (cb)
{
    for (var i = 0; i < this.num_buckets; i += 1)
    {
        var disruptor = this._disruptors[i];
        if (disruptor)
        {
            this._disruptors[i] = null;
            disruptor.release();
        }
    }

    if (this._process_all && this._process_all.stop)
    {
        var ths = this;
        return this._process_all.stop(function (err)
        {
            ths.__stop_watching(err, cb);
        });
    }

    this.__stop_watching(null, cb);
};

/**
Stop scanning for new messages.

@param {Function} [cb] Optional function to call once scanning has stopped. Alternatively, you can listen for the [`stop` event](#qlobberfsqeventsstop).
*/
QlobberFSQ.prototype.stop_watching = function (cb)
{
    if (this.stopped)
    {
        if (cb)
        {
            cb.call(this);
        }
        return;
    }
    this.stopped = true;

    var watcher = this._watcher;
    if (watcher)
    {
        this._watcher = null;
        if (semver.gte(process.version, '10.0.0'))
        {
            var ths = this;
            watcher.on('close', function ()
            {
                ths._stop_watching(cb);
            });
            return watcher.close();
        }
        watcher.close();
    }
    this._stop_watching(cb);
};

/**
Check the `UPDATE` file now rather than waiting for the next periodic check to occur
*/
QlobberFSQ.prototype.refresh_now = function ()
{
    if (this.stopped) { return; }

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

   for (var i = 0; i < this.num_buckets; i += 1)
   {
       this._last_refreshed.set(i, 0);
   }

   this.refresh_now();
};

/**
Given a radix to use for characters in bucket names and the number of digits in
each name, return the number of buckets that can be represented.

@param {Integer} bucket_base Radix for bucket name characters.

@param {Integer} bucket_num_chars Number of characters in bucket names.

@return {Integer} The number of buckets that can be represented.
*/
QlobberFSQ.get_num_buckets = function (base, num_chars)
{
    return Math.pow(base || 16, num_chars || 2);
};

exports.QlobberFSQ = QlobberFSQ;

