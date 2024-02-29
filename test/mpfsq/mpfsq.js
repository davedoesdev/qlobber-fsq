"use strict";

(async function ()
{

var options = JSON.parse(Buffer.from(process.argv[2], 'hex')),
    QlobberFSQ = require('../..').QlobberFSQ,
    cbs = {},
    cb_count = 0,
    handlers = {},
    host = require('os').hostname(),
    async = require('async'),
    crypto = require('crypto'),
    { expect } = await import('chai');

if (options.handler_concurrency === null)
{
    options.handler_concurrency = Infinity;
}

//console.log(options);

if (options.disruptor)
{
    var Disruptor = require('shared-memory-disruptor').Disruptor;

    options.get_disruptor = function (bucket)
    {
        if (bucket < Math.min(options.total, options.num_buckets))
        {
            return new Disruptor('/test' + bucket,
                                 options.num_elements,
                                 options.element_size,
                                 options.total,
                                 options.index,
                                 false,
                                 false);
        }

        return null;
    };

    options.bucket_stamp_size = 0;
}

if (options.direct)
{
    const { Disruptor, DisruptorReadStream } = require('shared-memory-disruptor');

    options.direct_handler = new class
    {
        constructor()
        {
            this.streams = new Map();
        }

        get_stream_for_publish(unused_filename, unused_direct)
        {
            // we never publish direct in child
            throw new Error('should not be called');
        }

        make_disruptor(filename)
        {
            return new Disruptor(`/${crypto.createHash('sha256').update(filename).digest('hex')}`,
                                 options.num_elements,
                                 1,
                                 options.total,
                                 options.index,
                                 false,
                                 false);
        }

        get_stream_for_subscribers(filename)
        {
            const rs = new DisruptorReadStream(this.make_disruptor(filename));
            this.streams.set(filename, rs);
            return rs;
        }

        publish_stream_destroyed(unused_filename, unused_stream)
        {
            throw new Error('should not be called');
        }

        publish_stream_expired(filename)
        {
            expect(this.streams.has(filename)).to.be.false;
        }

        subscriber_stream_destroyed(filename, stream)
        {
            this.streams.delete(filename);
            stream.disruptor.release();
        }

        subscriber_stream_ignored(filename)
        {
            expect(this.streams.has(filename)).to.be.false;
            this.make_disruptor(filename).release(true);
        }
    }();
}

var fsq = new QlobberFSQ(options);

//console.log('run', host, process.pid);

function sum(buf)
{
    var i, r = 0;

    for (i = 0; i < buf.length; i += 1)
    {
        r += buf[i];
    }

    return r;
}

// https://github.com/nodejs/node/issues/7657
// https://github.com/libuv/libuv/issues/1099

var send_queue = async.queue(function (msg, cb)
{
    process.send(msg, cb);
});

function send(msg)
{
    send_queue.push(msg);
}

fsq.on('start', function ()
{
    send({ type: 'start' });
    //console.log('start', host, process.pid);
});

fsq.on('stop', function ()
{
    //console.log('stop', host, process.pid, handlers, cbs);
    send({ type: 'stop' });
});

process.on('message', function (msg)
{
    //console.log("child got message", msg);

    if (msg.type === 'subscribe')
    {
        handlers[msg.handler] = function (data, info, cb)
        {
            //console.log('got', host, process.pid, msg.topic, info.topic, info.single, info.path, msg.handler);

            if (options.hash)
            {
                const hash = crypto.createHash('sha256');

                data.on('readable', function ()
                {
                    let chunk;
                    while (chunk = this.read()) // eslint-disable-line no-cond-assign
                    {
                        hash.update(chunk);
                    }
                });

                data.on('end', function ()
                {
                    const cbc = cb_count++;
                    cbs[cbc] = cb;
                    send(
                    {
                        type: 'received',
                        handler: msg.handler,
                        sum: hash.digest('hex'),
                        info: info,
                        cb: cbc,
                        host: host,
                        pid: process.pid
                    });
                });
            }
            else
            {
                const cbc = cb_count++;
                cbs[cbc] = cb;
                send(
                {
                    type: 'received',
                    handler: msg.handler,
                    sum: sum(data),
                    info: info,
                    cb: cbc,
                    host: host,
                    pid: process.pid
                });
            }
        };
        
        if (options.hash)
        {
            handlers[msg.handler].accept_stream = true;
        }

        fsq.subscribe(msg.topic, handlers[msg.handler], function ()
        {
            send(
            {
                type: 'sub_callback',
                cb: msg.cb
            });
        });

        //console.log(host, process.pid, 'sub', msg.topic);
    }
    else if (msg.type === 'recv_callback')
    {
        cbs[msg.cb](msg.err);
        delete cbs[msg.cb];
    }
    else if (msg.type === 'publish')
    {
        //console.log('publishing', host, process.pid, msg.topic, msg.options);

        if (options.disruptor)
        {
            msg.options.bucket = options.index % options.num_buckets;
            msg.options.ephemeral = true;
        }

        fsq.publish(msg.topic, Buffer.from(msg.payload, 'base64'), msg.options,
        function (err, fname)
        {
            send(
            {
                type: 'pub_callback',
                cb: msg.cb,
                err: err,
                fname: fname
            });
        });
    }
    else if (msg.type === 'stop_watching')
    {
        fsq.stop_watching();
    }
    else if (msg.type === 'exit')
    {
        process.exit();
    }
    else if (msg.type === 'unsubscribe')
    {
        if (msg.topic)
        {
            fsq.unsubscribe(msg.topic, handlers[msg.handler], function ()
            {
                delete handlers[msg.handler];
                send(
                {
                    type: 'unsub_callback',
                    cb: msg.cb
                });
            });
        }
        else
        {
            fsq.unsubscribe(function ()
            {
                handlers = {};
                send(
                {
                    type: 'unsub_callback',
                    cb: msg.cb
                });
            });
        }
    }
});

})();
