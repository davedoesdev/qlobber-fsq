"use strict";

var options = JSON.parse(Buffer.from(process.argv[2], 'hex')),
    QlobberFSQ = require('../..').QlobberFSQ,
    cbs = {},
    cb_count = 0,
    handlers = {},
    host = require('os').hostname(),
    async = require('async');

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

            cbs[cb_count] = cb;
            send(
            {
                type: 'received',
                handler: msg.handler,
                sum: sum(data),
                info: info,
                cb: cb_count,
                host: host,
                pid: process.pid
            });
            cb_count += 1;
        };

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
