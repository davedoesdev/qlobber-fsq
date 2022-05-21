/*eslint brace-style: "error"*/
const { EventEmitter } = require('events');
const { queue } = require('async');

exports.MPFSQBase = class extends EventEmitter {
    constructor(child) {
        super();

        this.handlers = {};
        this.handler_count = 0;
        this.pub_cbs = {};
        this.pub_cb_count = 0;
        this.sub_cbs = {};
        this.sub_cb_count = 0;
        this.unsub_cbs = {};
        this.unsub_cb_count = 0;
        this.topics = {};
        // https://github.com/nodejs/node/issues/7657
        // https://github.com/libuv/libuv/issues/1099
        this.send_queue = queue(function (msg, cb) {
            child.send(msg, cb);
        });

        child.on('error', err => {
            this.emit('error', err);
        });

        child.on('exit', (unused_code, unused_signal) => {
            this.emit('stop');
            //console.log('exit', ths._host, code, signal);
        });

        child.on('message', msg => {
            //console.log("parent got message", msg);

            if (msg.type === 'start') {
                //console.log('got start', ths._host);
                this.emit('start');
            } else if (msg.type === 'stop') {
                this.send_queue.push({ type: 'exit' });
            } else if (msg.type === 'received') {
                //console.log('recv', msg.host, msg.pid, msg.info.topic, msg.info.single, msg.handler);
                this.handlers[msg.handler].call(this,msg.sum, msg.info, err => {
                    this.send_queue.push({
                        type: 'recv_callback',
                        cb: msg.cb,
                        err: err
                    });
                });
            } else if (msg.type === 'sub_callback') {
                const cb = this.sub_cbs[msg.cb];
                delete this.sub_cbs[msg.cb];
                //console.log('sub_callback', ths._host, msg.cb, Object.keys(sub_cbs));
                cb.call(this);
            } else if (msg.type === 'unsub_callback') {
                const cb = this.unsub_cbs[msg.cb];
                delete this.unsub_cbs[msg.cb];
                //console.log('unsub_callback', ths._host, msg.cb, Object.keys(unsub_cbs));
                cb.call(this);
            } else if (msg.type === 'pub_callback') {
                const cb = this.pub_cbs[msg.cb];
                delete this.pub_cbs[msg.cb];
                //console.log('pub_callback', ths._host, msg.cb, Object.keys(pub_cbs));
                cb.call(this, msg.err, msg.fname);
            }
        });
    }

    subscribe(topic, handler, cb) {
        this.handlers[this.handler_count] = handler;
        handler.__count = this.handler_count;

        this.sub_cbs[this.sub_cb_count] = cb;

        this.topics[topic] = this.topics[topic] || {};
        this.topics[topic][this.handler_count] = true;

        this.send_queue.push({
            type: 'subscribe',
            topic: topic,
            handler: this.handler_count,
            cb: this.sub_cb_count
        });

        //console.log('subscribe', ths._host, topic, handler_count, sub_cb_count);

        this.handler_count += 1;
        this.sub_cb_count += 1;
    }

    unsubscribe(topic, handler, cb) {
        if (topic === undefined) {
            this.unsub_cbs[this.unsub_cb_count] = () => {
                this.handlers = {};
                this.topics = {};
                cb();
            };

            this.send_queue.push( {
                type: 'unsubscribe',
                cb: this.unsub_cb_count
            });

            this.unsub_cb_count += 1;
        } else if (handler === undefined) {
            let n = this.topics[topic].length;

            this.topics[topic].forEach(h => {
                this.unsub_cbs[this.unsub_cb_count] = () => {
                    delete this.handlers[h];
                    n -= 1;
                    if (n === 0) {
                        delete this.topics[topic];
                        cb();
                    }
                };

                this.send_queue.push( {
                    type: 'unsubscribe',
                    topic: topic,
                    handler: h,
                    cb: this.unsub_cb_count
                });

                this.unsub_cb_count += 1;
            });
        } else {
            this.unsub_cbs[this.unsub_cb_count] = () => {
                delete this.handlers[handler.__count];
                cb();
            };

            this.send_queue.push({
                type: 'unsubscribe',
                topic: topic,
                handler: handler.__count,
                cb: this.unsub_cb_count
            });

            this.unsub_cb_count += 1;
        }
    }

    publish(topic, payload, options, cb) {
        this.pub_cbs[this.pub_cb_count] = cb;

        this.send_queue.push({
            type: 'publish',
            topic: topic,
            payload: payload.toString('base64'),
            options: options,
            cb: this.pub_cb_count
        });

        //console.log('publish', ths._host, topic, pub_cb_count, Object.keys(pub_cbs));
        this.pub_cb_count += 1;
    }

    stop_watching(cb) {
        this.send_queue.push({ type: 'stop_watching' });

        if (cb) {
            this.once('stop', cb);
        }
    }
};
