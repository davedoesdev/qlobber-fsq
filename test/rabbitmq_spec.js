/*globals fsq: false,
          async: false,
          flags: false,
          expect: false,
          fsq_dir: false,
          sum: false,
          crypto: false,
          child_process: false,
          cp_remote: false,
          path: false,
          argv: false,
          QlobberFSQ: false,
          os: false,
          rabbitmq_test_bindings: false,
          rabbitmq_expected_results_before_remove: false,
          rabbitmq_bindings_to_remove: false,
          rabbitmq_expected_results_after_remove: false,
          rabbitmq_expected_results_after_remove_all: false,
          rabbitmq_expected_results_after_clear: false */
"use strict";

const { MPFSQBase } = require('./mpfsq_base.js');

function rabbitmq_tests(name, QCons, num_queues, rounds, msglen, retry_prob, expected, f)
{
    it('should pass rabbitmq tests (' + name + ', num_queues=' + num_queues + ', rounds=' + rounds + ', msglen=' + msglen + ', retry_prob=' + retry_prob + ')', function (done)
    {
        var timeout = 20 * 60 * 1000;
        this.timeout(timeout);

        fsq.stop_watching(function ()
        {
            async.times(num_queues, function (n, cb)
            {
                var fsq = new QCons(
                {
                    fsq_dir: fsq_dir,
                    flags: flags
                }, num_queues, n);

                fsq.on('start', function ()
                {
                    cb(null, fsq);
                });
            }, function (err, fsqs)
            {
                if (err) { return done(err); }

                var i,
                    j,
                    q,
                    total = 0,
                    count = 0,
                    subs = [],
                    result = {},
                    expected2 = {},
                    expected_sums = {},
                    sums = {},
                    count_single = 0,
                    expected_single_sum = 0,
                    single_sum = 0,
                    result_single = [],
                    expected_result_single = [],
                    assigned = {};

                for (i = 0; i < expected.length; i += 1)
                {
                    total += expected[i][1].length * rounds;
                }

                function topic_sort(a, b)
                {
                    return parseInt(a.substr(1), 10) - parseInt(b.substr(1), 10);
                }

                for (i = 0; i < expected.length; i += 1)
                {
                    expected2[expected[i][0]] = [];

                    for (j = 0; j < rounds; j += 1)
                    {
                        expected2[expected[i][0]] = expected2[expected[i][0]].concat(expected[i][1]);
                    }

                    expected2[expected[i][0]].sort(topic_sort);
                }

                for (i = 0; i < rounds; i += 1)
                {
                    expected_result_single = expected_result_single.concat(Object.keys(expected2));
                }

                expected_result_single.sort();

                function received(n, topic, value, data, single)
                {
                    expect(subs[n][value], value).to.equal(true);

                    if (single)
                    {
                        expect(expected2[topic]).to.contain(value);
                        single_sum += Buffer.isBuffer(data) ? sum(data) : data;
                        result_single.push(topic);
                        count_single += 1;

                        //console.log(topic, n);
                    }
                    else
                    {
                        result[topic] = result[topic] || [];
                        result[topic].push(value);

                        sums[topic] = sums[topic] || 0;
                        sums[topic] += Buffer.isBuffer(data) ? sum(data) : data;

                        count += 1;
                    }

                    //console.log('count:', count, total);
                    //console.log('count_single:', count_single, expected.length * rounds);

                    if ((count === total) && (count_single === expected.length * rounds))
                    {
                        // wait a bit to catch duplicates
                        setTimeout(function ()
                        {
                            var t;

                            result_single.sort();
                            expect(result_single).to.eql(expected_result_single);
                            expect(single_sum).to.equal(expected_single_sum);

                            for (t in result)
                            {
                                if (result.hasOwnProperty(t)) // eslint-disable-line no-prototype-builtins
                                {
                                    result[t].sort(topic_sort);
                                }
                            }

                            expect(result).to.eql(expected2);

                            expect(sums).to.eql(expected_sums);

                            async.each(fsqs, function (fsq, next)
                            {
                                fsq.stop_watching(next);
                            }, done);
                        }, 10 * 1000);
                    }
                    else
                    {
                        if (count_single > expected.length * rounds)
                        {
                            console.error(arguments, count_single, expected.length * rounds);
                            throw new Error('more single messages than expected');
                        }

                        if (count > total)
                        {
                            console.error(arguments, count, total);
                            throw new Error('more messages than expected total');
                        }
                    }
                }

                function subscribe(fsq, n, topic, value, cb)
                {
                    function handler(data, info, cb)
                    {
                        if (info.single && (Math.random() < retry_prob))
                        {
                            return cb('dummy retry');
                        }

                        received(n, info.topic, value, data, info.single);
                        cb();
                    }

                    fsq.subscribe(topic, handler, function ()
                    {
                        fsq.__submap = fsq.__submap || {};
                        fsq.__submap[value] = handler;
                        cb();
                    });
                }

                function unsubscribe(fsq, topic, value, cb)
                {
                    if (value)
                    {
                        fsq.unsubscribe(topic, fsq.__submap[value], function ()
                        {
                            delete fsq.__submap[value];
                            cb();
                        });
                    }
                    else
                    {
                        fsq.unsubscribe(topic, value, cb);
                    }
                }

                function publish()
                {
                    var pq = async.queue(function (task, cb)
                    {
                        var buf = crypto.randomBytes(msglen),
                            s = sum(buf),
                            pi;

                        expected_sums[task] = expected_sums[task] || 0;

                        for (pi = 0; pi < expected2[task].length / rounds; pi += 1)
                        {
                            expected_sums[task] += s;
                        }

                        expected_single_sum += s;

                        //console.log(task);

                        async.parallel(
                        [
                            function (cb)
                            {
                                fsqs[Math.floor(Math.random() * num_queues)].publish(
                                        task,
                                        buf,
                                        { ttl: timeout },
                                        cb);
                            },
                            function (cb)
                            {
                                fsqs[Math.floor(Math.random() * num_queues)].publish(
                                        task,
                                        buf,
                                        { ttl: timeout, single: true },
                                        cb);
                            }
                        ], cb);
                    }, num_queues * 5), pi, pj;

                    for (pi = 0; pi < rounds; pi += 1)
                    {
                        for (pj = 0; pj < expected.length; pj += 1)
                        {
                            pq.push(expected[pj][0]);
                        }
                    }

                    if (Object.keys(subs).length === 0)
                    {
                        pq.drain(function ()
                        {
                            //console.log('drained');

                            setTimeout(function ()
                            {
                                expect(count).to.equal(0);
                                expect(count_single).to.equal(0);

                                async.each(fsqs, function (fsq, next)
                                {
                                    fsq.stop_watching(next);
                                }, done);
                            }, 10 * 1000);
                        });
                    }
                }

                q = async.queue(function (i, cb)
                {
                    var n = Math.floor(Math.random() * num_queues),
                        entry = rabbitmq_test_bindings[i];

                    subs[n] = subs[n] || {};
                    subs[n][entry[1]] = true;
                    assigned[i] = n;
                    assigned[entry[0]] = assigned[entry[0]] || [];
                    assigned[entry[0]].push({ n: n, v: entry[1] });

                    subscribe(fsqs[n], n, entry[0], entry[1], cb);
                }, num_queues * 5);

                for (i = 0; i < rabbitmq_test_bindings.length; i += 1)
                {
                    q.push(i);
                }

                q.drain(function ()
                {
                    if (f)
                    {
                        f(fsqs, subs, assigned, unsubscribe, publish);
                    }
                    else
                    {
                        publish();
                    }
                });
            });
        });
    });
}

function rabbitmq(prefix, QCons, queues, rounds, msglen, retry_prob)
{
    prefix += ', ';

    rabbitmq_tests(prefix + 'before_remove', QCons, queues, rounds, msglen, retry_prob, rabbitmq_expected_results_before_remove);

    rabbitmq_tests(prefix + 'after_remove', QCons, queues, rounds, msglen, retry_prob, rabbitmq_expected_results_after_remove,
    function (fsqs, subs, assigned, unsubscribe, cb)
    {
        async.eachLimit(rabbitmq_bindings_to_remove, fsqs.length * 5,
        function (i, next)
        {
            var n = assigned[i - 1],
                v = rabbitmq_test_bindings[i - 1][1];
            
            unsubscribe(fsqs[n], rabbitmq_test_bindings[i - 1][0], v,
            function ()
            {
                assigned[i - 1] = null;
                subs[n][v] = null;
                next();
            });
        }, cb);
    });

    rabbitmq_tests(prefix + 'after_remove_all', QCons, queues, rounds, msglen, retry_prob, rabbitmq_expected_results_after_remove_all,
    function (fsqs, subs, assigned, unsubscribe, cb)
    {
        async.eachLimit(rabbitmq_bindings_to_remove, fsqs.length * 5,
        function (i, next)
        {
            var topic = rabbitmq_test_bindings[i - 1][0];

            async.eachLimit(assigned[topic], fsqs.length * 5, 
            function (nv, next2)
            {
                unsubscribe(fsqs[nv.n], topic, nv.v, function ()
                {
                    subs[nv.n][nv.v] = null;
                    next2();
                });
            }, function ()
            {
                assigned[i - 1] = null;
                assigned[topic] = [];
                next();
            });
        }, cb);
    });

    rabbitmq_tests(prefix + 'after_clear', QCons, queues, rounds, msglen, retry_prob, rabbitmq_expected_results_after_clear,
    function (fsqs, subs, assigned, unsubscribe, cb)
    {
        async.each(fsqs, function (fsq, next)
        {
            unsubscribe(fsq, undefined, undefined, next);
        }, function ()
        {
            subs.length = 0;
            cb();
        });
    });
}

function rabbitmq2(prefix, QCons, queues, rounds, msglen)
{
    rabbitmq(prefix, QCons, queues, rounds, msglen, 0);
    //rabbitmq(prefix, QCons, queues, rounds, msglen, 0.25);
    rabbitmq(prefix, QCons, queues, rounds, msglen, 0.5);
    //rabbitmq(prefix, QCons, queues, rounds, msglen, 0.75);
}

function rabbitmq3(prefix, QCons, queues, rounds)
{
    rabbitmq2(prefix, QCons, queues, rounds, 1);
    //rabbitmq2(prefix, QCons, queues, rounds, 1024);
    rabbitmq2(prefix, QCons, queues, rounds, 25 * 1024);
    //rabbitmq2(prefix, QCons, queues, rounds, 100 * 1024);
}

function rabbitmq4(prefix, QCons, queues)
{
    rabbitmq3(prefix, QCons, queues, 1);
    //rabbitmq3(prefix, QCons, queues, 10);
    rabbitmq3(prefix, QCons, queues, 50);
    //rabbitmq3(prefix, QCons, queues, 100);
    //rabbitmq3(prefix, QCons, queues, 500);
    //rabbitmq3(prefix, QCons, queues, 1000);
}

function make_MPFSQ(use_disruptor)
{
    return class extends MPFSQBase
    {
        constructor(options, total, index)
        {
            const num_buckets = QlobberFSQ.get_num_buckets(options.bucket_base,
                                                           options.bucket_num_chars);

            if (use_disruptor && (index === 0))
            {
                const { Disruptor } = require('shared-memory-disruptor');

                for (let i = 0; i < Math.min(total, num_buckets); i += 1)
                {
                    const d = new Disruptor('/test' + i,
                                            20 * 1024,
                                            20 * 1024,
                                            total,
                                            0,
                                            true,
                                            false);
                    d.release();
                }
            }

            options = Object.assign({}, options,
            {
                disruptor: use_disruptor,
                num_elements: 20 * 1024,
                element_size: 20 * 1024,
                total: total,
                index: index,
                num_buckets: num_buckets
            });

            super(child_process.fork(path.join(__dirname, 'mpfsq', 'mpfsq.js'),
                                     [Buffer.from(JSON.stringify(options)).toString('hex')]));
        }
    };
}

function make_RemoteMPFSQ(hosts)
{
    return class extends MPFSQBase
    {
        constructor(options, total, index)
        {
            super(cp_remote.run(hosts[index],
                                path.join(__dirname, 'mpfsq', 'mpfsq.js'),
                                Buffer.from(JSON.stringify(options)).toString('hex')));
            this._host = hosts[index];
        }
    };
}

describe('rabbit', function ()
{
    if (argv.remote)
    {
        var hosts = typeof argv.remote === 'string' ? [argv.remote] : argv.remote;
        rabbitmq4('distributed', make_RemoteMPFSQ(hosts), hosts.length);
    }
    else
    {
        /*rabbitmq4('', QlobberFSQ, 1);
        rabbitmq4('', QlobberFSQ, 10);
        rabbitmq4('', QlobberFSQ, 26);
        rabbitmq4('', QlobberFSQ, 100);*/

        rabbitmq4('multi-process, disruptor=' + argv.disruptor,
                  make_MPFSQ(argv.disruptor), argv.queues || os.cpus().length);
    }
});
