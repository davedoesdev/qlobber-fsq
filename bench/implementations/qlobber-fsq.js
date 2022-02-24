/*jslint node: true, nomen: true */
"use strict";

var child_process = require('child_process'),
    cp_remote = require('cp-remote'),
    path = require('path'),
    async = require('async'),
    rimraf = require('rimraf'),
    yargs = require('yargs'),
    QlobberFSQ = require('../..').QlobberFSQ,
    num_buckets = QlobberFSQ.get_num_buckets(),
    argv = yargs(JSON.parse(Buffer.from(yargs.argv.data, 'hex')))
            .demand('rounds')
            .demand('size')
            .demand('ttl')
            .option('num_elements', { default: 20 * 1024 })
            .option('element_size', { default: 2048 })
            .option('bucket_stamp_size', { default: 32 })
            .check(function (argv)
            {
                if (!(argv.queues || argv.remote))
                {
                    throw 'missing --queues or --remote';
                }

                if (argv.queues && argv.remote)
                {
                    throw "can't specify --queues and --remote";
                }
                
                return true;
            })
            .argv,
    fsq_dir = path.join(argv['fsq-dir'] || path.join(__dirname, '..', 'fsq'), 'fsq'),
    queues;

if (argv.remote)
{
    if (typeof argv.remote === 'string')
    {
        argv.remote = [argv.remote];
    }

    argv.queues = argv.remote.length;
}

function error(err)
{
    throw err;
}

/*jslint unparam: true */
before(function (times, done)
{
    async.series([
        function (cb)
        {
            rimraf(fsq_dir, cb);
        },
        function (cb)
        {
            if (argv.disruptor)
            {
                var Disruptor = require('shared-memory-disruptor').Disruptor;

                for (var i = 0; i < Math.min(argv.queues, num_buckets); i += 1)
                {
                    var d = new Disruptor('/test' + i,
                                          argv.num_elements,
                                          argv.element_size,
                                          argv.queues,
                                          0,
                                          true,
                                          false);
                    d.release();
                }
            }
            cb();
        },
        function (cb)
        {
            async.times(argv.queues, function (n, cb)
            {
                var bench_fsq = path.join(__dirname, 'bench-fsq', 'bench-fsq.js'),
                    opts = Buffer.from(JSON.stringify({
                        fsq_dir: fsq_dir,
                        n: n,
                        queues: argv.queues,
                        num_buckets: num_buckets,
                        num_elements: argv.num_elements,
                        element_size: argv.element_size,
                        rounds: argv.rounds,
                        size: argv.size,
                        ttl: argv.ttl * 1000,
                        getdents_size: argv.getdents_size,
                        disruptor: argv.disruptor,
                        bucket_stamp_size: argv.bucket_stamp_size,
                        ephemeral: argv.ephemeral,
                        refresh_ttl: argv.refresh_ttl === undefined ? undefined : argv.refresh_ttl * 1000
                    })).toString('hex'),
                    child;
                    
                if (argv.remote)
                {
                    child = cp_remote.run(argv.remote[n], bench_fsq, opts);
                }
                else
                {
                    child = child_process.fork(bench_fsq, [opts]);
                }

                child.on('error', error);
                child.on('exit', error);

                child.on('message', function ()
                {
                    cb(null, child);
                });
            }, function (err, qs)
            {
                queues = qs;
                cb(err);
            });
        }], done);
});
/*jslint unparam: false */

exports.publish = function (done)
{
    async.each(queues, function (q, cb)
    {
        q.removeListener('exit', error);
        q.on('exit', function (code, signal)
        {
            cb(code || signal);
        });
        q.send({ type: 'start' });
    }, done);
};

