/*globals global,
          path: false,
          argv: false,
          beforeEach: false,
          afterEach: false,
          fsq_dir: false,
          constants: false,
          QlobberFSQ: false,
          async: false,
          wu: false,
          rimraf: false,
          fsq: true,
          expect: false,
          flags: false,
          retry_interval: false,
          ignore_ebusy: false,
          single_supported: false */
/*jslint node: true, nomen: true, bitwise: true */
"use strict";

try
{
    global.fsext = require('fs-ext');
    global.single_supported = true;
}
catch (ex)
{
    if (process.env.REQUIRE_SINGLE === 'true')
    {
        throw ex;
    }
    global.single_supported = false;
}
global.fs = require('fs');
global.path = require('path');
global.crypto = require('crypto');
global.os = require('os');
global.events = require('events');
global.util = require('util');
global.child_process = require('child_process');
global.rimraf = require('rimraf');
global.async = require('async');
global.wu = require('wu');
global.constants = require('constants');
global.expect = require('chai').expect;
global.cp_remote = require('cp-remote');
global.QlobberFSQ = require('..').QlobberFSQ;
global.argv = require('yargs').argv;

global.fsq = null;
global.fsq_dir = path.join(argv['fsq-dir'] || path.join(__dirname, 'fsq'), 'fsq');
global.msg_dir = path.join(fsq_dir, 'messages');
global.flags = 0;
global.retry_interval = 5;

global.default_options = {
    fsq_dir: fsq_dir,
    flags: flags,
    retry_interval: retry_interval
};

var counters_before;

const { lsof: list_open_files } = require('list-open-files');
global.lsof = async function ()
{
    const all = os.platform() === 'win32' ? [] : (await list_open_files())[0].files;
    return [all.filter(f => {
        return f.type === 'DIR' ||
               f.type === 'LINK' ||
               f.type === 'REG' ||
               f.type === 'PSXSHM';
    }).map(f => {
        return {
            fd: f.fd,
            type: f.type,
            name: f.name
        };
    }), all];
}

if (argv.direct)
{
    global.flags |= constants.O_DIRECT;
}

if (argv.sync)
{
    global.flags |= constants.O_SYNC;
}

global.ignore_ebusy = function (fsq, extra)
{
    fsq.on('warning', function (err)
    {
        if (!(err && (err.code === 'EBUSY' || (extra && err.code === extra))))
        {
            console.error(err);
        }
    });

    fsq.on('single_disabled', function ()
    {
        if (process.env.REQUIRE_SINGLE === 'true')
        {
            throw new Error('single required');
        }
    });
};

beforeEach(function (done)
{
    this.timeout(10 * 60 * 1000);

    async.series([
        function (cb)
        {
            function cleanup()
            {
                rimraf(fsq_dir, function (err)
                {
                    // FhGFS can return EBUSY. Sometimes it erroneously
                    // returns EBUSY instead of ENOTEMPTY, meaning rimraf
                    // never deletes the directory. Interestingly, modifying
                    // rimraf to remove children when it sees EBUSY results
                    // in it successfully removing the directory.
                    if (err && (err.code === 'EBUSY'))
                    {
                        console.error(err);
                        return setTimeout(cleanup, 1000);
                    }

                    cb(err);
                });
            }

            cleanup();
        },
        function (cb)
        {
            fsq = new QlobberFSQ(default_options);
            ignore_ebusy(fsq);
            fsq.on('start', async function ()
            {
                counters_before = await lsof();
                cb();
            });
        }], done);
});

afterEach(function (done)
{
    this.timeout(10 * 60 * 1000);

    fsq.stop_watching(async function ()
    {
        let counters_after = await lsof();
        try
        {
            expect(counters_after[0]).to.eql(counters_before[0]);
        }
        catch (ex)
        {
            return setTimeout(async function ()
            {
                counters_after = await lsof();
                try
                {
                    expect(counters_after[0]).to.eql(counters_before[0]);
                }
                catch (ex)
                {
                    console.error(counters_before[1], counters_after[1]);
                    return done(ex);
                }
                done();
            }, 60000);
        }
        done();
    });
});

global.get_message_files = function (dir, cb)
{
    fs.readdir(dir, function (err, files)
    {
        if (err) { return cb(err); }

        async.mapSeries(files, function (subdir, next)
        {
            fs.readdir(path.join(dir, subdir), next);
        }, function (err, files)
        {
            if (err) { return cb(err); }
            cb(null, [].concat.apply([], files));
        });
    });
};

global.check_empty = function (dir, done, cb)
{
    fs.readdir(dir, function (err, files)
    {
        if (err) { return done(err); }

        async.eachSeries(files, function (subdir, next)
        {
            fs.readdir(path.join(dir, subdir), function (err, files)
            {
                if (err) { return next(err); }
                expect(files).to.eql([]);
                next();
            });
        }, function (err)
        {
            if (err) { return done(err); }
            cb();
        });
    });
};

global.sum = function (buf)
{
    var i, r = 0;

    for (i = 0; i < buf.length; i += 1)
    {
        r += buf[i];
    }

    return r;
};

