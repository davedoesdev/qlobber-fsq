
describe('single message lock behaviour', function ()
{
    if (!single_supported)
    {
        return;
    }

    var fname;

    beforeEach(function (cb)
    {
        fs.mkdtemp(path.join(os.tmpdir(), 'qlobber-fsq-test'), function (err, dname)
        {
            if (err) { return cb(err); }
            fname = path.join(dname, 'test');
            fs.createWriteStream(fname).end('0123', function ()
            {
                fs.stat(fname, function (err, stats)
                {
                    if (err) { return cb(err); }
                    expect(stats.size).to.equal(4);
                    cb();
                });
            });
        });
    });

    afterEach(function (cb)
    {
        fs.unlink(fname, function (err)
        {
            if (err && (err.code !== 'ENOENT')) { return cb(err); }
            fs.rmdir(path.dirname(fname), cb);
        });
    });

    it('should lock file', function (cb)
    {
        fs.open(fname, constants.O_RDWR, function (err, fd)
        {
            if (err) { return cb(err); }
            fs.open(fname, constants.O_RDWR, function (err, fd2)
            {
                if (err) { return cb(err); }
                fs.flock(fd, 'exnb', function (err)
                {
                    if (err) { return cb(err); }
                    fs.flock(fd2, 'exnb', function (err)
                    {
                        expect(err.code).to.equal(process.platform === 'win32' ? 'EWOULDBLOCK' : 'EAGAIN');
                        fs.close(fd, function (err)
                        {
                            if (err) { return cb(err); }
                            fs.flock(fd2, 'exnb', function (err)
                            {
                                if (err) { return cb(err); }
                                fs.close(fd2, cb);
                            });
                        });
                    });
                });
            });
        });
    });

    it('should truncate file', function (cb)
    {
        fs.open(fname, constants.O_RDWR, function (err, fd)
        {
            if (err) { return cb(err); }
            fs.open(fname, constants.O_RDWR, function (err, fd2)
            {
                if (err) { return cb(err); }
                fs.flock(fd, 'exnb', function (err)
                {
                    if (err) { return cb(err); }
                    var stream = fs.createReadStream(null,
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
                        expect(got_data).to.equal(1);

                        var stream2 = fs.createReadStream(null,
                        {
                            fd: fd,
                            autoClose: false,
                            start: 1
                        }),
                        bufs = [];

                        stream2.on('readable', function ()
                        {
                            var data = this.read();
                            if (data)
                            {
                                bufs.push(data);
                            }
                        });

                        stream2.on('end', function ()
                        {
                            expect(Buffer.concat(bufs).toString()).to.equal('123');
                            fs.ftruncate(fd, 0, function (err)
                            {
                                if (err) { return cb(err); }

                                function unlock()
                                {
                                    fs.flock(fd, 'un', function (err)
                                    {
                                        if (err) { return cb(err); }
                                        fs.flock(fd2, 'exnb', function (err)
                                        {
                                            if (err) { return cb(err); }

                                            var stream3 = fs.createReadStream(null,
                                            {
                                                fd: fd2,
                                                autoClose: false,
                                                start: 0,
                                                end: 0
                                            }),
                                            got_data = 0;

                                            stream3.on('readable', function ()
                                            {
                                                var data = this.read();
                                                if (data)
                                                {
                                                    got_data += data.length;
                                                }
                                            });

                                            stream3.once('end', function ()
                                            {
                                                expect(got_data).to.equal(0);
                                                fs.close(fd, function (err)
                                                {
                                                    if (err) { return cb(err); }
                                                    fs.close(fd2, cb);
                                                });
                                            });
                                        });
                                    });
                                }

                                if (process.platform === 'win32')
                                {
                                    return unlock();
                                }

                                fs.unlink(fname, function (err)
                                {
                                    if (err) { return cb(err); }
                                    unlock();
                                });
                            });
                        });
                    });
                });
            });
        });
    });
});

