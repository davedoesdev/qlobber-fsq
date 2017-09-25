const util = require('util'),
	  path = require('path'),
	  async = require('async'),
	  Getdents = require('getdents').Getdents;

module.exports = function (size, num_buckets, caches, pending, make_fname_handler)
{
    const ths = this,
		  bucket_getdents = [],
    	  popen = util.promisify(this._fs.open),
     	  pclose = util.promisify(this._fs.close);

    for (let count = 0; count < num_buckets; count += 1)
    {
        bucket_getdents[count] = new Getdents(size);
    }

    return async function (bucket, extra_matcher, cb)
    {
        let bucket_fmt = ths._format_bucket(bucket),
            cache = caches.get(bucket),
            cache2 = new Map(),
            pending2 = [];

        cache2.bucket_fmt = bucket_fmt;
        caches.set(bucket, cache2);
        pending.set(bucket, pending2);

        let queue = async.queue(make_fname_handler(cache,
                                                   cache2,
                                                   pending2,
                                                   extra_matcher),
                                ths._message_concurrency);

        let called = false;
        let done = false;
        let count = 0;

        function check(err)
        {
            if ((ths._error(err) ||
                 (done && (count === 0))) &&
                !ths._chkstop() &&
                !called)
            {
                called = true;
                cb();
            }
        }

        function processed()
        {
            count--;
            check();
        }

        try
        {
            let fd = await popen(ths._msg_dir + path.sep + bucket_fmt, 'r');

            try
            {
                let getdents = bucket_getdents[bucket];
                getdents.reset(fd);

                for await (let _ of getdents)
                {
                    if (getdents.type !== Getdents.DT_DIR)
                    {
                        count++;
                        queue.push(getdents.name, processed);
                    }
                }
            }
            finally
            {
                await pclose(fd);
                done = true;
                check();
            }
        }
        catch (err)
        {
            check(err);
        }
    }
};

