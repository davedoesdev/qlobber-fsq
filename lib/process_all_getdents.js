const util = require('util'),
	  path = require('path'),
	  async = require('async'),
	  Getdents = require('getdents').Getdents,
      sleep = util.promisify(setTimeout);

module.exports = function (size,
                           num_buckets,
                           delivered,
                           pending,
                           make_fname_handler)
{
    const ths = this,
		  bucket_getdents = [],
    	  popen = util.promisify(this._fs.open),
     	  pclose = util.promisify(this._fs.close);

    for (let count = 0; count < num_buckets; count += 1)
    {
        let getdents = new Getdents(size);
        getdents.active = false;
        bucket_getdents[count] = getdents;
    }

    let r = async function (bucket,
                            bucket_fmt,
                            extra_matcher,
                            cache,
                            only_check_expired,
                            cb)
    {
        let d = cache ? delivered.get(bucket) : null,
            d2 = cache ? new Set() : null,
            pending2 = [];

        if (cache)
        {
            delivered.set(bucket, d2);
        }

        pending.set(bucket, pending2);

        let queue = async.queue(make_fname_handler(d,
                                                   d2,
                                                   pending2,
                                                   bucket_fmt,
                                                   extra_matcher,
                                                   only_check_expired),
                                ths._message_concurrency);

        let called = false;
        let done = false;
        let count = 0;
        let err = null;

        function check()
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

        if (ths._chkstop()) { return; }

        let getdents = bucket_getdents[bucket];

        try
        {
            getdents.active = true;

            let fd = await popen(ths._msg_dir + path.sep + bucket_fmt, 'r');

            try
            {
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
                while (true)
                {
                    try
                    {
                        await pclose(fd);
                        break;
                    }
                    catch (ex)
                    {
                        if (!ths._try_again(ex))
                        {
                            throw ex;
                        }
                    }
                    await sleep(ths._retry_interval);
                }
                done = true;
            }
        }
        catch (ex)
        {
            err = ex;
        }
        finally
        {
            getdents.active = false;
            check();
        }
    };

    r.stop = async function (cb)
    {
        for (let count = 0; count < num_buckets; count += 1)
        {
            let getdents = bucket_getdents[count];
            if (getdents.active)    
            {
                await sleep(ths._retry_interval);
                count -= 1;
            }
        }
        cb();
    };

    return r;
};

