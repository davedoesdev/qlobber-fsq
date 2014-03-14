/*jslint node: true */
"use strict";

var fs = require('fs'),
    dir = JSON.parse(new Buffer(process.argv[2], 'hex')),
    first = [];

process.on('message', function (msg)
{
    //console.log("child got message", msg);

    if (msg.type === 'first')
    {
        fs.readdir(dir, function (err, files)
        {
            first = files;
            process.send(
            {
                type: 'first_done',
                err: err
            });
        });
    }
    else if (msg.type === 'second')
    {
        fs.readdir(dir, function (err, files)
        {
            process.send(
            {
                type: 'second_done',
                err: err,
                first: first,
                second: files
            });
        });
    }
    else if (msg.type === 'exit')
    {
        process.exit();
    }
});

process.send({ type: 'start' });
