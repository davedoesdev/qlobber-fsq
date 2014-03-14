/*global QlobberFSQ: false */
/*jslint node: true, unparam: true */
"use strict";

/**
`start` event

`QlobberFSQ` objects fire a `start` event when they're ready to publish messages. Don't call [`publish`](#qlobberfsqprototypepublish) until the `start` event is emitted or the message may be dropped. You can [`subscribe`](#qlobberfsqprototypesubscribe) to messages before `start` is fired, however.

A `start` event won't be fired after a `stop` event.
*/
QlobberFSQ.events.start = function () { return undefined; };

/**
`stop` event

`QlobberFSQ` objects fire a `stop` event after you call [`stop_watching`](#qlobberfsqstop_watching') and they've stopped scanning for new messages. Messages already read may still be being processed, however.
*/
QlobberFSQ.events.stop = function () { return undefined; };

/**
`error` event

`QlobberFSQ` objects fire an `error` event if an error occurs before `start` is emitted. The `QlobberFSQ` object is unable to continue at this point and is not scanning for new messages.

@param {Object} err The error that occurred.
*/
QlobberFSQ.events.error = function (err) { return undefined; };

/**
`warning` event

`QlobberFSQ` objects fire a `warning` event if an error occurs after `start` is emitted. The `QlobberFSQ` object will still be scanning for new messages after emitting a `warning` event.

@param {Object} err The error that occurred.
*/
QlobberFSQ.events.warning = function (err) { return undefined; };
