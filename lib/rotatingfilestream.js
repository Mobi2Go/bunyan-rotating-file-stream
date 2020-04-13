/* global Buffer */
// A rotating file stream will just
// stream to a file and rotate the files when told to

'use strict';

var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var util = require('util');
var iopath = require('path');
var async = require('async');
var _ = require('lodash');
var jsesc = require('jsesc');

var optionParser = require('./optionParser');
var LimitedQueue = require('./limitedqueue');
var FileRotator = require('./filerotator');
var semver = require('semver');

var bunyan = require('bunyan');

var _DEBUG = false;

var internalFields = [
    "name",
    "hostname",
    "pid",
    "level",
    "msg",
    "time",
    "v"
];

// There is an annoying bug where setImmediate sometimes doesn't fire.
// Fixed in node v5
var nextTick = semver.gt(process.version, '5.0.0') ?
    setImmediate :
    function (next) { setTimeout(next, 0); };

function RotatingFileStream(options) {
    var base = new EventEmitter();

    var gzip = Boolean(options.gzip);
    var totalSize = optionParser.parseSize(options.totalSize);
    var totalFiles = options.totalFiles;
    var path = options.path;
    var shared = options.shared;

    var rotator = FileRotator(path, totalFiles, totalSize, gzip);

    var stream = null;
    var streambytesWritten = 0;

    // Copied from bunyan source
    function safeCycles() {
        var seen = [];
        return function (key, val) {
            if (!val || typeof (val) !== 'object') {
                return val;
            }
            if (seen.indexOf(val) !== -1) {
                return '[Circular]';
            }
            seen.push(val);
            return val;
        };
    }

    function nullJsonify(textlog) {
        return textlog;
    }

    function fastJsonify(rawlog) {
        return JSON.stringify(rawlog, safeCycles()) + '\n';
    }

    function fastUnsafeJsonify(rawlog) {
        return JSON.stringify(rawlog) + '\n';
    }

    function orderedJsonify(rawlog) {
        var log = {};

        var fo = options.fieldOrder;

        for (var sortIndex = 0; fo && sortIndex < fo.length; sortIndex += 1) {
            if (rawlog.hasOwnProperty(options.fieldOrder[sortIndex])) {
                log[fo[sortIndex]] = rawlog[fo[sortIndex]];
            }
        }

        for (var k in rawlog) {
            log[k] = rawlog[k];
        }

        return JSON.stringify(log, safeCycles()) + '\n';
    }

    function chooseJsonify(log) {
        if (typeof (log) === 'string' && options.fieldOrder) {
            base.emit(
                'error',
                'Can only set fieldOrder with the stream set to "raw"'
            );
        }

        if (typeof (log) === 'string') {
            jsonify = nullJsonify;
        } else if (options.fieldOrder) {
            jsonify = orderedJsonify;
        } else if (options.noCyclesCheck) {
            jsonify = fastUnsafeJsonify;
        } else {
            jsonify = fastJsonify;
        }

        return jsonify(log);
    };

    var jsonify = chooseJsonify;

    options.map = options.map || function (log) { return log; }

    function writer(logs, callback) {
        var written = -1; // the index of the last successful write
        var bytesWritten = 0; // Bytes written to the stream this batch
        for (var i = 0; stream && i < logs.length; i += 1) {
            var log = options.map(logs[i]);

            if (log) {
                var str = jsonify(log);

                var writeBuffer = new Buffer(str, 'utf8');

                var emitinfo = {
                    logSize: writeBuffer.length,
                    logstr: str
                };

                base.emit('logwrite', emitinfo);

                bytesWritten += writeBuffer.length;

                if (stream) {
                    try {
                        stream.write(writeBuffer, function (err) {
                            if (err) {
                                base.emit('error', err);
                            }
                        });
                    } catch (err) {
                        base.emit('error', err);
                    }

                    written = i;
                }
            }
        }

        // If we didn't get all the way through the array, unshift the remaining
        // records back onto our queue in reverse order
        for (var rollback = logs.length -1; rollback > written; rollback -= 1) {
            writeQueue.unshift(logs[rollback]);
        }

        nextTick(callback);

        base.emit('perf-writebatch', bytesWritten, written + 1, writeQueue.length());
    }

    var writeQueue = LimitedQueue(writer);

    writeQueue.pause();

    writeQueue.on('losingdata', function () {
        base.emit('losingdata');
    });

    writeQueue.on('caughtup', function () {
        base.emit('caughtup');
    });

    rotator.on('error', function (err) {
        base.emit('error', err);
    });

    rotator.on('closefile', function () {
        writeQueue.pause();
        stream = null;
    });

    rotator.on('newfile', function (newfile) {
        stream = newfile.stream;
        streambytesWritten = 0;
        base.emit('newfile', newfile);
        writeQueue.resume();
    });

    function initialise() {
        rotator.initialise(options.startNewFile, function (err) {
            if (err) {
                base.emit('error', err);
            }
        });
    }

    function rotateActual(triggerinfo) {
        var rotateStart = Date.now();

        rotateFunction = function () {};

        rotator.rotate(triggerinfo, function (err) {
            if (err) {
                base.emit('error', err);
            }

            rotateFunction = rotateActual;

            base.emit('perf-rotation', Date.now() - rotateStart);
        });
    }

    var rotateFunction = rotateActual;

    function rotate(triggerinfo) {
        rotateFunction(triggerinfo);
    }

    function severityFromLevel(level) {
        switch(level) {
            case 10:
                return "TRACE";
            case 20:
                return "DEBUG";
            case 40:
                return "WARN";
            case 50:
                return "ERROR";
            case 60:
                return "FATAL";
            default:
                return "INFO";
        }
    }

    function formatValue(data) {
        return jsesc(data, {
            quotes: 'double'
        });
    }

    function formatExtraFields(data) {
        var output = '';
        for (var key in data) {
            if (internalFields.indexOf(key) === -1) {
                if (typeof(data[key]) === 'object') {
                    try {
                        output += util.format(" %s=\"%s\"", formatValue(key), formatValue(JSON.stringify(data[key])));
                    } catch (e) {

                    }
                } else {
                    try {
                        output += util.format(" %s=\"%s\"", formatValue(key), (data[key] && data[key].toString) ? formatValue(data[key].toString()) : 'undefined');
                    } catch (e) {

                    }
                }
            }
        }
        return output;
    }

    function write(s, callback) {
        if (typeof(s) === 'object') {
            var output = util.format('[%s] %s.%s L=%s E="%s" pid=%s', s.time.toISOString(), s.name, severityFromLevel(s.level), s.level, s.msg, s.pid);
            output += formatExtraFields(s);
            output += "\n";
            var length = writeQueue.push(output, callback);
            base.emit('perf-queued', length);
        }
    }

    function end(s) {
        writeQueue.pause();
        rotator.end(function () {
            base.emit('shutdown');
        });
    };

    function destroy(s) {
        writeQueue.pause();
        rotator.end();
        base.emit('shutdown');
    };

    function destroySoon(s) {
        writeQueue.pause();
        rotator.end();
        base.emit('shutdown');
    };

    function join(cb) {
        writeQueue.join(function () {
            rotator.end(function () {
                base.emit('shutdown');
                if (cb) {
                    cb();
                }
            });
        });
    }

    return _.extend({}, {
        stream: stream,
        initialise: initialise,
        rotate: rotate,
        write: write,
        end: end,
        destroy: destroy,
        destroySoon: destroySoon,
        join: join,
        shared: shared
    }, base);
}

module.exports = RotatingFileStream;
