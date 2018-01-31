const werelogs = require('werelogs');

const _config = require('../../../Config.js').config;

werelogs.configure({
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});

const logger = new werelogs.Logger('gcpUtil');

class JsonError extends Error {
    constructor(type, code, desc) {
        super(type);
        this.code = code;
        this.description = desc;
        this[type] = true;
    }
}

module.exports = {
    // util objects
    JsonError,
    logger,
};
