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

function eachSlice(size) {
    this.array = [];
    let partNumber = 1;
    for (let ind = 0; ind < this.length; ind += size) {
        this.array.push({
            Parts: this.slice(ind, ind + size),
            PartNumber: partNumber++,
        });
    }
    return this.array;
}

function getRandomInt(min, max) {
    /* eslint-disable no-param-reassign */
    min = Math.ceil(min);
    max = Math.floor(max);
    /* eslint-enable no-param-reassign */
    return Math.floor(Math.random() * (max - min)) + min;
}

function createMpuKey(key, uploadId, partNumber, fileName) {
    /* eslint-disable no-param-reassign */
    if (typeof partNumber === 'string' && fileName === undefined) {
        fileName = partNumber;
        partNumber = null;
    }
    /* esline-enable no-param-reassign */
    if (fileName && typeof fileName === 'string') {
        // if partNumber is given, return a "full file path"
        // else return a "directory path"
        return partNumber ? `${key}-${uploadId}/${fileName}/${partNumber}` :
            `${key}-${uploadId}/${fileName}`;
    }
    if (partNumber && typeof partNumber === 'number') {
        // filename wasn't passed as an argument. Create default
        return `${key}-${uploadId}/parts/${partNumber}`;
    }
    // returns a "directory parth"
    return `${key}-${uploadId}/`;
}

function createMpuList(params, level, size) {
    // populate and return a parts list for compose
    const retList = [];
    for (let i = 1; i <= size; ++i) {
        retList.push({
            PartName: `${params.Key}-${params.UploadId}/${level}/${i}`,
            PartNumber: i,
        });
    }
    return retList;
}

module.exports = {
    // functions
    eachSlice,
    getRandomInt,
    createMpuKey,
    createMpuList,
    // util objects
    JsonError,
    logger,
};
