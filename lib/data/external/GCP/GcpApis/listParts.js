const async = require('async');
const { errors } = require('arsenal');
const { _verifyUploadId } = require('./mpuHelper');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * listParts - list uploaded MPU parts
 * @param {object} params - listParts param
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} params.UploadId - MPU upload id
 * @param {function} callback - callback function to call with the list of parts
 * @return {undefined}
 */
function _listParts(params, callback) {
    if (!params || !params.UploadId || !params.Bucket || !params.Key) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'err in listParts', error);
        return callback(error);
    }
    const mpuParams = {
        Bucket: params.Bucket,
        Prefix: createMpuKey(params.Key, params.UploadId, 'parts'),
        MaxKeys: params.MaxParts,
    };
    return async.waterfall([
        next => _verifyUploadId.call(this, {
            Bucket: params.Bucket,
            Key: params.Key,
            UploadId: params.UploadId,
        }, next),
        next => this.listObjects(mpuParams, (err, res) => {
            if (err) {
                logHelper(logger, 'error',
                    'err in listParts - listObjects', err);
                return next(err);
            }
            return next(null, res);
        }),
    ], (err, result) => callback(err, result));
}

module.exports = _listParts;
