const async = require('async');
const { errors } = require('arsenal');
const { _verifyUploadId } = require('./mpuHelper');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * uploadPartCopy - upload part copy
 * @param {object} params - upload part copy params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} params.CopySource - source object to copy
 * @param {function} callback - callback function to call
 * @return {undefined}
 */
function _uploadPartCopy(params, callback) {
    if (!params || !params.UploadId || !params.Bucket || !params.Key ||
        !params.CopySource) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'err in uploadPartCopy', error);
        return callback(error);
    }
    const mpuParams = {
        Bucket: params.Bucket,
        Key: createMpuKey(params.Key, params.UploadId, params.PartNumber),
        CopySource: params.CopySource,
    };
    return async.waterfall([
        next => (_verifyUploadId.call(this, params, next)),
        next => this.copyObject(mpuParams, (err, res) => {
            if (err) {
                logHelper(logger, 'error',
                    'err in uploadPartCopy - copyObject', err);
                return next(err);
            }
            const CopyPartObject = { CopyPartResult: res.CopyObjectResult };
            return next(null, CopyPartObject);
        }),
    ], (err, res) => {
        if (err) {
            return callback(err);
        }
        return callback(null, res);
    });
}

module.exports = _uploadPartCopy;
