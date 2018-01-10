const async = require('async');
const { errors } = require('arsenal');
const { _verifyUploadId, _removeParts } = require('./mpuHelper');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * abortMPU - remove all objects of a GCP Multipart Upload
 * @param {object} params - abortMPU params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.MPU - mpu bucket name
 * @param {string} params.Overflow - overflow bucket name
 * @param {string} params.Key - object key
 * @param {number} params.UploadId - MPU upload id
 * @param {function} callback - callback function to call
 * @return {undefined}
 */
function _abortMPU(params, callback) {
    if (!params || !params.Key || !params.UploadId ||
        !params.Bucket || !params.MPU || !params.Overflow) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'err in abortMultipartUpload', error);
        return callback(error);
    }
    const delParams = {
        Bucket: params.Bucket,
        MPU: params.MPU,
        Overflow: params.Overflow,
        Prefix: createMpuKey(params.Key, params.UploadId),
    };
    return async.waterfall([
        next => _verifyUploadId.call(this, {
            Bucket: params.MPU,
            Key: params.Key,
            UploadId: params.UploadId,
        }, next),
        next => _removeParts.call(this, delParams, err => next(err)),
    ], err => callback(err));
}

module.exports = _abortMPU;
