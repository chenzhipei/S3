const async = require('async');
const crypto = require('crypto');
const { errors } = require('arsenal');
const { _removeParts, _splitMerge, _verifyUploadId,
    _retryCompose, _retryCopy } = require('./mpuHelper');
const { createMpuList, createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * completeMPU - merges a list of parts into a single object
 * @param {object} params - completeMPU params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.MPU - mpu bucket name
 * @param {string} params.Overflow - overflow bucket name
 * @param {string} params.Key - object key
 * @param {number} params.UploadId - MPU upload id
 * @param {Object} params.MultipartUpload - MPU upload object
 * @param {Object[]} param.MultipartUpload.Parts - a list of parts to merge
 * @param {function} callback - callback function to call with MPU result
 * @return {undefined}
 */
function _completeMPU(params, callback) {
    if (!params || !params.MultipartUpload ||
        !params.MultipartUpload.Parts || !params.UploadId ||
        !params.Bucket || !params.Key) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'err in completeMultipartUpload', error);
        return callback(error);
    }
    const partList = params.MultipartUpload.Parts;
    // verify that the part list is in order
    if (params.MultipartUpload.Parts.length <= 0) {
        const error = errors.InvalidRequest
            .customizeDescription('You must specify at least one part');
        logHelper(logger, 'error', 'err in completeMultipartUpload', error);
        return callback(error);
    }
    for (let ind = 1; ind < partList.length; ++ind) {
        if (partList[ind - 1].PartNumber >= partList[ind].PartNumber) {
            logHelper(logger, 'error', 'err in completeMultipartUpload',
                errors.InvalidPartOrder);
            return callback(errors.InvalidPartOrder);
        }
    }

    const _copyToOverflow = (numParts, callback) => {
        // copy phase: in overflow bucket
        // resetting component count by moving item between
        // different region/class buckets
        logger.trace('completeMultipartUpload: copy to overflow',
            { partCount: numParts });
        const parts = createMpuList(params, 'mpu2', numParts);
        let doneCount = 0;
        if (parts.length !== numParts) throw errors.InternalError;
        return async.each(parts, (infoParts, cb) => {
            const partName = infoParts.PartName;
            const partNumber = infoParts.PartNumber;
            const overflowKey = createMpuKey(
                params.Key, params.UploadId, partNumber, 'overflow');
            const rewriteParams = {
                SourceBucket: params.MPU,
                SourceObject: partName,
                DestinationBucket: params.Overflow,
                DestinationObject: overflowKey,
            };
            logger.trace('rewrite object', { rewriteParams });
            let rewriteDone = false;
            async.whilst(() => !rewriteDone, moveOn => {
                this.rewriteObject(rewriteParams, (err, res) => {
                    if (err) {
                        logHelper(logger, 'error', 'err in ' +
                            'createMultipartUpload - rewriteObject', err);
                    } else {
                        rewriteDone = res.done;
                        rewriteParams.RewriteToken = res.rewriteToken;
                    }
                    return moveOn(err);
                });
            }, err => {
                if (!err) {
                    doneCount++;
                }
                return cb(err);
            });
        }, err => {
            if (err) {
                return callback(err);
            }
            return callback(null, doneCount);
        });
    };

    const _composeOverflow = (numParts, callback) => {
        // final compose: in overflow bucket
        // number of parts to compose <= 10
        // perform final compose in overflow bucket
        logger.trace('completeMultipartUpload: overflow compose');
        const parts = createMpuList(params, 'overflow', numParts);
        const partList = parts.map(item => (
            { PartName: item.PartName }));
        if (partList.length < 2) {
            logger.trace(
                'fewer than 2 parts in overflow, skip to copy phase');
            return callback(null, partList[0].PartName);
        }
        const composeParams = {
            Bucket: params.Overflow,
            Key: createMpuKey(params.Key, params.UploadId, 'final'),
            MultipartUpload: { Parts: partList },
        };
        return _retryCompose.call(this, composeParams, 0, err => {
            if (err) {
                return callback(err);
            }
            return callback(null, null);
        });
    };

    const _copyToMain = (res, callback) => {
        // move object from overflow bucket into the main bucket
        // retrieve initial metadata then compose the object
        const copySource = res ||
            createMpuKey(params.Key, params.UploadId, 'final');
        return async.waterfall([
            next => {
                // retrieve metadata from init object in mpu bucket
                const headParams = {
                    Bucket: params.MPU,
                    Key: createMpuKey(params.Key, params.UploadId,
                        'init'),
                };
                logger.trace('retrieving object metadata');
                return this.headObject(headParams, (err, res) => {
                    if (err) {
                        logHelper(logger, 'error',
                            'err in createMultipartUpload - headObject',
                            err);
                        return next(err);
                    }
                    return next(null, res.Metadata);
                });
            },
            (metadata, next) => {
                // copy the final object into the main bucket
                const copyParams = {
                    Bucket: params.Bucket,
                    Key: params.Key,
                    Metadata: metadata,
                    MetadataDirective: 'REPLACE',
                    CopySource: `${params.Overflow}/${copySource}`,
                };
                logger.trace('copyParams', { copyParams });
                _retryCopy.call(this, copyParams, 0, (err, res) => {
                    if (err) {
                        logHelper(logger, 'error', 'err in ' +
                            'createMultipartUpload - final copyObject',
                            err);
                        return next(err);
                    }
                    return next(null, res);
                });
            },
        ], (err, copyResult) => callback(err, copyResult));
    };

    /*
     * The returned etag in MPU is different from that of regular put and copy
     * Steps for generating the MPU Etag are:
     * - Concatenate the binary format of each upload etags
     * - generate the has for the concatenated etag as hex
     * - append "-X", where X = number of parts merged
     * Resources about the matter:
     * https://forums.aws.amazon.com/thread.jspa?messageID=203436&#203436
     * http://permalink.gmane.org/gmane.comp.file-systems.s3.s3tools/583
     */
    const _generateMpuResult = (copyRes, callback) => {
        const partListEtags = partList.map(
            item => Buffer.from(
                item.ETag.substring(1, item.ETag.length - 1), 'hex'));
        const tmpEtag = Buffer.concat(partListEtags);
        const etagVal = crypto.createHash('md5').update(tmpEtag).digest('hex');
        const ETag = `"${etagVal}-${partList.length}"`;
        const mpuResult = {
            Bucket: params.Bucket,
            Key: params.Key,
            VersionId: copyRes.VersionId,
            ETag,
        };
        return callback(null, mpuResult);
    };

    return async.waterfall([
        next => _verifyUploadId.call(this, {
            Bucket: params.MPU,
            Key: params.Key,
            UploadId: params.UploadId,
        }, next),
        next => {
            // first compose: in mpu bucket
            // max 10,000 => 313 parts
            // max component count per object 32
            logger.trace('completeMultipartUpload: compose round 1',
                { partCount: partList.length });
            _splitMerge.call(this, params, partList, 'mpu1', next);
        },
        (numParts, next) => {
            // second compose: in mpu bucket
            // max 313 => 10 parts
            // max component count per object 1024
            logger.trace('completeMultipartUpload: compose round 2',
                { partCount: numParts });
            const parts = createMpuList(params, 'mpu1', numParts);
            if (parts.length !== numParts) throw errors.InternalError;
            return _splitMerge.call(this, params, parts, 'mpu2', next);
        },
        (numParts, next) => _copyToOverflow(numParts, next),
        (numParts, next) => _composeOverflow(numParts, next),
        (res, next) => _copyToMain(res, next),
        (copyRes, next) => _generateMpuResult(copyRes, next),
        (mpuResult, next) => {
            const delParams = {
                Bucket: params.Bucket,
                MPU: params.MPU,
                Overflow: params.Overflow,
                Prefix: createMpuKey(params.Key, params.UploadId),
            };
            return _removeParts.call(this, delParams, err => {
                if (err) {
                    return next(err);
                }
                return next(null, mpuResult);
            });
        },
    ], (err, mpuResult) => {
        if (err) {
            return callback(err);
        }
        return callback(null, mpuResult);
    });
}

module.exports = _completeMPU;
