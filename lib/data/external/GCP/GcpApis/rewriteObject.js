const async = require('async');
const request = require('request');
const { errors } = require('arsenal');

const { logger, JsonError } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * rewriteObject - copy object between buckets of different storage class or
 * regions. As copyObject has incosistent results when performed on large
 * objects across different buckets
 * @param {object} params - JSON request parameters
 * @param {string} params.SourceBucket - copy source bucket
 * @param {string} params.SourceObject - copy source object
 * @param {string} params.SourceVersionId - specify source version
 * @param {string} params.DestinationBucket - copy destination bucket
 * @param {string} params.DestinationObject - copy destination object
 * @param {string} param.RewriteToken - token to pick up where previous rewrite
 * had left off
 * @param {function} callback - callback function to call with object rewrite
 * results
 * @return {undefined}
 */
function _rewriteObject(params, callback) {
    async.waterfall([
        next => {
            this.getToken((err, res) => next(err, res));
        },
        (token, next) => {
            const uri = '/storage/v1' +
                        `/b/${encodeURIComponent(params.SourceBucket)}` +
                        `/o/${encodeURIComponent(params.SourceObject)}` +
                        '/rewriteTo' +
                        `/b/${encodeURIComponent(params.DestinationBucket)}` +
                        `/o/${encodeURIComponent(params.DestinationObject)}`;
            const qs = {
                sourceGeneration: params.SourceVersionId,
                rewriteToken: params.RewriteToken,
            };
            request({
                method: 'POST',
                baseUrl: this.config.jsonEndpoint,
                uri,
                qs,
                auth: { bearer: token } },
            (err, resp, body) => {
                if (err) {
                    logHelper(logger, 'error',
                        'copyObject: err in json method',
                        errors.InternalError.customizeDescription(
                            'json method copyObject failed'));
                    return next(errors.InternalError
                        .customizeDescription('err in JSON Request'));
                }
                if (resp.statusCode >= 300) {
                    return next(
                        new JsonError(resp.statusMessage, resp.statusCode));
                }
                let res;
                try {
                    res = JSON.parse(body);
                } catch (err) { res = undefined; }
                if (res && res.error && res.error.code >= 300) {
                    return next(
                        new JsonError(res.error.message, res.error.code));
                }
                return next(null, res);
            });
        },
    ], (err, result) => callback(err, result));
}

module.exports = _rewriteObject;
