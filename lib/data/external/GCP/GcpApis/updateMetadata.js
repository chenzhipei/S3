const async = require('async');
const request = require('request');
const { errors } = require('arsenal');

const { logger, JsonError } = require('../GcpUtils');
const { logHelper } = require('../../utils');

/**
 * updateMetadata - update the metadata of an object. Only used in when
 * changes to an object metadata should not affect the version id. Example:
 * objectTagging, in which creation/deletion of medatadata is required for GCP,
 * and copyObject.
 * @param {object} params - update metadata params
 * @param {string} params.Bucket - bucket name
 * @param {string} params.Key - object key
 * @param {string} params.VersionId - object version id
 * @param {function} callback - callback function to call with the object result
 * @return {undefined}
 */
function _updateMetadata(params, callback) {
    async.waterfall([
        next => {
            this.getToken((err, res) => next(err, res));
        },
        (token, next) => {
            const uri = '/storage/v1' +
                        `/b/${encodeURIComponent(params.Bucket)}` +
                        `/o/${encodeURIComponent(params.Key)}`;
            const body = {
                acl: {},
                metadata: params.Metadata,
                generation: params.VersionId,
            };
            request({
                method: 'PUT',
                baseUrl: this.config.jsonEndpoint,
                uri,
                body,
                json: true,
                auth: { bearer: token } },
            (err, resp, body) => {
                if (err) {
                    logHelper(logger, 'error',
                        'updateMetadata: err in json method',
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
                    res = body && typeof body === 'string' ?
                        JSON.parse(body) : body;
                } catch (error) { res = undefined; }
                if (res && res.error && res.error.code >= 300) {
                    return next(
                        new JsonError(res.error.message, res.error.code));
                }
                return next(null, res);
            });
        },
    ], callback);
}

module.exports = _updateMetadata;
