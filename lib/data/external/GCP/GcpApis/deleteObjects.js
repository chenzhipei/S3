const async = require('async');
const request = require('request');
const uuid = require('uuid/v4');
const { errors } = require('arsenal');

const { logger, JsonError } = require('../GcpUtils');
const { logHelper } = require('../../utils');

function formBatchRequest(bucket, deleteList) {
    let retBody = '';
    const boundary = uuid().replace(/-/g, '');

    deleteList.forEach(object => {
        // add boundary
        retBody += `--${boundary}\n`;
        // add req headers
        retBody += `Content-Type: application/http\n`;
        retBody += '\n';
        const key = object.Key;
        const versionId = object.VersionId;
        let path = `/storage/v1/b/${bucket}/o/${encodeURIComponent(key)}`;
        if (versionId) path += `?generation=${versionId}`;
        retBody += `DELETE ${path} HTTP/1.1\n`;
        retBody += '\n';
    });
    retBody += `--${boundary}\n`;
    return { body: retBody, boundary };
}

/**
 * deleteObjects - delete a list of objects
 * @param {object} params - deleteObjects parameters
 * @param {string} params.Bucket - bucket location
 * @param {object} params.Delete - delete config object
 * @param {object[]} params.Delete.Objects - a list of objects to be deleted
 * @param {string} params.Delete.Objects[].Key - object key
 * @param {string} params.Delete.Objects[].VersionId - object version Id, if
 * not given the master version will be archived
 * @param {function} callback - callback function to call when a batch response
 * is returned
 * @return {undefined}
 */
function _deleteObjects(params, callback) {
    if (!params || !params.Delete || !params.Delete.Objects) {
        return callback(errors.MalformedXML);
    }
    return async.waterfall([
        next => {
            this.getToken((err, res) => next(err, res));
        },
        (token, next) => {
            const { body, boundary } =
                formBatchRequest(params.Bucket, params.Delete.Objects, token);
            request({
                method: 'POST',
                baseUrl: this.config.jsonEndpoint,
                uri: '/batch',
                headers: {
                    'Content-Type': `multipart/mixed; boundary=${boundary}`,
                },
                body,
                auth: { bearer: token },
            }, (err, resp, body) => {
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
                // batch response is a string of http bodies
                // attempt to parse response body
                // if body element can be transformed into an object
                // there then check if the response is a error object
                // TO-DO: maybe, check individual batch op response
                let res;
                try {
                    res = JSON.parse(body);
                } catch (err) { res = undefined; }
                if (res && res.error && res.error.code >= 300) {
                    return next(
                        new JsonError(res.error.message, res.error.code));
                }
                return next(null);
            });
        },
    ], (err, result) => callback(err, result));
}

module.exports = _deleteObjects;
