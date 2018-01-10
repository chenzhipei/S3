const assert = require('assert');
const async = require('async');
const { GCP, GcpUtils } = require('../../../../../../lib/data/external/GCP');
const { gcpRequestRetry, setBucketClass, gcpMpuSetup } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: `somebucket-${Date.now()}`,
        Type: 'MULTI_REGIONAL',
    },
    mpu: {
        Name: `mpubucket-${Date.now()}`,
        Type: 'REGIONAL',
    },
    overflow: {
        Name: `overflowbucket-${Date.now()}`,
        Type: 'MULTI_REGIONAL',
    },
};
const numParts = 10;
const partSize = 10;

function gcpMpuSetupWrapper(params, callback) {
    gcpMpuSetup(params, (err, result) => {
        assert.equal(err, null,
            `Unable to setup MPU test, error ${err}`);
        const { uploadId, etagList } = result;
        this.currentTest.uploadId = uploadId;
        this.currentTest.etagList = etagList;
        return callback();
    });
}

describe('GCP: Abort MPU', function testSuite() {
    this.timeout(30000);
    let config;
    let gcpClient;

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        async.eachSeries(bucketNames,
            (bucket, next) => gcpRequestRetry({
                method: 'PUT',
                bucket: bucket.Name,
                authCredentials: config.credentials,
                requestBody: setBucketClass(bucket.Type),
            }, 0, err => {
                if (err) {
                    process.stdout.write(`err in creating bucket ${err}\n`);
                }
                return next(err);
            }),
        err => done(err));
    });

    after(done => {
        async.eachSeries(bucketNames,
            (bucket, next) => gcpClient.listObjects({
                Bucket: bucket.Name,
            }, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got error ${err}`);
                async.map(res.Contents, (object, moveOn) => {
                    const deleteParams = {
                        Bucket: bucket.Name,
                        Key: object.Key,
                    };
                    gcpClient.deleteObject(
                        deleteParams, err => moveOn(err));
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got error ${err}`);
                    gcpRequestRetry({
                        method: 'DELETE',
                        bucket: bucket.Name,
                        authCredentials: config.credentials,
                    }, 0, err => {
                        if (err) {
                            process.stdout.write(
                                `err in deleting bucket ${err}\n`);
                        }
                        return next(err);
                    });
                });
            }),
        err => done(err));
    });

    describe('when MPU has 0 parts', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: this.currentTest.key,
                partCount: 0, partSize,
            }, done);
        });

        it('should abort MPU with 0 parts', function testFn(done) {
            return async.waterfall([
                next => {
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Overflow: bucketNames.overflow.Name,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                    };
                    gcpClient.abortMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        return next();
                    });
                },
                next => {
                    const keyName =
                        `${this.test.key}-${this.test.uploadId}/init`;
                    gcpClient.headObject({
                        Bucket: bucketNames.mpu.Name,
                        Key: keyName,
                    }, err => {
                        assert(err);
                        assert.strictEqual(err.code, 404);
                        return next();
                    });
                },
            ], err => done(err));
        });
    });

    describe('when MPU is incomplete', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpMpuSetupWrapper.call(this, {
                gcpClient,
                bucketNames,
                key: this.currentTest.key,
                partCount: numParts, partSize,
            }, done);
        });

        it('should abort incomplete MPU', function testFn(done) {
            return async.waterfall([
                next => {
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Overflow: bucketNames.overflow.Name,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                    };
                    gcpClient.abortMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        return next();
                    });
                },
                next => {
                    const keyName =
                        `${this.test.key}-${this.test.uploadId}/init`;
                    gcpClient.headObject({
                        Bucket: bucketNames.mpu.Name,
                        Key: keyName,
                    }, err => {
                        assert(err);
                        assert.strictEqual(err.code, 404);
                        return next();
                    });
                },
            ], err => done(err));
        });
    });

    describe('when MPU has been completed', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            async.waterfall([
                next => gcpMpuSetupWrapper.call(this, {
                    gcpClient,
                    bucketNames,
                    key: this.currentTest.key,
                    partCount: numParts, partSize,
                }, next),
                next => {
                    const parts = GcpUtils.createMpuList({
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                    }, 'parts', numParts).map(item => {
                        Object.assign(item, {
                            ETag:
                                this.currentTest.etagList[item.PartNumber - 1],
                        });
                        return item;
                    });
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Overflow: bucketNames.overflow.Name,
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                        MultipartUpload: {
                            Parts: parts,
                        },
                    };
                    gcpClient.completeMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Unable to complete MPU test, error ${err}`);
                        return next();
                    });
                },
            ], err => done(err));
        });

        it('should return error', function testFn(done) {
            const params = {
                Bucket: bucketNames.main.Name,
                MPU: bucketNames.mpu.Name,
                Overflow: bucketNames.overflow.Name,
                Key: this.test.key,
                UploadId: this.test.uploadId,
            };
            gcpClient.abortMultipartUpload(params, err => {
                assert(err);
                assert.strictEqual(err.code, 404);
                return done();
            });
        });
    });
});
