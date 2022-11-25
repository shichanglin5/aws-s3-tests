import itertools
import uuid

import boto3
from botocore import UNSIGNED
from botocore.client import (
    Config as S3Config
)
from botocore.exceptions import ClientError

BUCKET_PREFIX = '1-aws-s3-tests-bucket'
BUCKET_ORDINAL = itertools.count(1)


def newBucketName():
    global BUCKET_ORDINAL
    current_bucket_ordinal = next(BUCKET_ORDINAL)
    return '%s-%d' % (BUCKET_PREFIX, current_bucket_ordinal)


def generateBucketName(serviceModel=None, suiteLocals=None, caseLocals=None):
    if suiteLocals is None:
        suiteLocals = {}
    bucketName = newBucketName()
    result = {'Bucket': bucketName}
    suiteLocals.update(result)
    return result


def generateObjectKey(serviceModel=None, suiteLocals=None, caseLocals=None):
    if 'Prefix' in caseLocals:
        Prefix = caseLocals['Prefix']
    else:
        Prefix = ''

    ObjectKey = Prefix + uuid.uuid1().hex
    result = {'Key': ObjectKey}
    suiteLocals.update(result)
    return result


def DeleteObjects(serviceModel=None, suiteLocals=None, caseLocals=None):
    client = caseLocals['Client']
    Bucket = caseLocals['Bucket']

    apiInvokedCount = 0
    try:
        apiInvokedCount += 1
        response = client.list_objects(Bucket=Bucket)
        while True:
            finalResponse = response
            if 'Contents' in response and (objects := response['Contents']):
                apiInvokedCount += 1
                objectIdentifierList = [{'Key': obj['Key'] for obj in objects}]
                finalResponse = client.delete_objects(Bucket=Bucket, Delete={
                    'Objects': objectIdentifierList
                })
            if 'IsTruncated' in response and response['IsTruncated'] and 'NextMarker' in response and (
                    NextMarker := response['NextMarker']):
                apiInvokedCount += 1
                response = client.list_objects(Bucket=Bucket, Marker=NextMarker)
            else:
                return finalResponse
    except ClientError as e:
        return e.response
    finally:
        serviceModel.extra_case_api_invoked_count += apiInvokedCount - 1


predefinedFuncDict = {
    'GenerateBucketName': generateBucketName,
    'GenerateObjectKey': generateObjectKey,
    'DeleteObjects': DeleteObjects
}


def newAnonymousClient(serviceName):
    return boto3.client(service_name=serviceName, use_ssl=False, verify=False,
                        config=S3Config(signature_version=UNSIGNED))


def newAwsClient(serviceName, clientConfig):
    return boto3.client(serviceName, **clientConfig)
