import threading
from copy import deepcopy

import boto3
from botocore import UNSIGNED
from botocore.client import (
    Config as S3Config
)
from botocore.exceptions import ClientError

BUCKET_PREFIX = '1-aws-s3-tests-bucket'


def newBucketName():
    global BUCKET_ORDINAL
    current_bucket_ordinal = next(BUCKET_ORDINAL)
    return '%s-%d' % (BUCKET_PREFIX, current_bucket_ordinal)


def setVars(serviceModel=None, suiteLocals=None, caseLocals=None, parameters=None):
    suiteLocals.update(parameters)
    return parameters


def DropAllBuckets(serviceModel=None, suiteLocals=None, caseLocals=None, parameters=None):
    client = caseLocals['Client']
    apiInvokedCount = 0
    try:
        listBucketsResp = client.list_buckets()
        if 'Buckets' in listBucketsResp and (Buckets := listBucketsResp['Buckets']):
            hooks = []
            Buckets = [Bucket['Name'] for Bucket in Buckets]
            listBucketsResp['Buckets'] = Buckets
            for Bucket in Buckets:
                apiInvokedCount += 1
                caseLocalsCopy = caseLocals.copy()
                caseLocalsCopy['Bucket'] = Bucket
                t = threading.Thread(target=DropBucket, args=(serviceModel, suiteLocals, caseLocalsCopy, parameters))
                hooks.append(t.join)
                t.start()
            for hook in hooks:
                hook()
        return listBucketsResp
    except ClientError as e:
        return e.response
    finally:
        serviceModel.increaseExtraCaseApisCount(apiInvokedCount)


def DropBucket(serviceModel=None, suiteLocals=None, caseLocals=None, parameters=None):
    client = caseLocals['Client']
    Bucket = caseLocals['Bucket']

    try:
        response = DropObjects(serviceModel, suiteLocals, caseLocals, parameters)
        if 'ResponseMetadata' in response and (responseMetadata := response['ResponseMetadata']):
            if 400 <= responseMetadata['HTTPStatusCode'] < 500:
                return response
        response = client.delete_bucket(Bucket=Bucket)
        return response
    except ClientError as e:
        return e.response


def DropObjects(serviceModel=None, suiteLocals=None, caseLocals=None, parameters=None):
    client = caseLocals['Client']
    Bucket = caseLocals['Bucket']

    apiInvokedCount = 0
    try:
        response = client.list_objects(Bucket=Bucket)
        if 'ResponseMetadata' in response and (responseMetadata := response['ResponseMetadata']):
            if responseMetadata['HTTPStatusCode'] == 404:
                return response
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
        serviceModel.increaseExtraCaseApisCount(apiInvokedCount)


predefinedFuncDict = {
    'DeleteObjects': DropObjects,
    'DropAllBuckets': DropAllBuckets,
    'DropObjects': DropObjects,
    'DropBucket': DropBucket,
    'SetVars': setVars,
}

defaultClientConfig = {
    'connect_timeout': 5,
    'read_timeout': 10,
    'retries': {
        'max_attempts': 2, 'mode': 'standard'
    }
}


def newAnonymousClient(serviceName):
    configOptions = deepcopy(defaultClientConfig)
    configOptions['signature_version'] = UNSIGNED
    return boto3.client(service_name=serviceName, use_ssl=False, verify=False, config=S3Config(**configOptions))


def newAwsClient(serviceName, clientConfig):
    configOptions = deepcopy(defaultClientConfig)
    return boto3.client(serviceName, **clientConfig, config=S3Config(configOptions))
