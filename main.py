import copy
import itertools
import numbers
import os
import re
import uuid
from threading import Thread, Lock
from time import time

import boto3
import yaml
from botocore import UNSIGNED
from botocore.client import (
    Config as S3Config, BaseClient
)
from botocore.exceptions import ClientError
from loguru import logger

bucket_prefix = '1-aws-s3-tests-bucket'
bucketOrdinal = itertools.count(1)
bucketOrdinalLock = Lock()
globalVariables = {'uuid': uuid}
globalStore = {}


def newBucketName():
    global bucketOrdinal
    # bucketOrdinalLock.acquire()
    current_bucket_ordinal = next(bucketOrdinal)
    # bucketOrdinalLock.acquire()
    return '%s-%d' % (bucket_prefix, current_bucket_ordinal)


def generateBucketName(localParams, **kwargs):
    bucketName = newBucketName()
    result = {'Bucket': bucketName}
    localParams.update(result)
    return result


def generateObjectKey(localParams, **kwargs):
    if 'Prefix' in kwargs:
        Prefix = kwargs['Prefix']
    else:
        Prefix = ''

    ObjectKey = Prefix + uuid.uuid1().hex
    result = {'Key': ObjectKey}
    localParams.update(result)
    return result


def DeleteObjects(localParams, **kwargs):
    client = kwargs['Client']
    Bucket = kwargs['Bucket']
    try:
        response = client.list_objects(Bucket=Bucket)
        while True:
            finalResponse = response
            if 'Contents' in response and (objects := response['Contents']):
                objectIdentifierList = [{'Key': obj['Key'] for obj in objects}]
                finalResponse = client.delete_objects(Bucket=Bucket, Delete={
                    'Objects': objectIdentifierList
                })
            if 'IsTruncated' in response and response['IsTruncated'] and 'NextMarker' in response and (
                    NextMarker := response['NextMarker']):
                response = client.list_objects(Bucket=Bucket, Marker=NextMarker)
            else:
                return finalResponse
    except ClientError as e:
        return e.response


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


def loadConfig():
    filePath = os.getenv("aws_config", "./config.yaml")
    logger.info(f'logger config from {filePath}')
    with open(filePath, 'r') as fp:
        config = yaml.safe_load(fp)
        if config is None or not isinstance(config, dict) or not config:
            raise RuntimeError('config not valid')

        # validate config
        for requiredKey in ["client_config", "identities"]:
            if requiredKey not in config:
                raise RuntimeError('missing required key', requiredKey)
        return config
    raise RuntimeError('failed to load config')


def configLogger():
    pass
    # logger.


def initServicesTestModels(config):
    identities = config['identities']
    clientConfig = config['client_config']

    testsDir = "./tests"
    if 'test_dir' in config:
        testsDir = config['tests_dir']
    if 'global_variables' in config:
        global globalVariables
        globalVariables.update(config['global_variables'])

    if not os.path.exists(testsDir) or not os.path.isdir(testsDir):
        raise RuntimeError('tests dir must be a directory', testsDir)
    serviceModels = {}
    for serviceName in os.listdir(testsDir):
        if not os.path.isdir(os.path.join(testsDir, serviceName)):
            continue
        suiteFiles = []
        serviceDir = os.path.join(testsDir, serviceName)
        for testFile in os.listdir(serviceDir):
            filePath = os.path.join(serviceDir, testFile)
            if os.path.isfile(filePath) and testFile.endswith(".yaml"):
                suiteFiles.append(filePath)
        testModel = ServiceTestModel(serviceName, suiteFiles, identities, clientConfig)
        serviceModels[serviceName] = testModel
    return serviceModels


def loadFileData(filePath, loader):
    if not os.path.isfile(filePath):
        return
    with open(filePath, 'rb') as fp:
        payload = fp.read().decode('utf-8')

    logger.debug("Loading : %s" % filePath)
    # noinspection PyTypeChecker
    return loader(payload)


def validateAssertions(path: str, assertions: dict, response: dict):
    for key, value in assertions.items():
        result = parseResponseByDot(path, key, response)
        if isinstance(value, dict) and value:
            validateAssertions(f'{path}.{key}', value, result)
        elif isinstance(value, list) and value:
            validateAssertionArr(path, value, result)
        else:
            validateAssertionValue(path, key, value, result)


def validateAssertionArr(path, expect, result):
    if result is None or not isinstance(result, list) or len(result) != len(expect):
        msg = f'Assertion Error: at {path}, array length not equal'
        raise AssertionError(msg)
    for i, e in enumerate(expect):
        e1 = result[i]
        if isinstance(e, dict):
            validateAssertions(path, e, e1)
        elif isinstance(e, list):
            validateAssertionArr(path, e, e1)
        else:
            validateAssertionValue(path, f'[{i}]', e, e1)


def validateAssertionValue(path, key, expect, result):
    if result != expect:
        msg = f'Assertion Error: at {path}.{key}, expect: {expect}, actual: {result}'
        raise AssertionError(msg)


def parseResponseByDot(path, key, response):
    keyArr = key.split('.')
    result = response
    for k in keyArr:
        if k is None or k not in result:
            raise AssertionError(f'Assertion Error: {path}.{key} not exists!')
        else:
            result = result[k]
    return result


def resolvePlaceholderDict(parameters, context):
    if parameters is None or len(parameters) == 0:
        return
    for k, v in parameters.items():
        if isinstance(v, dict) and dict:
            resolvePlaceholderDict(v, context)
        elif isinstance(v, list) and len(v):
            resolvePlaceHolderArr(v, context)
        elif isinstance(v, str):
            parameters[k] = resolvePlaceHolder(v, context)
        elif isinstance(v, numbers.Number):
            continue
        else:
            raise RuntimeError(f'Unsupported parameter: {v}')


placeHolderHandlers = {
    re.compile('(\$\{(.*?)})'): lambda arg, context: context[arg] if arg in context else None,
    re.compile('(@\{(.*?)})'): lambda arg, context: eval(arg, context)
}


def resolvePlaceHolderArr(valueArray, context):
    if valueArray is None or len(valueArray) == 0:
        return
    for index, value in enumerate(valueArray):
        if isinstance(value, list) and len(value):
            resolvePlaceHolderArr(value, context)
        if isinstance(value, dict) and len(value):
            resolvePlaceholderDict(value, context)
        elif isinstance(value, str):
            valueArray[index] = resolvePlaceHolder(value, context)
        else:
            raise RuntimeError('Unsupported parameter', value)


def resolvePlaceHolder(value, context):
    for pattern, handler in placeHolderHandlers.items():
        for placeHolder, paramName in set(pattern.findall(value)):
            paramValue = handler(paramName, context)
            if placeHolder == value:
                if paramValue is None:
                    return None
                return paramValue
            else:
                if paramValue is None:
                    paramValue = ''
                value = value.replace(placeHolder, str(paramValue))
    return value


def newContext(*dicts):
    requestContext = globalVariables.copy()
    for dc in dicts:
        requestContext.update(dc)
    return requestContext


def parseSuite(parentSuites: [], parentSuiteName, suites: dict):
    if suites is None:
        return parentSuites
    if not isinstance(suites, dict):
        raise ValueError('suite_nodes must be a dict')
    result = []
    for suiteName, suite in suites.items():
        midSuites = copy.deepcopy(parentSuites) if parentSuites else [[]]
        for suiteCase in suite:
            suiteCaseCopy = copy.deepcopy(suiteCase)
            suitePath = f'{parentSuiteName}::{suiteName}'
            suiteCaseCopy['path'] = suitePath
            midPath: str
            if 'operation' in suiteCase:
                suiteCaseOperation = suiteCase['operation']
                midPath = f'{suitePath}::{suiteCaseOperation}'
                for midSuite in midSuites:
                    midSuite.append(suiteCaseCopy)
            else:
                midPath = f'{suitePath}'
            if 'suites' in suiteCase:
                subSuites = suiteCase['suites']
                del suiteCase['suites']
                midSuites = parseSuite(midSuites, midPath, subSuites)
        result.extend(midSuites)
    return result


class ServiceTestModel:
    def __init__(self, serviceName, suiteFiles, identities, clientConfig):
        self.serviceName = serviceName
        self.suiteFiles = suiteFiles
        self.identities = identities
        self.clientConfig = clientConfig

        self.clientDict = {}
        self.suiteModels = {}
        self.hooks = []

    def setUp(self):
        for suiteFile in self.suiteFiles:
            suiteData = loadFileData(suiteFile, yaml.safe_load)
            if suiteData is not None:
                self.suiteModels[suiteFile] = parseSuite([], os.path.basename(suiteFile), suiteData)
        self.clientDict = {}
        for identityName, identityConfig in self.identities.items():
            for prop in identityConfig:
                globalVariables[f'{identityName}_{prop}'] = identityConfig[prop]
            try:
                clientConfig = copy.deepcopy(self.clientConfig)
                if identityName == 'anonymous':
                    serviceClient = newAnonymousClient(self.serviceName)
                else:
                    for prop in ['service_name', 'region_name', 'api_version', 'use_ssl', 'verify', 'endpoint_url',
                                 'aws_access_key_id', 'aws_secret_access_key', 'aws_session_token', 'config']:
                        if prop in identityConfig:
                            clientConfig[prop] = identityConfig[prop]
                    serviceClient = newAwsClient(self.serviceName, clientConfig)
                serviceClient.supportOperations = serviceClient.meta.service_model.operation_names

                identityConfig['identity_name'] = identityName
                identityConfig.update(clientConfig)
                serviceClient.identityConfig = identityConfig
                self.clientDict[identityName] = serviceClient
            except Exception as e:
                logger.error(f"Failed to create client for {identityName}", e)
                raise e
        logger.info(f"SetUp completed. "
                    f"SuiteModel Total: {len(self.suiteModels)}, "
                    f"Suite Total: {sum([len(l) for l in self.suiteModels.values()])}, "
                    f"SuiteCase Total: {sum([len(s) for m in self.suiteModels.values() for s in m])}, ")

    def tearDown(self):
        for hook in self.hooks:
            hook()
        for k, v in self.clientDict.items():
            logger.debug(f"Closing client: {k}")
            try:
                v.close()
            except:
                pass

    def run(self):
        for suiteFile, suiteModel in self.suiteModels.items():
            for suite in suiteModel:
                suiteId = uuid.uuid1().hex
                self.submitTask(self.doRun, [f'{self.serviceName}[{suiteId}]', suite, {}])

    def doRun(self, suiteId, suite, localParams):
        for case in suite:
            operationName = case['operation']
            path = case['path']
            caseId = f'{suiteId}::{path}::{operationName}'
            if filterPattern and not re.match(filterPattern, caseId):
                logger.trace(f"Skipping Test ->  {suiteId}::{operationName}")
                continue

            logger.info(f"Executing {caseId}")
            opContext = None
            clientName = None
            # response = None

            if operationName in predefinedFuncDict.keys():
                caseLocalParams = localParams.copy()
                parameters = {}
                if 'parameters' in case:
                    parameters = case['parameters']
                if 'clientName' in case and (clientName := case['clientName']) in self.clientDict:
                    serviceClient = self.clientDict[clientName]
                    caseLocalParams['Client'] = serviceClient
                    opContext = newContext(serviceClient.identityConfig, localParams)
                    resolvePlaceholderDict(parameters, opContext)
                else:
                    opContext = newContext(localParams)
                    resolvePlaceholderDict(parameters, opContext)
                caseLocalParams.update(parameters)
                try:
                    response = predefinedFuncDict[operationName](localParams, **caseLocalParams)
                except Exception as e:
                    logger.exception('{} -> type: {}, msg: {} ', caseId, 'predefinedFuncDict', e)
                    return
            elif 'clientName' in case and (clientName := case['clientName']) in self.clientDict \
                    and operationName in (serviceClient := self.clientDict[clientName]).supportOperations:
                parameters = {}
                if 'parameters' in case:
                    parameters = case['parameters']
                    opContext = newContext(serviceClient.identityConfig, localParams)
                    try:
                        resolvePlaceholderDict(parameters, opContext)
                    except Exception as e:
                        logger.exception('{} -> type: {}, msg: {} ', caseId, 'resolvePlaceholder', e)
                        return
                try:
                    # noinspection PyProtectedMember
                    response = BaseClient._make_api_call(serviceClient, operationName, parameters)
                except ClientError as e:
                    response = e.response
                except Exception as e:
                    logger.exception('{} -> type: {}, msg: {} ', caseId, 'BaseClient', e)
                    return

            else:
                logger.exception('{} -> msg: {} ', caseId, 'operation not exists')

            logger.debug(f"{clientName}_Response_{caseId}: {response}")
            if 'assertion' in case:
                assertion = case['assertion']
                try:
                    resolvePlaceholderDict(assertion, opContext)
                    validateAssertions('response', assertion, response)
                except Exception as e:
                    logger.exception('{} -> type: {}, msg: {} ', caseId, 'validateAssertions', e)
                    return

    def submitTask(self, target, args):
        t = Thread(target=target, args=args)
        t.start()
        self.hooks.append(t.join)


filterPattern = ''


def main(*prefixes):
    start = time()
    global filterPattern
    if prefixes:
        filterPattern = re.compile(prefixes[0])
    config = loadConfig()
    sms = initServicesTestModels(config)

    for serviceName, serviceModel in sms.items():
        logger.info(f'Run ServiceModel: {serviceName}')
        serviceModel.setUp()
        serviceModel.run()
        serviceModel.tearDown()
    end = time()
    logger.info('Tests Completed. Time Spent: %.2fs' % (end - start))


if __name__ == '__main__':
    # main(sys.argv[1:])
    # main('.*TestPutObject.*')
    main()
