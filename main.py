import itertools
import json
import numbers
import os
import re
from collections import OrderedDict
from datetime import datetime

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


def newBucketName():
    global bucketOrdinal
    current_bucket_ordinal = next(bucketOrdinal)
    return '%s-%d' % (bucket_prefix, current_bucket_ordinal)


def prepareBucket(localParams, **kwargs):
    bucketName = newBucketName()
    localParams['Bucket'] = bucketName
    return localParams


predefinedFuncDict = {'PrepareBucket': prepareBucket}


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


def initServicesTestModels(config):
    identities = config['identities']
    clientConfig = config['client_config']

    testsDir = "./tests"
    if 'test_dir' in config:
        testsDir = config['tests_dir']

    if not os.path.exists(testsDir) or not os.path.isdir(testsDir):
        raise RuntimeError('tests dir must be a directory', testsDir)
    serviceModels = {}
    for serviceName in os.listdir(testsDir):
        testFiles = []
        serviceDir = os.path.join(testsDir, serviceName)
        for testFile in os.listdir(serviceDir):
            filePath = os.path.join(serviceDir, testFile)
            if os.path.isfile(filePath) and testFile.endswith(".json"):
                testFiles.append(filePath)
        testModel = ServiceTestModel(serviceName, testFiles, identities, clientConfig)
        serviceModels[serviceName] = testModel
    return serviceModels


def loadTestCaseFile(filePath):
    if not os.path.isfile(filePath):
        return
    with open(filePath, 'rb') as fp:
        payload = fp.read().decode('utf-8')

    logger.debug("Loading : %s" % filePath)
    # noinspection PyTypeChecker
    return json.loads(payload, object_pairs_hook=OrderedDict)


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


def resolvePlaceholderDict(parameters, resolveArg):
    if parameters is None or len(parameters) == 0:
        return
    for k, v in parameters.items():
        if isinstance(v, dict) and dict:
            resolvePlaceholderDict(v, resolveArg)
        elif isinstance(v, list) and len(v):
            resolvePlaceHolderArr(v, resolveArg)
        elif isinstance(v, str):
            parameters[k] = resolvePlaceHolder(v, resolveArg)
        elif isinstance(v, numbers.Number):
            continue
        else:
            logger.error(f'Unsupported parameter: {v}')
            raise RuntimeError()


placeHolderPattern = re.compile(r'\$\{(.*?)}')


def resolvePlaceHolderArr(valueArray, resolveArg):
    if valueArray is None or len(valueArray) == 0:
        return
    for index, value in enumerate(valueArray):
        if isinstance(value, list) and len(value):
            resolvePlaceHolderArr(value, resolveArg)
        if isinstance(value, dict) and len(value):
            resolvePlaceholderDict(value, resolveArg)
        elif isinstance(value, str):
            valueArray[index] = resolvePlaceHolder(value, resolveArg)
        else:
            raise RuntimeError('Unsupported parameter', value)


def resolvePlaceHolder(value, resolveArg):
    for paramName in set(placeHolderPattern.findall(value)):
        paramValue = resolveArg(paramName)
        if paramValue is None:
            raise RuntimeError('placeholder variable not found!', paramName)
        value = value.replace('${%s}' % paramName, paramValue)
    return value


def resolveArgInDicts(*dicts):
    def decorator(paramName):
        for dc in dicts:
            if paramName in dc:
                return dc[paramName]
            continue

    return decorator


class ServiceTestModel:
    def __init__(self, serviceName, testFiles, identities, clientConfig):
        self.serviceName = serviceName
        self.testFiles = testFiles
        self.identities = identities
        self.clientConfig = clientConfig

        self._testCases = None
        self._clientDict = {}

    def setUp(self):
        self._testCases = {}
        for testFile in self.testFiles:
            testCase = loadTestCaseFile(testFile)
            if testCase is not None:
                self._testCases[testFile] = testCase

        self._clientDict = {}
        for identityName, identityConfig in self.identities.items():
            try:
                clientConfig = self.clientConfig.copy()
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
                self._clientDict[identityName] = serviceClient
            except Exception as e:
                logger.error(f"Failed to create client for {identityName}", e)
                raise e

    def tearDown(self):
        for k, v in self._clientDict.items():
            logger.debug(f"Closing client: {k}")
            try:
                v.close()
            except:
                pass

    def run(self):
        for fileName, fileCase in self._testCases.items():
            logger.debug(f"Running testCase: {fileName}")
            for suiteName, suiteCase in fileCase.items():
                localParams = {}
                self.doRun("%s::%s::%s" % (self.serviceName, os.path.basename(fileName), suiteName), suiteCase,
                           localParams)

    def doRun(self, suiteName, suiteCase, localParams):
        for case in suiteCase:
            operationName = case['operation']
            logger.info(f"Executing {suiteName}::{operationName}")
            resolveFunc = None
            clientName = None
            # response = None

            if operationName in predefinedFuncDict.keys():
                caseLocalParams = localParams.copy()
                parameters = {}
                if 'parameters' in case:
                    parameters = case['parameters']
                if 'clientName' in case and (clientName := case['clientName']) in self._clientDict:
                    serviceClient = self._clientDict[clientName]
                    caseLocalParams['client'] = serviceClient
                    resolveFunc = resolveArgInDicts(serviceClient.identityConfig, localParams)
                    resolvePlaceholderDict(parameters, resolveFunc)
                else:
                    resolveFunc = resolveArgInDicts(localParams)
                    resolvePlaceholderDict(parameters, resolveFunc)
                caseLocalParams.update(parameters)
                response = predefinedFuncDict[operationName](localParams, **caseLocalParams)
            elif 'clientName' in case and (clientName := case['clientName']) in self._clientDict \
                    and operationName in (serviceClient := self._clientDict[clientName]).supportOperations:
                parameters = {}
                if 'parameters' in case:
                    parameters = case['parameters']
                    resolveFunc = resolveArgInDicts(serviceClient.identityConfig, localParams)
                    resolvePlaceholderDict(parameters, resolveFunc)
                try:
                    # noinspection PyProtectedMember
                    response = BaseClient._make_api_call(serviceClient, operationName, parameters)
                except ClientError as e:
                    response = e.response
            else:
                logger.error(f'Invalid operation: {operationName}')
                raise RuntimeError()

            localParams['response'] = response
            logger.debug(f"[{clientName}] Execute {operationName} response: {response}")
            if 'assertion' in case:
                assertion = case['assertion']
                resolvePlaceholderDict(assertion, resolveFunc)
                validateAssertions('response', assertion, response)


class DTEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


if __name__ == '__main__':
    config = loadConfig()
    sms = initServicesTestModels(config)
    for serviceName, serviceModel in sms.items():
        logger.info(f'Run ServiceModel: {serviceName}')
        serviceModel.setUp()
        serviceModel.run()
        serviceModel.tearDown()
