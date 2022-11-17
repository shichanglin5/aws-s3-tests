import copy
import itertools
import numbers
import os
import re
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
globalVariables = {}
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


predefinedFuncDict = {'GenerateBucketName': generateBucketName}


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
    if 'global_variables' in config:
        global globalVariables
        globalVariables = config['global_variables']

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
        if paramName in globalVariables:
            return globalVariables[paramName]

    return decorator


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

        self._suiteModels = []
        self._clientDict = {}
        self.suiteModels = {}
        self.hooks = []

    def setUp(self):
        for suiteFile in self.suiteFiles:
            suiteData = loadFileData(suiteFile, yaml.safe_load)
            if suiteData is not None:
                self.suiteModels[suiteFile] = parseSuite([], os.path.basename(suiteFile), suiteData)
        self._clientDict = {}
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
                self._clientDict[identityName] = serviceClient
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
        for k, v in self._clientDict.items():
            logger.debug(f"Closing client: {k}")
            try:
                v.close()
            except:
                pass

    def run(self):
        for suiteFile, suiteModel in self.suiteModels.items():
            for suite in suiteModel:
                self.submitTask(self.doRun, [self.serviceName, suite, {}])

    def doRun(self, parentPath, suite, localParams):
        for case in suite:
            operationName = case['operation']
            path = case['path']
            caseId = f'{parentPath}::{path}::{operationName}'
            if filterPattern and not re.match(filterPattern, caseId):
                logger.info(f"Skipping Test ->  {parentPath}::{operationName}")
                continue

            logger.info(f"Executing {caseId}")
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

    def submitTask(self, target, args):
        t = Thread(target=target, args=args)
        t.start()
        self.hooks.append(t.join)


filterPattern = None


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
    # main('.PrepareOps.*')
    main()
