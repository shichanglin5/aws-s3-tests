import copy
import itertools
import json
import os
import re
import threading
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor

import yaml
from botocore.client import (
    BaseClient
)
from botocore.exceptions import ClientError
from loguru import logger

from core import const
from core.assertion import validateAssertions
from core.loader import loadFileData, loadXmindData
from core.place_holder import resolvePlaceholderDict, resolvePlaceHolder
from core.predefind import predefinedFuncDict, newAnonymousClient, newAwsClient
from core.utils import IgnoreNotSerializable

GLOBAL_VARIABLES = {
    'urlEncode': urllib.parse.quote,
    'uuidStr': lambda: uuid.uuid1().hex,
    "bucketOrdinal": itertools.count(1)
}

_ = urllib.parse


class ServiceTestModel:
    def __init__(self, serviceName, suiteFiles, identities, clientConfig, includePatterns, excludePatterns, hideEnabled, xmindSuites, concurrency=5, customHeaders=None, autoClean=False):
        self.serviceName = serviceName
        self.suiteFiles = suiteFiles
        self.identities = identities
        self.clientConfig = clientConfig
        self.suiteIncludePatterns = includePatterns
        self.suiteExcludePatterns = excludePatterns
        self.hideEnabled = hideEnabled
        self.xmindSuites = xmindSuites

        self.clientDict = {}
        self.suiteModels = {}
        self.hooks = []
        self.idGenerator = itertools.count(1)

        self.suite_pass = []
        self.suite_failed = []
        self.suite_skipped = []
        self.extra_case_api_invoked_count = 0
        self.extra_case_api_invoked_count_lock = threading.Lock()
        self.threadPool = ThreadPoolExecutor(max_workers=concurrency)
        self.customHeaders = customHeaders
        self.autoClean = autoClean

    def increaseExtraCaseApisCount(self, increment):
        with self.extra_case_api_invoked_count_lock:
            self.extra_case_api_invoked_count += increment

    def setUp(self):
        if self.xmindSuites is not None:
            self.suiteModels[const.XMIND_SUITES] = parseSuite([[]], self.xmindSuites)
        if self.suiteFiles is not None:
            for suiteFile in self.suiteFiles:
                suiteData = loadFileData(suiteFile, yaml.safe_load)
                # don't present filename in result tree
                # self.suiteModels[suiteFile] = parseSuite([[{const.CASE_NAME:suiteFile}]], suiteData)
                self.suiteModels[suiteFile] = parseSuite([[]], suiteData)
        self.clientDict = {}
        for identityName, identityConfig in self.identities.items():
            for prop in identityConfig:
                GLOBAL_VARIABLES[f'{identityName}_{prop}'] = identityConfig[prop]
            try:
                clientConfig = copy.deepcopy(self.clientConfig)
                for prop in const.CLIENT_PROPERTIES:
                    if prop in identityConfig:
                        clientConfig[prop] = identityConfig[prop]
                if identityName == const.ANONYMOUS:
                    serviceClient = newAnonymousClient(self.serviceName, clientConfig)
                else:
                    serviceClient = newAwsClient(self.serviceName, clientConfig)
                serviceClient.supportOperations = serviceClient.meta.service_model.operation_names

                identityConfig['identity_name'] = identityName
                identityConfig.update(clientConfig)
                serviceClient.identityConfig = identityConfig
                if self.customHeaders is not None:
                    def addHeaders(request, **kwargs):
                        for k, v in self.customHeaders.items():
                            request.headers[k] = v

                    serviceClient.meta.events.register('request-created.s3.*', addHeaders)
                self.clientDict[identityName] = serviceClient
            except Exception as e:
                logger.error(f"Failed to create client for {identityName}", e)
                raise e

    def tearDown(self):
        for hook in self.hooks:
            hook()
        for k, v in self.clientDict.items():
            logger.debug(f"Closing client: {k}")
            try:
                v.close()
            except:
                pass

    def submitTask(self, target, *args):
        future = self.threadPool.submit(target, *args)
        self.hooks.append(future.result)

    def run(self):
        try:
            filteredSuites = self.filterSuites()
        except Exception as e:
            logger.exception(e)
            return
        for suiteFile, suiteModel in filteredSuites.items():
            for suite in suiteModel:
                if suite:
                    suiteId = suite[0][const.SUITE_ID]
                    self.submitTask(self.doRun, f'{suiteId}', suite, GLOBAL_VARIABLES.copy())

    def filterSuites(self):
        filteredSuites = self.suiteModels
        if self.suiteIncludePatterns or self.suiteExcludePatterns:
            filteredSuites = {}
            for suiteModelName, suiteModel in self.suiteModels.items():
                filteredSuites[suiteModelName] = []
                suiteModelCounter = itertools.count(1)
                for suite in suiteModel:
                    # 1、生成 suiteId，格式为 __服务名__@suiteModelName@__序号__
                    suiteId = '__%s__@%s@__%d__' % (self.serviceName, suiteModelName, next(suiteModelCounter))
                    if suite:
                        suite[0][const.SUITE_ID] = suiteId
                    pathList = getSuitePath(suite)
                    # 2、将 suiteID 添加到 pathList 中，这样后续可以直接复制 suiteID 进行过滤
                    pathList.append(suiteId)
                    includePatternMatch = True
                    if self.suiteIncludePatterns:
                        includePatternMatch = False
                        for suitePath in list(pathList):
                            for includePattern in self.suiteIncludePatterns:
                                if not includePatternMatch and includePattern.match(suitePath):
                                    includePatternMatch = True
                                    break
                    excludePatternMatch = False
                    if self.suiteExcludePatterns:
                        for suitePath in list(pathList):
                            for excludePattern in self.suiteExcludePatterns:
                                if not excludePatternMatch and excludePattern.match(suitePath):
                                    excludePatternMatch = True
                                    break
                    if includePatternMatch and not excludePatternMatch:
                        filteredSuites[suiteModelName].append(suite)
                    else:
                        self.suite_skipped.append(suite)
        return filteredSuites

    def getTitle(self, case):
        if const.CASE_TITLE in case:
            title = case[const.CASE_TITLE]
            # del case[const.CASE_TITLE]
        else:
            title = case[const.CASE_OPERATION]
        return title

    def doRun(self, suiteId, suite, suiteLocals):
        autoClean = self.autoClean
        suiteExecPath = ''
        try:
            for case in suite:
                suiteExecPath, terminate = self.runCase(case, suiteExecPath, suiteLocals, suite, suiteId)
                if terminate:
                    return
            self.suite_pass.append(suite)
        except Exception as e:
            logger.exception(e)
            # autoClean = False
        finally:
            if autoClean:
                self.runCase({
                    "operation": "DropBucket",
                    "clientName": "admin",
                    "parameters": {
                        "Bucket": "${Bucket}"
                    },
                    const.HIDE: False
                }, "AutoClean", suiteLocals, None, suiteId)

    def runCase(self, case, suiteExecPath, suiteLocals, suite, suiteId):
        terminate = False
        caseName = case[const.CASE_TITLE] if const.CASE_TITLE in case else case[const.CASE_OPERATION]
        currentSuiteExecPath = f'{suiteExecPath}::{caseName}' if suiteExecPath else caseName
        ignore = self.hideEnabled and const.HIDE in case and case[const.HIDE]
        parameters = {}

        caseLocals = suiteLocals.copy()
        caseLocals[const.RESET_HOOKS] = []
        clientName, caseResponse = None, None
        try:
            if const.CASE_OPERATION not in case:
                if not ignore:
                    suiteExecPath = currentSuiteExecPath
                return suiteExecPath
            # client
            operationName = case[const.CASE_OPERATION]
            if const.CASE_CLIENT_NAME in case:
                clientName = case[const.CASE_CLIENT_NAME]
                if clientName in self.clientDict:
                    serviceClient = self.clientDict[clientName]
                    caseLocals['Client'] = serviceClient
                    caseLocals.update(serviceClient.identityConfig)

            # parameters
            if const.CASE_PARAMETERS in case:
                parameters = case[const.CASE_PARAMETERS]
                resolvePlaceholderDict(parameters, caseLocals)
                caseLocals.update(parameters)

            # execute
            if operationName in predefinedFuncDict.keys():
                caseResponse = predefinedFuncDict[operationName](serviceModel=self, suiteLocals=suiteLocals, caseLocals=caseLocals, parameters=parameters)
            elif operationName in serviceClient.supportOperations:
                try:
                    # noinspection PyProtectedMember
                    caseResponse = BaseClient._make_api_call(serviceClient, operationName, parameters)
                except ClientError as e:
                    caseResponse = e.response
            else:
                raise RuntimeError(f'operation[{operationName}] undefined')

            # update title
            case[const.CASE_RESPONSE] = caseResponse
            # if const.CASE_TITLE not in case and const.CASE_ASSERTION in case and 'ResponseMetadata' in caseResponse and 'HTTPStatusCode' in (responseMetadata := caseResponse['ResponseMetadata']):
            #     case[const.CASE_TITLE] = '%s-%s' % (operationName, responseMetadata['HTTPStatusCode'])

            # assertion
            if const.CASE_ASSERTION in case:
                assertion = case[const.CASE_ASSERTION]
                resolvePlaceholderDict(assertion, caseLocals)
                validateAssertions('caseResponse', assertion, caseResponse)

            # suite locals (resolve properties and put it into suiteLocals)
            if const.SUITE_LOCALS in case and (caseSuiteLocals := case[const.SUITE_LOCALS]) and isinstance(caseSuiteLocals, dict):
                caseLocals.update(caseResponse)
                for key, value in caseSuiteLocals.items():
                    suiteLocals[key] = resolvePlaceHolder(value, caseLocals)

            if not ignore:
                suiteExecPath = currentSuiteExecPath

            # log response
            if not ignore:
                logger.debug(f"{currentSuiteExecPath} ==> req[{operationName}]:{json.dumps(parameters, default=IgnoreNotSerializable)}, resp: {caseResponse}")

            case[const.CASE_SUCCESS] = True
        except Exception as e:
            terminate = True
            if suite is not None:
                self.suite_failed.append(suite)

            case[const.CASE_SUCCESS] = False
            case[const.ERROR_INFO] = f'{e.__class__.__name__}({json.dumps(e.args, default=IgnoreNotSerializable)})'
            if caseResponse:
                case[const.CASE_RESPONSE] = caseResponse
                if const.CASE_ASSERTION in case and 'ResponseMetadata' in caseResponse and 'HTTPStatusCode' in (responseMetadata := caseResponse['ResponseMetadata']):
                    case[const.CASE_TITLE] = '%s-%s' % (caseName, responseMetadata['HTTPStatusCode'])
            logger.error(f"{suiteId}->{currentSuiteExecPath}\n"
                         f"### req[{operationName}] ### {json.dumps(parameters, default=IgnoreNotSerializable)}\n"
                         f"### resp ### {json.dumps(caseResponse, default=IgnoreNotSerializable)}\n")

            if isinstance(e, AssertionError):
                logger.error('{}->{}', suiteId, e)
            else:
                logger.exception('{}->{}', suiteId, e)
        finally:
            resetHooks = caseLocals[const.RESET_HOOKS]
            for hook in resetHooks:
                try:
                    hook()
                except Exception as e:
                    logger.exception("Exception while resetting hooks: {}", e)
            return suiteExecPath, terminate


def initServicesTestModels(config, includePatterns, excludePatterns):
    identities = config['identities']
    clientConfig = config['client_config']
    concurrency = 5
    if 'concurrency' in config:
        c = config['concurrency']
        if c > 0:
            concurrency = c
    customHeaders = None
    if 'custom_headers' in config:
        customHeaders = config['custom_headers']

    suitesDir = "suites"
    if 'tests_dir' in config:
        suitesDir = config['tests_dir']

    if 'global_variables' in config:
        global GLOBAL_VARIABLES
        GLOBAL_VARIABLES.update(config['global_variables'])

    autoClean = False
    if 'auto_clean' in config and config['auto_clean']:
        autoClean = True

    if not os.path.exists(suitesDir) or not os.path.isdir(suitesDir):
        raise RuntimeError('tests dir must be a directory', suitesDir)

    if const.SUITE_FILTERS in config and (suiteFilters := config[const.SUITE_FILTERS]):
        if const.INCLUDES in suiteFilters and (includes := suiteFilters[const.INCLUDES]):
            includePatterns.extend([re.compile(includeStr) for s in includes if (includeStr := s.strip())])
        if const.EXCLUDES in suiteFilters and (excludes := suiteFilters[const.EXCLUDES]):
            excludePatterns.extend([re.compile(excludeStr) for s in excludes if (excludeStr := s.strip())])

    hideEnabled = True
    if const.HIDE_ENABLED in config:
        hideEnabled = config[const.HIDE_ENABLED]

    serviceModels = {}
    # load xmind cases
    # 加载 suites 目录下的所有 xmind 文件
    xmindSuites = {}
    if const.LOAD_XMIND_SUITES in config and config[const.LOAD_XMIND_SUITES] and \
            (xmindFiles := [xmindFile for file in os.listdir(suitesDir) if (xmindFile := os.path.join(suitesDir, file)) and os.path.isfile(xmindFile) and file.endswith('.xmind')]) and xmindFiles:
        for xmindFile in xmindFiles:
            servicesSuites = loadXmindData(xmindFile)
            for serviceName, serviceSuite in servicesSuites.items():
                if serviceName not in xmindSuites:
                    xmindSuites[serviceName] = []
                xmindSuites[serviceName].extend(serviceSuite)
    # export to yaml
    # 导出到 .wd/tests/suites/{serviceName}/integration_tests.yaml
    if const.EXPORT_SUITES in config and config[const.EXPORT_SUITES]:
        exportYaml(suitesDir, xmindSuites)
    
    # load yaml files
    # 加载 suites 字目录，子目录为服务名，按服务名加载 yaml 文件
    # eg: suites/s3/xxx.xmind
    serviceYamlFiles = {}
    if const.LOAD_YAML_SUITES in config and config[const.LOAD_YAML_SUITES]:
        for serviceName in os.listdir(suitesDir):
            if not os.path.isdir(os.path.join(suitesDir, serviceName)):
                continue
            suiteFiles = []
            serviceDir = os.path.join(suitesDir, serviceName)
            for testFile in os.listdir(serviceDir):
                filePath = os.path.join(serviceDir, testFile)
                if os.path.isfile(filePath) and testFile.endswith(".yaml"):
                    suiteFiles.append(filePath)
            if serviceName not in const.AWS_SERVICES:
                logger.debug("not a aws service: {}, ignored", serviceName)
                continue
            serviceYamlFiles[serviceName] = suiteFiles

    for serviceName in itertools.chain(xmindSuites.keys(), serviceYamlFiles.keys()):
        serviceModels[serviceName] = ServiceTestModel(serviceName,
                                                      serviceYamlFiles[serviceName] if serviceName in serviceYamlFiles else None,
                                                      identities, clientConfig, includePatterns, excludePatterns, hideEnabled,
                                                      xmindSuites[serviceName] if serviceName in xmindSuites else None, concurrency, customHeaders, autoClean)
    return serviceModels


def exportYaml(suitesDir, result):
    for serviceName, data in result.items():
        exportYamlPath = f'{suitesDir}/exports/{serviceName}'
        logger.info('exportYaml to: {}', exportYamlPath)
        if not os.path.exists(exportYamlPath):
            os.makedirs(exportYamlPath, exist_ok=True)
        with open(f'{exportYamlPath}/integration_tests.yaml', 'w') as fp:
            yaml.dump(data, fp)


def parseSuite(parentSuites: [], suites: (dict, list) = None, hideSub=False):
    if suites is None:
        return parentSuites
    if not isinstance(suites, (list, dict)):
        raise TypeError('suites: (dict, list)')

    # init
    if hideSub is None:
        hideSub = False
    if parentSuites is None:
        parentSuites = []
    suiteList = []
    if isinstance(suites, dict):
        for suiteWrapperName, suiteWrapper in suites.items():
            wrappedSuites = {suiteWrapperName: suiteWrapper}
            hide = hideSub
            if suiteWrapperName == const.NOT_HIDE:
                hide = False
                wrappedSuites = suiteWrapper
            elif suiteWrapperName == const.HIDE:
                hide = True
                wrappedSuites = suiteWrapper
            for suiteName, suite in wrappedSuites.items():
                forkNode = {const.CASE_TITLE: suiteName}
                newSuite = [forkNode]
                if hide:
                    forkNode[const.HIDE] = True
                    for suiteCase in suite:
                        suiteCase[const.HIDE] = True
                        newSuite.append(suiteCase)
                else:
                    newSuite.extend(suite)
                suiteList.append(newSuite)
    else:
        suiteList = suites
        if hideSub:
            for suiteCases in suiteList:
                for suiteCase in suiteCases:
                    if const.HIDE not in suiteCase:
                        suiteCase[const.HIDE] = hideSub

    # fork
    resultSuites = []
    caseOrdinal = itertools.count(0)
    for suite in suiteList:
        suiteOrdinal = next(caseOrdinal)
        midSuites = copy.deepcopy(parentSuites) if parentSuites else [[]]
        for suiteCase in suite:
            suiteCase[const.ORDER] = suiteOrdinal
            subSuites = None
            if const.CASE_SUITES in suiteCase:
                subSuites = suiteCase[const.CASE_SUITES]
                del suiteCase[const.CASE_SUITES]
            if const.CASE_TITLE in suiteCase or const.CASE_OPERATION in suiteCase:
                for midSuite in midSuites:
                    suiteCaseCopy = copy.deepcopy(suiteCase)
                    midSuite.append(suiteCaseCopy)
            if subSuites is not None:
                caseHideSub = const.HIDE in suiteCase and suiteCase[const.HIDE]
                midSuites = parseSuite(midSuites, subSuites, caseHideSub)
        resultSuites.extend(midSuites)
    return resultSuites

# 生成测试用例的路径，可以用来标识该用例
# suite 表示一个完整的 case 集合，每个 case 是一个api调用
# fullPath => ::Ownership-BucketOwnerEnforced::DropBucket::ACL-None::CreateBucket::OwnershipControls::Admin::GetBucketOwnershipControls::PutBucketOwnershipControls-ObjectWriter::GetBucketOwnershipControls
# midPath => ::Ownership-BucketOwnerEnforced::ACL-None::CreateBucket::OwnershipControls::Admin::GetBucketOwnershipControls::PutBucketOwnershipControls-ObjectWriter::GetBucketOwnershipControls
def getSuitePath(suite):
    fullPath, midPath = '', ''
    for case in suite:
        caseName = case[const.CASE_TITLE] if const.CASE_TITLE in case else case[const.CASE_OPERATION] if const.CASE_OPERATION in case else None
        fullPath = '%s::%s' % (fullPath, caseName) if fullPath else caseName
        # 如果节点配置为隐藏（__hide__属性为true）则从 midPath 中排除
        if not (const.HIDE in case and case[const.HIDE]):
            midPath = '%s::%s' % (midPath, caseName) if midPath else caseName
    return [fullPath, midPath]


def reportResult(serviceModels):
    summary = {}
    for serviceName, serviceModel in serviceModels.items():
        # suiteFileTotal = len(serviceModel.suiteModels)

        suiteTotal = sum([len(l) for l in serviceModel.suiteModels.values()])
        suitePassCount = len(serviceModel.suite_pass)
        suiteFailedCount = len(serviceModel.suite_failed)
        suiteSkippedCount = len(serviceModel.suite_skipped)

        caseTotal, casePassCount, caseFailedCount, caseSkippedCount, apiInvokedCount = 0, 0, 0, 0, 0
        for suites in serviceModel.suiteModels.values():
            for suite in suites:
                for case in suite:
                    if const.CASE_SUCCESS in case:
                        if case[const.CASE_SUCCESS]:
                            if const.HIDE in case and case[const.HIDE]:
                                continue
                            casePassCount += 1
                        else:
                            caseFailedCount += 1
                        if const.CASE_CLIENT_NAME in case:
                            apiInvokedCount += 1
                    else:
                        caseSkippedCount += 1
                    caseTotal += 1
        apiInvokedCount += serviceModel.extra_case_api_invoked_count
        summary[serviceName] = {
            'suiteTotal': suiteTotal,
            'suitePassCount': suitePassCount,
            'suiteFailedCount': suiteFailedCount,
            'suiteSkippedCount': suiteSkippedCount,
            'caseTotal': caseTotal,
            'casePassCount': casePassCount,
            'caseFailedCount': caseFailedCount,
            'caseSkippedCount': caseSkippedCount,
            'apiInvokedCount': apiInvokedCount
        }

        message = f"{str(serviceName).upper()}: " \
                  f"Suite [TOTAL: {suiteTotal}, " \
                  f"PASS: {suitePassCount}, " \
                  f"FAILED: {suiteFailedCount}, " \
                  f"SKIPPED: {suiteSkippedCount}], " \
                  f"SuiteCase [TOTAL: {caseTotal}, " \
                  f"PASS: {casePassCount}, " \
                  f"FAILED: {caseFailedCount}, " \
                  f"SKIPPED: {caseSkippedCount} " \
                  f"API_INVOKED: {apiInvokedCount}]"

        if suiteFailedCount:
            logger.debug("failed suites ids: {}", [suite[0][const.SUITE_ID] for suite in serviceModel.suite_failed if suite])
            logger.error(message)
        else:
            logger.info(message)
        return summary
