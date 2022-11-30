import copy
import itertools
import json
import os
import re
import urllib.parse
import uuid
from threading import Thread

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
    def __init__(self, serviceName, suiteFiles, identities, clientConfig, includePatterns, excludePatterns, hideEnabled, xmindSuites):
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
                if identityName == const.ANONYMOUS:
                    serviceClient = newAnonymousClient(self.serviceName)
                else:
                    for prop in const.CLIENT_PROPERTIES:
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

    def tearDown(self):
        for hook in self.hooks:
            hook()
        for k, v in self.clientDict.items():
            logger.debug(f"Closing client: {k}")
            try:
                v.close()
            except:
                pass

    def submitTask(self, target, args):
        t = Thread(target=target, args=args)
        t.start()
        self.hooks.append(t.join)

    def run(self):
        try:
            filteredSuites = self.filterSuites()
        except Exception as e:
            logger.exception(e)
            return
        for suiteFile, suiteModel in filteredSuites.items():
            for suite in suiteModel:
                suiteId = next(self.idGenerator)
                self.submitTask(self.doRun, [f'_{suiteId}_{self.serviceName}', suite, GLOBAL_VARIABLES.copy()])

    def filterSuites(self):
        filteredSuites = self.suiteModels
        if self.suiteIncludePatterns or self.suiteExcludePatterns:
            filteredSuites = {}
            for suiteFile, suiteModel in self.suiteModels.items():
                filteredSuites[suiteFile] = []
                for suite in suiteModel:
                    fullPath = suiteFile
                    midPath = suiteFile
                    for case in suite:
                        caseName = case[const.CASE_TITLE] if const.CASE_TITLE in case else case[const.CASE_OPERATION] if const.CASE_OPERATION in case else None
                        fullPath = '%s::%s' % (fullPath, caseName)
                        if not (const.HIDE in case and case[const.HIDE]):
                            midPath = '%s::%s' % (midPath, caseName)
                    includePatternMatch = True
                    if self.suiteIncludePatterns:
                        includePatternMatch = False
                        for suitePath in [fullPath, midPath]:
                            for includePattern in self.suiteIncludePatterns:
                                if not includePatternMatch and includePattern.match(suitePath):
                                    includePatternMatch = True
                                    break
                    excludePatternMatch = False
                    if self.suiteExcludePatterns:
                        for suitePath in [fullPath, midPath]:
                            for excludePattern in self.suiteExcludePatterns:
                                if not excludePatternMatch and excludePattern.match(suitePath):
                                    excludePatternMatch = True
                                    break
                    if includePatternMatch and not excludePatternMatch:
                        filteredSuites[suiteFile].append(suite)
                    else:
                        self.suite_skipped.append(suite)
        return filteredSuites

    def doRun(self, suiteId, suite, suiteLocals):
        suiteExecPath = suiteId
        for case in suite:
            caseName = case[const.CASE_TITLE] if const.CASE_TITLE in case else case[const.CASE_OPERATION]
            currentSuiteExecPath = f'{suiteExecPath}::{caseName}'
            ignore = self.hideEnabled and const.HIDE in case and case[const.HIDE]
            parameters = {}

            caseLocals = suiteLocals.copy()
            caseLocals[const.RESET_HOOKS] = []
            clientName, caseResponse = None, None
            try:
                if const.CASE_OPERATION not in case:
                    if not ignore:
                        suiteExecPath = currentSuiteExecPath
                    continue

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
                if const.CASE_TITLE not in case and const.CASE_ASSERTION in case and 'ResponseMetadata' in caseResponse and 'HTTPStatusCode' in (responseMetadata := caseResponse['ResponseMetadata']):
                    case[const.CASE_TITLE] = '%s-%s' % (operationName, responseMetadata['HTTPStatusCode'])

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
                    logger.debug(f"{f'{clientName}@' if clientName else ''}{currentSuiteExecPath} ==> req:{json.dumps(parameters, default=IgnoreNotSerializable)}, resp: {caseResponse}")

                case[const.CASE_SUCCESS] = True
            except Exception as e:
                self.suite_failed.append(suite)
                case[const.CASE_SUCCESS] = False
                case[const.ERROR_INFO] = f'{e.__class__.__name__}({json.dumps(e.args, default=IgnoreNotSerializable)})'
                if caseResponse:
                    case[const.CASE_RESPONSE] = caseResponse

                logger.error(f"{f'{clientName}@' if clientName else ''}{currentSuiteExecPath}\n"
                             f"### req ### {json.dumps(parameters, default=IgnoreNotSerializable)}\n"
                             f"### resp ### {json.dumps(caseResponse, default=IgnoreNotSerializable)}\n")
                if isinstance(e, AssertionError):
                    logger.error(e)
                else:
                    logger.exception(e)
                return
            finally:
                resetHooks = caseLocals[const.RESET_HOOKS]
                for hook in resetHooks:
                    try:
                        hook()
                    except Exception as e:
                        logger.exception("Exception while resetting hooks: {}", e)

        # suite pass
        self.suite_pass.append(suite)


def initServicesTestModels(config, includePatterns, excludePatterns):
    identities = config['identities']
    clientConfig = config['client_config']

    testsDir = "./tests"
    if 'tests_dir' in config:
        testsDir = config['tests_dir']
    if 'global_variables' in config:
        global GLOBAL_VARIABLES
        GLOBAL_VARIABLES.update(config['global_variables'])

    if not os.path.exists(testsDir) or not os.path.isdir(testsDir):
        raise RuntimeError('tests dir must be a directory', testsDir)

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
    xmindSuites = {}
    if const.LOAD_XMIND_SUITES in config and config[const.LOAD_XMIND_SUITES] and \
            (xmindFiles := [xmindFile for file in os.listdir(testsDir) if (xmindFile := os.path.join(testsDir, file)) and os.path.isfile(xmindFile) and file.endswith('.xmind')]) and xmindFiles:
        for xmindFile in xmindFiles:
            servicesSuites = loadXmindData(xmindFile)
            for serviceName, serviceSuite in servicesSuites.items():
                if serviceName not in xmindSuites:
                    xmindSuites[serviceName] = []
                xmindSuites[serviceName].extend(serviceSuite)
    # load yaml files
    serviceYamlFiles = {}
    if const.LOAD_YAML_SUITES in config and config[const.LOAD_YAML_SUITES]:
        for serviceName in os.listdir(testsDir):
            if not os.path.isdir(os.path.join(testsDir, serviceName)):
                continue
            suiteFiles = []
            serviceDir = os.path.join(testsDir, serviceName)
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
                                                      xmindSuites[serviceName] if serviceName in xmindSuites else None)
    return serviceModels


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
            logger.error(message)
        else:
            logger.info(message)
        return summary
