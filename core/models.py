import copy
import itertools
import json
import os
import re
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
from core.loader import loadFileData
from core.place_holder import resolvePlaceholderDict
from core.predefind import predefinedFuncDict, newAnonymousClient, newAwsClient

GLOBAL_VARIABLES = {'uuid': uuid}


# class SuiteCase(dict):
#     def __init__(self, data):
#         super().__init__(data)
#         # self.name = name
#         # self.order = order
#         # self.operation = operation
#         # self.clientName = clientName
#         # self.parameters = parameters
#         # self.assertion = assertion
#         # self.suites = suites
#
#         self.order = None
#         self.caseSuccess = None
#         self.errorInfo = None
#         self.hide = False


class ServiceTestModel:
    def __init__(self, serviceName, suiteFiles, identities, clientConfig, filterPattern):
        self.serviceName = serviceName
        self.suiteFiles = suiteFiles
        self.identities = identities
        self.clientConfig = clientConfig
        self.filterPattern = filterPattern

        self.clientDict = {}
        self.suiteModels = {}
        self.hooks = []
        self.idGenerator = itertools.count(1)

        self.suite_pass = []
        self.suite_failed = []
        self.suite_skipped = []
        self.extra_case_api_invoked_count = 0

    def setUp(self):
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

    def run(self):
        for suiteFile, suiteModel in self.suiteModels.items():
            for suite in suiteModel:
                suiteId = next(self.idGenerator)
                self.submitTask(self.doRun, [f'_{self.serviceName}_{suiteId}_', suite, {}])

    def doRun(self, suiteId, suite, suiteLocals):
        suiteState = const.STATE_INIT
        suiteExecPath = suiteId

        for case in suite:
            caseName = case[const.CASE_OPERATION] if const.CASE_OPERATION in case else case[
                const.CASE_NAME] if const.CASE_NAME else 'error[case name undefined]]'
            currentSuiteExecPath = f'{suiteExecPath}::{caseName}'
            ignore = const.HIDE in case and case[const.HIDE]

            parameters = {}
            suiteLocals[const.RESET_HOOKS] = []
            try:
                if self.filterPattern and not re.match(self.filterPattern, currentSuiteExecPath):
                    # nonlocal suiteState
                    if not suiteState & const.STATE_STARTED:
                        suiteState = suiteState | const.STATE_SKIPPED
                        self.suite_skipped.append(suite)
                    logger.debug(f"Skipping Test ->  {currentSuiteExecPath}")
                    return

                if const.CASE_OPERATION not in case:
                    if not ignore:
                        suiteExecPath = currentSuiteExecPath
                    continue

                operationName = case[const.CASE_OPERATION]
                opContext = None
                clientName = None

                if operationName in predefinedFuncDict.keys():
                    caseLocals = suiteLocals.copy()
                    if const.CASE_PARAMETERS in case:
                        parameters = case[const.CASE_PARAMETERS]
                    if const.CASE_CLIENT_NAME in case and (
                            clientName := case[const.CASE_CLIENT_NAME]) in self.clientDict:
                        serviceClient = self.clientDict[clientName]
                        caseLocals['Client'] = serviceClient
                        opContext = newContext(serviceClient.identityConfig, suiteLocals)
                        resolvePlaceholderDict(parameters, opContext)
                    else:
                        opContext = newContext(suiteLocals)
                        resolvePlaceholderDict(parameters, opContext)
                    caseLocals.update(parameters)
                    response = predefinedFuncDict[operationName](serviceModel=self, suiteLocals=suiteLocals,
                                                                 caseLocals=caseLocals)

                elif const.CASE_CLIENT_NAME in case and (clientName := case[const.CASE_CLIENT_NAME]) in self.clientDict \
                        and operationName in (serviceClient := self.clientDict[clientName]).supportOperations:
                    if const.CASE_PARAMETERS in case:
                        parameters = case[const.CASE_PARAMETERS]
                        opContext = newContext(serviceClient.identityConfig, suiteLocals)
                        resolvePlaceholderDict(parameters, opContext)
                    try:
                        # noinspection PyProtectedMember
                        response = BaseClient._make_api_call(serviceClient, operationName, parameters)
                    except ClientError as e:
                        response = e.response

                else:
                    raise RuntimeError(f'operation[{operationName}] undefined')

                case[const.CASE_RESPONSE] = response
                if not ignore:
                    logger.debug(
                        f"Response@{f'{clientName}@' if clientName else ''}{currentSuiteExecPath} => {response}")

                if const.CASE_ASSERTION in case:
                    assertion = case[const.CASE_ASSERTION]
                    resolvePlaceholderDict(assertion, opContext)
                    validateAssertions('response', assertion, response)
                case[const.CASE_SUCCESS] = True

                if not ignore:
                    suiteExecPath = currentSuiteExecPath
                suiteState = 1

            except Exception as e:
                case[const.CASE_SUCCESS] = False
                case[const.ERROR_INFO] = f'{e.__class__.__name__}({json.dumps(e.args)})'
                self.suite_failed.append(suite)
                logger.exception(currentSuiteExecPath, e)
                # terminate suite
                return
            finally:
                resetHooks = suiteLocals[const.RESET_HOOKS]
                for hook in resetHooks:
                    hook()

        if suiteState & const.STATE_SKIPPED:
            return

        # suite pass
        self.suite_pass.append(suite)

    def submitTask(self, target, args):
        t = Thread(target=target, args=args)
        t.start()
        self.hooks.append(t.join)


def initServicesTestModels(config, filterPattern):
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
        if serviceName not in const.AWS_SERVICES:
            logger.debug("not a aws service: {}, ignored", serviceName)
            continue
        testModel = ServiceTestModel(serviceName, suiteFiles, identities, clientConfig, filterPattern)
        serviceModels[serviceName] = testModel
    return serviceModels


def newContext(*dicts):
    requestContext = GLOBAL_VARIABLES.copy()
    for dc in dicts:
        requestContext.update(dc)
    return requestContext


def parseSuite(parentSuites: [], suites: dict):
    if suites is None:
        return parentSuites
    if not isinstance(suites, dict):
        raise ValueError('suite_nodes must be a dict')
    resultSuites = []
    caseOrdinal = itertools.count(0)
    for suiteWrapperName, suiteWrapper in suites.items():
        wrappedSuites = {suiteWrapperName: suiteWrapper}
        ignore = False
        if suiteWrapperName == const.HIDE:
            ignore = True
            wrappedSuites = suiteWrapper
        for suiteName, suite in wrappedSuites.items():
            midSuites = copy.deepcopy(parentSuites) if parentSuites else [[]]
            forkNode = {const.CASE_NAME: suiteName, const.ORDER: next(caseOrdinal)}
            if ignore:
                forkNode[const.HIDE] = True
            for midSuite in midSuites:
                midSuite.append(forkNode)
            for suiteCase in suite:
                if ignore:
                    suiteCase[const.HIDE] = True
                suiteCaseCopy = copy.deepcopy(suiteCase)
                midPath: str
                if const.CASE_OPERATION in suiteCase:
                    for midSuite in midSuites:
                        midSuite.append(suiteCaseCopy)
                if const.CASE_SUITES in suiteCase:
                    subSuites = suiteCase[const.CASE_SUITES]
                    del suiteCase[const.CASE_SUITES]
                    midSuites = parseSuite(midSuites, subSuites)
            resultSuites.extend(midSuites)
    return resultSuites


def reportResult(serviceModels):
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

        summary = f"{str(serviceName).upper()}: " \
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
            logger.error(summary)
        else:
            logger.info(summary)
