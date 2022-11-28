import io
import json
import os
import zipfile

import yaml
from loguru import logger

from core import const


def loadFileData(filePath, loader):
    if not os.path.isfile(filePath):
        return
    with open(filePath, 'rb') as fp:
        payload = fp.read().decode('utf-8')

    logger.debug("Loading : %s" % filePath)
    # noinspection PyTypeChecker
    return loader(payload)


def loadConfig():
    filePath = os.getenv("aws_config", "../config.yaml")
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


def parseTopics(suites: [] = None, suite: [] = None, case: {} = None, topics: [] = None):
    if topics is None or len(topics) == 0:
        return
    if len(topics) == 1:
        suiteCase, subTopics = parseTopic(topics[0])
        if suite is None:
            suite = []
            suites.append(suite)
        suite.append(suiteCase)
        parseTopics(None, suite, suiteCase, subTopics)
    else:
        hide, isCreateNew, entries = False, False, []
        if const.HIDE in case and case[const.HIDE]:
            hide = True

        for topic in topics:
            suiteCase, subTopics = parseTopic(topic)
            entries.append((suiteCase, subTopics))
            if const.HIDE not in suite or suiteCase[const.HIDE] != hide:
                isCreateNew = True

        caseSuites = suites
        if isCreateNew:
            caseSuites = []
            newForkNodeCase = {"suites": caseSuites}
            suite.append(newForkNodeCase)
        elif caseSuites is None:
            caseSuites = []
            case['suites'] = caseSuites

        for suiteCase, subTopics in entries:
            newSuite = []
            caseSuites.append(newSuite)
            newSuite.append(suiteCase)
            parseTopics(None, newSuite, suiteCase, subTopics)


def parseTopic(topic):
    suiteCase = {'title': topic['title']}
    if 'notes' in topic and (notes := topic['notes']) and 'plain' in notes and (plain := notes['plain']) and 'content' in plain:
        if content := plain['content']:
            try:
                suiteCase = json.loads(content)
            except:
                pass
    subTopics = None
    if 'children' in topic and (children := topic['children']) and 'attached' in children:
        subTopics = children['attached']
    return suiteCase, subTopics


def loadXmindData(path):
    zf = None
    try:
        zf = zipfile.ZipFile(path)
        content = zf.read('content.json')
        data = json.load(io.BytesIO(content))

        result = {}
        for sheet in data:
            serviceName = sheet['title']
            rootTopic = sheet['rootTopic']

            # children
            if 'children' in rootTopic and 'attached' in rootTopic['children'] and (suitesCats := rootTopic['children']['attached']):
                if len(suitesCats) == 1 and (skipped := suitesCats[0])['title'] == 'SKIPPED':
                    if 'children' in skipped and 'attached' in skipped['children']:
                        topics = skipped['children']['attached']
                        if len(topics) > 0:
                            serviceSuites = []
                            parseTopics(suites=serviceSuites, topics=topics)
                            if len(serviceSuites) > 0:
                                result[serviceName] = serviceSuites

        return result
    finally:
        if zf is not None:
            zf.close()
