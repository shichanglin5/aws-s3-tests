import io
import json
import os
import sys
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
    filePath = os.getenv("aws_config", "config/config-seaweedfs.yaml")
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


from typing import List

# 解析 topic（用例树）
## path 主要用于日志输出，定位哪个用例解析事变
### 1、首次调用传入 path 为空字符
### 2、后续 path 为上一次调用的 path + '->' + topic['title']，比如 a -> b -> c
def parseTopics(path="", suites: List = None, suite: List = None, case: dict = None, topics: List = None):
    if topics is None or len(topics) == 0:
        return
    if len(topics) == 1:
        topicTitle, suiteCase, subTopics = parseTopic(f'{path}->{topics[0]["title"]}', topics[0])
        if suite is None:
            suite = []
            suites.append(suite)
        # 将用例 case 追加到 suite 中
        suite.append(suiteCase)
        parseTopics(f'{path}->{topicTitle}', None, suite, suiteCase, subTopics)
    else:
        hide, isCreateNew, entries = False, False, []
        if case is None:
            case = {}
        if suite is None:
            suite = []
            suites.append(suite)
        if const.HIDE in case and case[const.HIDE]:
            hide = True

        for topic in topics:
            # 按照 topic 展开，每个 topic 分别解析后续用例
            topicTitle, suiteCase, subTopics = parseTopic(f'{path}->{topic["title"]}', topic)
            entries.append((topicTitle, suiteCase, subTopics))
            if const.HIDE not in suiteCase or suiteCase[const.HIDE] != hide:
                isCreateNew = True

        caseSuites = suites
        if isCreateNew:
            caseSuites = []
            newForkNodeCase = {"suites": caseSuites}
            suite.append(newForkNodeCase)
        elif caseSuites is None:
            caseSuites = []
            case['suites'] = caseSuites

        for topicTitle, suiteCase, subTopics in entries:
            newSuite = []
            caseSuites.append(newSuite)
            newSuite.append(suiteCase)
            parseTopics(f'{path}->{topicTitle}', None, newSuite, suiteCase, subTopics)


def parseTopic(path, topic):
    topicTitle = topic['title']
    suiteCase = {}
    # 解析注释（json文本），填充case内容
    if 'notes' in topic and (notes := topic['notes']) and 'plain' in notes and (plain := notes['plain']) and 'content' in plain:
        if (content := plain['content']) and (content := content.strip()):
            try:
                suiteCase = json.loads(content.replace(' ', ''))
            except Exception as e:
                logger.error("path:{}, json decode failed:\n{}", path, content)
                logger.exception(e)
                sys.exit(1)

    # 解析标签信息
    if const.CASE_CLIENT_NAME in suiteCase and 'labels' in topic and (labels := topic['labels']) and (clientLabel := labels[0]):
        if (labelItems := str(clientLabel).split('-')) and len(labelItems) == 2:
            # 从标签解析 clientName 比如（admin-200）, 200 为预期的 response status code
            # 将 response status code 设置到 case 的 assertion 中
            try:
                suiteCase[const.CASE_CLIENT_NAME] = labelItems[0]
                responseStatus = int(str(labelItems[1]).strip())
                if const.CASE_ASSERTION in suiteCase:
                    assertion = suiteCase[const.CASE_ASSERTION]
                    if 'ResponseMetadata.HTTPStatusCode' in assertion:
                        if not assertion['ResponseMetadata.HTTPStatusCode'] == responseStatus:
                            suiteCase[const.CASE_ASSERTION] = {"ResponseMetadata.HTTPStatusCode": responseStatus}
                    else:
                        suiteCase[const.CASE_ASSERTION] = {"ResponseMetadata.HTTPStatusCode": responseStatus}
                else:
                    suiteCase[const.CASE_ASSERTION] = {"ResponseMetadata.HTTPStatusCode": responseStatus}
            except:
                pass
        else:
            # 如果 clientName 不包含状态吗信息（比如 admin)则不更新 assertion
            suiteCase[const.CASE_CLIENT_NAME] = clientLabel
            if const.CASE_ASSERTION in suiteCase:
                del suiteCase[const.CASE_ASSERTION]
    subTopics = None
    if 'children' in topic and (children := topic['children']) and 'attached' in children:
        subTopics = children['attached']

    # 将 topic title 设置到 case 中
    suiteCase[const.CASE_TITLE] = topicTitle
    return topicTitle, suiteCase, subTopics


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

            # 如果 rootTopic 的 title（转小写）不是 s3-tests 则跳过
            if rootTopic['title'].lower() != 's3-tests':
                continue

            # children
            if 'children' in rootTopic and 'attached' in rootTopic['children'] and (topics := rootTopic['children']['attached']):
                # topics 基于 ownership 归类：每个 ownership 单独定义一组 suite
                if len(topics) == 0:
                    continue
                serviceSuites = []
                parseTopics(path=serviceName, suites=serviceSuites, topics=topics)
                if len(serviceSuites) > 0:
                    if serviceName in result:
                        # 合并同一 serviceName 的 suites
                        result[serviceName].extend(serviceSuites)
                    else:
                        # 基于 serviceName 分组（s3, es, ..)
                        result[serviceName] = serviceSuites
        # postProcess(result)
        return result
    finally:
        if zf is not None:
            zf.close()


# def postProcess(result):
#     if 's3' in result and (s3Suites := result['s3']):
#         for s3Suite in s3Suites:
#             s3Suite.append({
#                 "operation": "DropBucket",
#                 "clientName": "admin",
#                 "parameters": {
#                     "Bucket": "${Bucket}"
#                 },
#                 "__hide__": True
#             })
