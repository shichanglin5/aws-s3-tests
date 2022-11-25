import itertools
import json
import os
import zipfile

from botocore.model import ServiceModel
from loguru import logger

from core import const
from core.models import ServiceTestModel


class Exporter:
    def __init__(self, fileExtension):
        self.fileExtension = fileExtension

    def generateReport(self, serviceModels: {str, ServiceModel}, filePath):
        try:
            filePath = self.determineFilePath(filePath, ext=self.fileExtension)
            self.doGenerateReport(serviceModels, filePath)
        except Exception as e:
            logger.error("[{}] Failed to generate export: {}", self.__class__.__name__, e)
            raise e

    def doGenerateReport(self, serviceModels: {str, ServiceModel}, filePath):
        raise NotImplementedError

    def determineFilePath(self, path=None, file='aws_test', ext=None):
        if ext is None:
            ext = self.fileExtension
        if path is None:
            path = "../export/"
        else:
            if path.endswith('/'):
                path = os.path.dirname(path)
            else:
                fileWithExt = os.path.basename(path)
                a, b = os.path.splitext(fileWithExt)
                if a:
                    file = a
                if b:
                    ext = b[1:]
        if os.path.exists(finalPath := os.path.abspath(f'{path}/{file}.{ext}')):
            fileOrdinal = itertools.count(1)
            while os.path.exists(finalPath := os.path.abspath(f'{path}/{file}_{next(fileOrdinal)}.{ext}')):
                continue
        if not os.path.exists(path):
            os.makedirs(path)
        return finalPath


class XmindExporter(Exporter):
    def __init__(self):
        super().__init__("xmind")

    def doGenerateReport(self, serviceModels: {str, ServiceTestModel}, filePath):
        createXmindFile(filePath, buildXmindData(serviceModels))


def buildXmindData(serviceModels: {str, ServiceTestModel}):
    content = []
    for serviceName, serviceModel in serviceModels.items():
        subTopics = []
        sheet = {
            "class": "sheet",
            "title": f"{serviceName}",
            "topicPositioning": "fixed",
            "rootTopic": {
                "class": "topic",
                "title": f'{serviceName.upper()}-Tests',
                "structureClass": "org.xmind.ui.logic.right",
                "children": {
                    "attached": subTopics
                },
                "style": {
                    "properties": {
                        "fill-pattern": "solid",
                        "line-color": "#9C27B0",
                        "svg:fill": "#9C27B0FF",
                        "line-pattern": "solid",
                        "line-width": "3pt"
                    }
                }
            },
            "extensions": [],
            "theme": {
                "id": "e59ea4da-d01f-4167-98ba-3a2354002b50",
                "centralTopic": {
                    "id": "c5069014-b642-4cf5-bb50-1d29bd0df2a1",
                    "properties": {
                        "line-color": "#52CC83",
                        "shape-class": "org.xmind.topicShape.roundedRect",
                        "line-class": "org.xmind.branchConnection.curve",
                        "line-width": "3pt",
                        "line-pattern": "solid",
                        "fill-pattern": "solid",
                        "border-line-width": "0pt",
                        "arrow-end-class": "org.xmind.arrowShape.none",
                        "alignment-by-level": "inactived",
                        "fo:font-family": "NeverMind",
                        "fo:font-style": "normal",
                        "fo:font-weight": 500,
                        "fo:font-size": "30pt",
                        "fo:text-transform": "manual",
                        "fo:text-decoration": "none",
                        "fo:text-align": "center",
                        "svg:fill": "#52CC83"
                    }
                },
                "mainTopic": {
                    "id": "70cef26a-bf8a-4a75-a3ba-c39b54a6401d",
                    "properties": {
                        "shape-class": "org.xmind.topicShape.roundedRect",
                        "line-class": "org.xmind.branchConnection.roundedElbow",
                        "line-width": "2pt",
                        "fill-pattern": "solid",
                        "border-line-width": "0pt",
                        "fo:font-family": "NeverMind",
                        "fo:font-style": "normal",
                        "fo:font-weight": 500,
                        "fo:font-size": "18pt",
                        "fo:text-transform": "manual",
                        "fo:text-decoration": "none",
                        "fo:text-align": "left",
                        "svg:fill": "#1F2766"
                    }
                },
                "subTopic": {
                    "id": "d5c7d9c0-e954-4c99-9e01-91cccd629c22",
                    "properties": {
                        "shape-class": "org.xmind.topicShape.roundedRect",
                        "line-class": "org.xmind.branchConnection.roundedElbow",
                        "fill-pattern": "solid",
                        "fo:font-family": "NeverMind",
                        "fo:font-style": "normal",
                        "fo:font-weight": 400,
                        "fo:font-size": "14pt",
                        "fo:text-transform": "manual",
                        "fo:text-decoration": "none",
                        "fo:text-align": "left",
                        "line-width": "2pt",
                        "border-line-width": "0pt",
                        "svg:fill": "#FFFFFF"
                    }
                },
                "summaryTopic": {
                    "id": "dc5d147f-2c2a-423f-8d4f-26c6e4cdc4ec",
                    "properties": {
                        "border-line-color": "#52CC83",
                        "shape-class": "org.xmind.topicShape.roundedRect",
                        "line-class": "org.xmind.branchConnection.roundedElbow",
                        "fill-pattern": "solid",
                        "fo:font-family": "NeverMind",
                        "fo:font-style": "normal",
                        "fo:font-weight": "400",
                        "fo:font-size": "14pt",
                        "fo:text-transform": "manual",
                        "fo:text-decoration": "none",
                        "fo:text-align": "left",
                        "svg:fill": "none"
                    }
                },
                "calloutTopic": {
                    "id": "b2ccd2cb-e4d0-4c1d-8615-fea7a1b98854",
                    "properties": {
                        "svg:fill": "#52CC83",
                        "border-line-color": "#52CC83",
                        "callout-shape-class": "org.xmind.calloutTopicShape.balloon.roundedRect",
                        "fill-pattern": "solid",
                        "fo:font-family": "NeverMind",
                        "fo:font-style": "normal",
                        "fo:font-weight": 400,
                        "fo:font-size": "14pt",
                        "fo:text-transform": "manual",
                        "fo:text-decoration": "none",
                        "fo:text-align": "left"
                    }
                },
                "floatingTopic": {
                    "id": "1db99452-2bb0-4de8-87f5-cdeb8a591d7b",
                    "properties": {
                        "border-line-color": "#EEEBEE",
                        "shape-class": "org.xmind.topicShape.roundedRect",
                        "line-class": "org.xmind.branchConnection.roundedElbow",
                        "line-width": "2pt",
                        "line-pattern": "solid",
                        "fill-pattern": "solid",
                        "arrow-end-class": "org.xmind.arrowShape.none",
                        "fo:font-family": "NeverMind",
                        "fo:font-style": "normal",
                        "fo:font-weight": 500,
                        "fo:font-size": "14pt",
                        "fo:text-transform": "manual",
                        "fo:text-decoration": "none",
                        "fo:text-align": "left",
                        "border-line-width": "0pt",
                        "svg:fill": "#EEEBEE"
                    }
                },
                "boundary": {
                    "id": "beaff7ce-691f-481d-847c-c5f43d125660",
                    "properties": {
                        "svg:fill": "#52CC83",
                        "line-color": "#52CC83",
                        "shape-class": "org.xmind.boundaryShape.roundedRect",
                        "shape-corner": "20pt",
                        "line-width": "2",
                        "line-pattern": "dash",
                        "fill-pattern": "solid",
                        "fo:font-family": "'NeverMind','Microsoft YaHei','PingFang SC','Microsoft JhengHei','sans-serif',sans-serif",
                        "fo:font-style": "normal",
                        "fo:font-weight": 400,
                        "fo:font-size": "14pt",
                        "fo:text-transform": "manual",
                        "fo:text-decoration": "none",
                        "fo:text-align": "center"
                    }
                },
                "summary": {
                    "id": "7e7d8704-9339-4a37-a8dc-f3a3aa2f4cf2",
                    "properties": {
                        "line-color": "#52CC83",
                        "shape-class": "org.xmind.summaryShape.round",
                        "line-width": "2pt",
                        "line-pattern": "solid",
                        "line-corner": "8pt"
                    }
                },
                "relationship": {
                    "id": "81c8d0be-6082-4a1c-b88c-23b1445e0647",
                    "properties": {
                        "line-color": "#52CC83",
                        "shape-class": "org.xmind.relationshipShape.curved",
                        "line-width": "2",
                        "line-pattern": "dash",
                        "arrow-begin-class": "org.xmind.arrowShape.none",
                        "arrow-end-class": "org.xmind.arrowShape.triangle",
                        "fo:font-family": "'NeverMind','Microsoft YaHei','PingFang SC','Microsoft JhengHei','sans-serif',sans-serif",
                        "fo:font-style": "normal",
                        "fo:font-weight": 400,
                        "fo:font-size": "13pt",
                        "fo:text-transform": "manual",
                        "fo:text-decoration": "none",
                        "fo:text-align": "center"
                    }
                },
                "map": {
                    "id": "ea5bbe08-7b7a-4b13-a4ee-49b20dcc4de2",
                    "properties": {
                        "svg:fill": "#FFFFFF",
                        "multi-line-colors": "#E7C2C0 #F3D4B2 #D8DCAF #A8D4CC #B0C0D0 #C8BAC9",
                        "color-list": "#000229 #1F2766 #52CC83 #4D86DB #99142F #245570",
                        "line-tapered": "none"
                    }
                },
                "importantTopic": {
                    "id": "6c4e5a0a-db82-4dc6-9816-213ad1a38238",
                    "properties": {
                        "svg:fill": "#CC8352",
                        "fill-pattern": "solid",
                        "border-line-color": "#CC8352"
                    }
                },
                "minorTopic": {
                    "id": "febf0c47-d75e-4149-a07f-a44cf847e7ab",
                    "properties": {
                        "svg:fill": "#8352CC",
                        "fill-pattern": "solid",
                        "border-line-color": "#8352CC"
                    }
                },
                "colorThemeId": "Rainbow-#52CC83-MULTI_LINE_COLORS",
                "expiredTopic": {
                    "id": "c49f792d-3ac6-4317-bb98-7a2cfe7120c7",
                    "properties": {
                        "fo:text-decoration": "line-through",
                        "svg:fill": "none"
                    }
                },
                "global": {
                    "id": "2888378f-bbb8-46ad-a4d6-980d780823a9",
                    "properties": {}
                },
                "skeletonThemeId": "db4a5df4db39a8cd1310ea55ea"
            },
            "style": {
                "properties": {
                    "multi-line-colors": "none",
                    "line-tapered": "none"
                }
            },
            "coreVersion": "2.54.0"
        }
        # logger.info(json.dumps(serviceModel.suite_pass))
        appendTopics(subTopics, 'PASS', serviceModel.suite_pass, "#15831C")
        appendTopics(subTopics, 'FAILED', serviceModel.suite_failed, "#E32C2D")
        appendTopics(subTopics, 'SKIPPED', serviceModel.suite_skipped, "#D0D0D0")
        content.append(sheet)
    return content


def appendTopics(subTopics, topicTitle, suites, lineColor):
    caseTree, caseNodes, var_node, var_tree = {}, [], 'nodes', 'tree'
    for suite in suites:
        midTree = caseTree
        midNodes = caseNodes
        for case in suite:
            ignoreCase = False
            caseFailed = False
            if const.HIDE in case and case[const.HIDE]:
                ignoreCase = True
            if 'operation' not in case:
                if ignoreCase:
                    continue
                # style for fork node
                style = {
                    "line-pattern": "solid",
                    "line-width": "3pt",
                    "line-color": lineColor,
                    "fo:color": "#000000FF",
                }
            elif const.CASE_SUCCESS not in case:
                if ignoreCase:
                    continue
                # style for skipped case
                style = {
                    "line-pattern": "solid",
                    "line-width": "3pt",
                    "svg:fill": "#D0D0D0FF",
                    "line-color": "#D0D0D0FF",
                    "fo:color": "#000000FF",
                }
            else:
                if case[const.CASE_SUCCESS]:
                    if ignoreCase:
                        continue
                    # style for skipped case
                    style = {
                        "line-pattern": "solid",
                        "svg:fill": "#15831CFF",
                        "line-width": "3pt",
                        "line-color": lineColor,
                    }
                else:
                    caseFailed = True
                    # style for skipped case
                    style = {
                        "line-pattern": "solid",
                        "svg:fill": "#E32C2D",
                        "line-width": "3pt",
                        "line-color": lineColor,
                    }
            title = case['name'] if 'name' in case else case['operation']

            # find existing tree node
            if title in midTree:
                mid = midTree[title]
                midTree = mid[var_tree]
                midNodes = mid[var_node]
                continue

            # point 2
            newSubNodes = []
            newSubTree = {}
            newNode = {var_tree: newSubTree, var_node: newSubNodes}

            # fill to mid
            midNodes.append({
                const.ORDER: case[const.ORDER] if const.ORDER in case else 0,
                "title": title,
                "style": {
                    "properties": style
                },
                "children": {
                    "attached": newSubNodes
                },
                "labels": [
                    case['clientName']
                ] if 'clientName' in case else None,
                "markers": [
                    {
                        "markerId": "symbol-exclam"
                    }
                ] if caseFailed else None,
                "notes": {
                    "plain": {
                        "content": ('parameters: %s\n\n' % json.dumps(
                            case["parameters"]) if "parameters" in case else "") + ('assertions: %s\n\n' % json.dumps(
                            case["assertion"]) if "assertion" in case else "") + ('error_info: %s\n\n' % case[
                            const.ERROR_INFO] if caseFailed and const.ERROR_INFO in case else "")
                    }
                } if 'parameters' in case or 'assertion' in case else None
            })
            midTree[title] = newNode

            # update mid
            midNodes = newSubNodes
            midTree = newSubTree
    if caseNodes:
        sortNodes(caseNodes)
        subTopics.append({
            "title": topicTitle,
            "children": {
                "attached": caseNodes,
            },
            "style": {
                "properties": {
                    "line-pattern": "solid",
                    "svg:fill": lineColor,
                    "line-width": "3pt",
                    "line-color": lineColor,
                }
            }})


def sortNodes(nodes):
    list.sort(nodes, key=lambda n: n[const.ORDER])
    for node in nodes:
        del node[const.ORDER]
        if 'children' in node:
            children = node['children']
            if 'attached' in children:
                attachedNodes = children['attached']
                sortNodes(attachedNodes)


def createXmindFile(filePath, content):
    if content is None:
        raise ValueError("content is None")
    if os.path.exists(filePath):
        raise FileExistsError(f'{filePath} already exists')
    zf = zipfile.ZipFile(filePath, 'w')
    try:
        zf.writestr('content.json', json.dumps(content))
        zf.writestr('manifest.json', json.dumps({"file-entries": {"content.json": {}, "metadata.json": {}}}))
        zf.writestr('metadata.json',
                    json.dumps({"creator": {"name": "Vana", "version": "12.0.2.202204260739"}}))
    except Exception as e:
        logger.exception(e)
    finally:
        zf.close()


EXPORTER_DICT = {
    "xmind": XmindExporter
}
