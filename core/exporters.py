import itertools
import json
import os
import zipfile

from botocore.model import ServiceModel
from loguru import logger

from core import const
from core.models import ServiceTestModel


class Exporter:
    def __init__(self, fileExtension, config):
        self.filePath = determineFilePath(filePath=config['file_path'] if 'file_path' in config else None, ext=fileExtension)
        self.includeFields = config[const.INCLUDE_FIELDS] if const.INCLUDE_FIELDS in config else []

    def generateReport(self, serviceModels: {str, ServiceModel}):
        try:
            self.doGenerateReport(serviceModels)
        except Exception as e:
            logger.error("[{}] Failed to generate export: {}", self.__class__.__name__, e)
            raise e

    def doGenerateReport(self, serviceModels: {str, ServiceModel}):
        raise NotImplementedError


def determineFilePath(filePath=None, file='aws_tests', ext="file"):
    if ext is None:
        ext = '.export'
    if filePath is None:
        filePath = "exports/"
    else:
        path = os.path.dirname(filePath)
        fileWithExt = os.path.basename(filePath)
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
    def __init__(self, config: dict):
        super().__init__("xmind", config)

    def doGenerateReport(self, serviceModels: {str, ServiceTestModel}):
        content = self.buildXmindData(serviceModels)
        if content is None:
            raise ValueError("content is None")
        if os.path.exists(self.filePath):
            raise FileExistsError(f'{self.filePath} already exists')
        zf = zipfile.ZipFile(self.filePath, 'w')
        try:
            zf.writestr('content.json', json.dumps(content))
            zf.writestr('manifest.json', json.dumps({"file-entries": {"content.json": {}, "metadata.json": {}}}))
            zf.writestr('metadata.json',
                        json.dumps({"creator": {"name": "Vana", "version": "12.0.2.202204260739"}}))
            logger.info('xmind_reports: %s' % self.filePath)
        except Exception as e:
            logger.exception(e)
        finally:
            zf.close()

    def buildXmindData(self, serviceModels: {str, ServiceTestModel}):
        content = []
        for serviceName, serviceModel in serviceModels.items():
            sheet, subTopics = createSheet(serviceName)
            # logger.info(json.dumps(serviceModel.suite_pass))
            self.appendTopics(subTopics, 'PASS', serviceModel.suite_pass, "#15831C")
            self.appendTopics(subTopics, 'FAILED', serviceModel.suite_failed, "#E32C2D")
            self.appendTopics(subTopics, 'SKIPPED', serviceModel.suite_skipped, "#D0D0D0")
            content.append(sheet)
        return content

    def appendTopics(self, subTopics, topicTitle, suites, lineColor):
        caseTree, caseNodes, var_node, var_tree = {}, [], 'nodes', 'tree'
        for suite in suites:
            midTree = caseTree
            midNodes = caseNodes
            for case in suite:
                ignoreCase = False
                caseFailed = False
                if const.HIDE in case and case[const.HIDE]:
                    ignoreCase = True
                if const.CASE_OPERATION not in case:
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
                title = case[const.CASE_NAME] if const.CASE_NAME in case else case[const.CASE_OPERATION]

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
                        case[const.CASE_CLIENT_NAME]
                    ] if const.CASE_CLIENT_NAME in case else None,
                    "markers": [
                        {
                            "markerId": "symbol-exclam"
                        }
                    ] if caseFailed else None,
                    "notes": {
                        "plain": {
                            "content": '\n\n'.join(itr) if (
                                itr := ['### %s ###\n%s' % (str(field).capitalize(), json.dumps(fieldValue, default=IgnoreNotSerializable)) for field in self.includeFields if
                                        field in case and (fieldValue := case[field])]) else None
                        }
                    } if self.includeFields and [field for field in self.includeFields if field in case] else None
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


def IgnoreNotSerializable(o):
    return f'skipped@{o.__class__.__name__}'


def sortNodes(nodes):
    list.sort(nodes, key=lambda n: n[const.ORDER])
    for node in nodes:
        del node[const.ORDER]
        if 'children' in node:
            children = node['children']
            if 'attached' in children:
                attachedNodes = children['attached']
                sortNodes(attachedNodes)


def createSheet(serviceName):
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
            "centralTopic": {
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
                "properties": {
                    "line-color": "#52CC83",
                    "shape-class": "org.xmind.summaryShape.round",
                    "line-width": "2pt",
                    "line-pattern": "solid",
                    "line-corner": "8pt"
                }
            },
            "relationship": {
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
                "properties": {
                    "svg:fill": "#FFFFFF",
                    "multi-line-colors": "#E7C2C0 #F3D4B2 #D8DCAF #A8D4CC #B0C0D0 #C8BAC9",
                    "color-list": "#000229 #1F2766 #52CC83 #4D86DB #99142F #245570",
                    "line-tapered": "none"
                }
            },
            "importantTopic": {
                "properties": {
                    "svg:fill": "#CC8352",
                    "fill-pattern": "solid",
                    "border-line-color": "#CC8352"
                }
            },
            "minorTopic": {
                "properties": {
                    "svg:fill": "#8352CC",
                    "fill-pattern": "solid",
                    "border-line-color": "#8352CC"
                }
            },
            "colorThemeId": "Rainbow-#52CC83-MULTI_LINE_COLORS",
            "expiredTopic": {
                "properties": {
                    "fo:text-decoration": "line-through",
                    "svg:fill": "none"
                }
            },
            "global": {
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
    return sheet, subTopics


EXPORTER_DICT = {
    "xmind": XmindExporter
}
