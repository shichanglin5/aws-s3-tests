import re
import sys
from time import time

from loguru import logger

from core import const
from core.exporters import EXPORTER_DICT
from core.loader import loadConfig
from core.models import initServicesTestModels, reportResult


def parseFilterPatterns(args: [] = None):
    if not args:
        return [], []
    includePatterns = []
    excludePatterns = []
    for arg in args:
        kv = arg.split('=')
        if len(kv) != 2:
            raise ValueError('Invalid Argument, Must be -k=v format')
        key = kv[0]
        value = kv[1]
        if key == '-includes':
            includePatterns = [re.compile(str.strip(s)) for s in value.split(',')]
        elif key == '-excludes':
            excludePatterns = [re.compile(str.strip(s)) for s in value.split(',')]
        else:
            raise ValueError("Invalid filter pattern", arg)
    return includePatterns, excludePatterns


def main(args: []):
    start = time()

    config = loadConfig()
    includePatterns, excludePatterns = parseFilterPatterns(args)
    sms = initServicesTestModels(config, includePatterns, excludePatterns)
    if len(sms) == 0:
        logger.info("No serviceModels loaded.")
        return

    for serviceName, serviceModel in sms.items():
        logger.info(f'Run ServiceModel: {serviceName}')
        serviceModel.setUp()
        serviceModel.run()
        serviceModel.tearDown()

    end = time()
    logger.info('Tests Completed. Time Spent: %.2fs' % (end - start))
    summary = reportResult(sms)

    if const.EXPORTERS in config:
        exporters = config[const.EXPORTERS]
        for name, conf in exporters.items():
            if name in EXPORTER_DICT:
                exporter = EXPORTER_DICT[name]
                exporter(conf, summary).generateReport(sms)
            else:
                logger.warning('export {} not found, skipping.'.format(name))


if __name__ == '__main__':
    main(sys.argv[1:])
