import re
from time import time

from loguru import logger

from core import const
from core.exporters import EXPORTER_DICT
from core.loader import loadConfig
from core.models import initServicesTestModels, reportResult


def main(*prefixes):
    start = time()
    filterPattern = ''
    if prefixes:
        filterPattern = re.compile(prefixes[0])
    config = loadConfig()
    sms = initServicesTestModels(config, filterPattern)

    for serviceName, serviceModel in sms.items():
        logger.info(f'Run ServiceModel: {serviceName}')
        serviceModel.setUp()
        serviceModel.run()
        serviceModel.tearDown()

    end = time()
    logger.info('Tests Completed. Time Spent: %.2fs' % (end - start))
    reportResult(sms)

    if const.EXPORTERS in config:
        exporters = config[const.EXPORTERS]
        for name, filePath in exporters.items():
            if name in EXPORTER_DICT:
                exporter = EXPORTER_DICT[name]
                exporter().generateReport(sms, filePath)
            else:
                logger.warning('export {} not found, skipping.'.format(name))


if __name__ == '__main__':
    # main(sys.argv[1:])
    main('.*OwnershipAndACL.*')
    # main()
