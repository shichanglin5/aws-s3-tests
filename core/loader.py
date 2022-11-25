import os

import yaml
from loguru import logger


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
