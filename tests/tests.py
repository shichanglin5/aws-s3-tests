import re
import unittest
import uuid

import boto3
from loguru import logger

from main import resolvePlaceHolder

_: uuid.UUID


class Tests(unittest.TestCase):
    def testResolvePlaceHolder(self):
        context = {
            'a': 100,
            'b': '100'
        }
        s0 = '${x}'
        s1 = '${a}'
        s2 = '${b}'
        s3 = 's2_${a}'
        s4 = 's2_${b}'
        s5 = '@{bytearray(${a})}'
        s6 = '@{bytearray(${b})}'
        self.assertEqual(None, resolvePlaceHolder(s0, context))
        self.assertEqual(100, resolvePlaceHolder(s1, context))
        self.assertEqual('100', resolvePlaceHolder(s2, context))
        self.assertEqual('s2_100', resolvePlaceHolder(s3, context))
        self.assertEqual('s2_100', resolvePlaceHolder(s4, context))
        self.assertEqual(bytearray(100), resolvePlaceHolder(s5, context))
        self.assertEqual(bytearray(100), resolvePlaceHolder(s6, context))

    def tests(self):
        logger.info(uuid.uuid1().hex)
