import json
import os
import unittest
import uuid

from loguru import logger

from core import const
from core.exporters import Exporter, appendTopics, createXmindFile
from core.place_holder import resolvePlaceHolder

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

    def testDetermineFilePath(self):
        p = uuid.uuid1().hex
        r = Exporter(".json")

        l = []
        try:
            for i in range(0, 3):
                f = r.determineFilePath(f'{p}/')
                l.append(f)
                if i == 0:
                    expect = f'{p}/aws_test.report'
                else:
                    expect = f'{p}/aws_test_{i}.report'
                self.assertEqual(f, os.path.abspath(expect))
                with open(f, 'a') as fp:
                    fp.write("")
        finally:
            for f in l:
                os.remove(f)
            os.removedirs(f'{p}')