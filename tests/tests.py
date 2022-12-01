import itertools
import json
import os
import re
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

from loguru import logger

from core.exporters import determineFilePath
from core.place_holder import resolvePlaceHolder

_: uuid.UUID


class Tests(unittest.TestCase):
    def testResolvePlaceHolder(self):
        context = {
            'a': 100,
            'b': '100',
            'uuid': lambda: uuid.uuid1().hex,
            "bucketOrdinal": itertools.count(1)
        }
        s0 = '${x}'
        s1 = '${a}'
        s2 = '${b}'
        s3 = 's2_${a}'
        s4 = 's2_${b}'
        s5 = '@{bytearray(${a})}'
        s6 = '@{bytearray(${b})}'
        self.assertEqual('${x}', resolvePlaceHolder(s0, context))
        self.assertEqual(100, resolvePlaceHolder(s1, context))
        self.assertEqual('100', resolvePlaceHolder(s2, context))
        self.assertEqual('s2_100', resolvePlaceHolder(s3, context))
        self.assertEqual('s2_100', resolvePlaceHolder(s4, context))
        self.assertEqual(bytearray(100), resolvePlaceHolder(s5, context))
        self.assertEqual(bytearray(100), resolvePlaceHolder(s6, context))
        self.assertTrue(resolvePlaceHolder('/a/b/c/@{uuid()}', context).startswith('/a/b/c'))
        self.assertEqual(resolvePlaceHolder('aws-s3-tests-@{next(bucketOrdinal)}', context), "aws-s3-tests-1")
        self.assertEqual(resolvePlaceHolder('@{next(bucketOrdinal)}', context), 2)
        self.assertEqual(resolvePlaceHolder('@{next(bucketOrdinal)}', context), 3)

    def testPattern(self):
        p1 = re.compile('(\$\{(.*?)})')
        self.assertEqual(p1.findall("${name}"), [('${name}', 'name')])

        p1 = re.compile('(@\{(.*?)})')
        self.assertEqual(p1.findall("@{name}"), [('@{name}', 'name')])

    def testDetermineFilePath(self):
        p, li = uuid.uuid1().hex, []
        try:
            for i in range(0, 3):
                f = determineFilePath(f'{p}/')
                li.append(f)
                if i == 0:
                    expect = f'{p}/aws_tests.file'
                else:
                    expect = f'{p}/aws_tests_{i}.file'
                self.assertEqual(f, os.path.abspath(expect))
                with open(f, 'a') as fp:
                    fp.write("")
        finally:
            for f in li:
                os.remove(f)
            os.removedirs(f'{p}')

    def testAtomicCounter(self):
        hooks = []
        threadCount = 1000
        counter = itertools.count(1)
        for i in range(threadCount):
            t = threading.Thread(target=lambda: next(counter))
            t.start()
            hooks.append(t.join)
        for hook in hooks:
            hook()
        self.assertEqual(next(counter), threadCount + 1)

    def testAssertionError(self):
        try:
            msg = "assertion error at 'Error.Code'"
            assertions = {
                'a': 1
            }
            response = {
                'a': 2,
                'b': 3
            }
            raise AssertionError(msg, "x", assertions, "y", response)
        except Exception as e:
            if isinstance(e, AssertionError):
                logger.error(e)
            else:
                logger.exception(e)

    def testThreadPool(self):
        with ThreadPoolExecutor(max_workers=50) as pool:
            f = pool.submit(lambda x,y: time.sleep(3), 1,2)
            print("wait three seconds")
            print(f.result())
            pool.shutdown()
