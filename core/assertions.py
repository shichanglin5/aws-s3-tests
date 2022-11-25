def parseResponseByDot(path, key, response):
    keyArr = key.split('.')
    result = response
    for k in keyArr:
        if k is None or k not in result:
            raise AssertionError(f'Assertion Error: {path}.{key} not exists!')
        else:
            result = result[k]
    return result


def validateAssertions(path: str, assertions: dict, response: dict):
    for key, value in assertions.items():
        result = parseResponseByDot(path, key, response)
        if isinstance(value, dict) and value:
            validateAssertions(f'{path}.{key}', value, result)
        elif isinstance(value, list) and value:
            validateAssertionArr(path, value, result)
        else:
            validateAssertionValue(path, key, value, result)


def validateAssertionArr(path, expect, result):
    if result is None or not isinstance(result, list) or len(result) != len(expect):
        msg = f'Assertion Error: at {path}, array length not equal'
        raise AssertionError(msg)
    for i, e in enumerate(expect):
        e1 = result[i]
        if isinstance(e, dict):
            validateAssertions(path, e, e1)
        elif isinstance(e, list):
            validateAssertionArr(path, e, e1)
        else:
            validateAssertionValue(path, f'[{i}]', e, e1)


def validateAssertionValue(path, key, expect, result):
    if result != expect:
        raise AssertionError(f'Assertion Error: at {path}.{key}, expect: {expect}, actual: {result}')
