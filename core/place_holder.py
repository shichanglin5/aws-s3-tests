import numbers
import re
from typing import Any

from core import const

PLACE_HOLDER_HANDLERS: [re.Pattern, lambda arg, context: Any] = {
    re.compile('(\$\{(.*?)})'): lambda arg, context: context[arg] if arg in context else None,
    re.compile('(@\{(.*?)})'): lambda arg, context: eval(arg, context)}


def resolvePlaceholderDict(parameters, context):
    if parameters is None or len(parameters) == 0:
        return
    for k, v in parameters.items():
        if isinstance(v, dict) and dict:
            resolvePlaceholderDict(v, context)
        elif isinstance(v, list) and len(v):
            resolvePlaceHolderArr(v, context)
        elif isinstance(v, str):
            originalValue = v
            try:
                v = resolvePlaceHolder(v, context)
                parameters[k] = v
            finally:
                if not isinstance(v, (numbers.Number, str)):
                    def resetValue():
                        parameters[k] = originalValue

                    context[const.RESET_HOOKS].append(resetValue)
        elif isinstance(v, numbers.Number):
            continue
        else:
            raise RuntimeError(f'Unsupported parameter: {v}')


def resolvePlaceHolderArr(valueArray, context):
    if valueArray is None or len(valueArray) == 0:
        return
    for index, value in enumerate(valueArray):
        if isinstance(value, list) and len(value):
            resolvePlaceHolderArr(value, context)
        if isinstance(value, dict) and len(value):
            resolvePlaceholderDict(value, context)
        elif isinstance(value, str):
            originalValue = value
            try:
                value = resolvePlaceHolder(value, context)
                valueArray[index] = value
            finally:
                if not isinstance(value, (numbers.Number, str)):
                    def resetValue():
                        valueArray[index] = originalValue

                    context[const.RESET_HOOKS].append(resetValue())
        elif isinstance(value, numbers.Number):
            continue
        else:
            raise RuntimeError('Unsupported parameter', value)


def resolvePlaceHolder(value, context):
    for pattern, handler in PLACE_HOLDER_HANDLERS.items():
        for placeHolder, paramName in set(pattern.findall(value)):
            paramValue = handler(paramName, context)
            if placeHolder == value:
                if paramValue is None:
                    return None
                return paramValue
            else:
                if paramValue is None:
                    paramValue = ''
                value = value.replace(placeHolder, str(paramValue))
    return value
