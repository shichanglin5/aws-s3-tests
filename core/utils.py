import json


def IgnoreNotSerializable(o):
    return f'skipped@{o.__class__.__name__}'


def ToJsonStr(o):
    return json.dumps(o, default=IgnoreNotSerializable, indent=2)
