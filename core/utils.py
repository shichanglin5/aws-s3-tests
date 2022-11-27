def IgnoreNotSerializable(o):
    return f'skipped@{o.__class__.__name__}'