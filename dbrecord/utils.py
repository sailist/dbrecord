import json

from joblib import hash


def construct_tuple(*values):
    return json.dumps(values).replace('[', '(').replace(']', ')')


class ContainsWrap:
    pass


class NoneType:
    pass


class NoneWrap:
    pass


none = NoneType()
contrain = ContainsWrap()


def inthash(item):
    return int(hash(item)[:8], 16)
