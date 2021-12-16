import json

from joblib import hash


def construct_tuple(*values):
    return json.dumps(values).replace('[', '(').replace(']', ')')


class ContainsWrap:
    pass


class NoneType:
    def __eq__(self, other):
        if isinstance(other, NoneType):
            return True
        return False

    def __hash__(self):
        return 0


class DelType:
    pass


class NoneWrap:
    pass


none = NoneType()
contain = ContainsWrap()


def inthash(item):
    return int(hash(item)[:8], 16)


def fetchmany(conn, sql, chunksize):
    """

    :param conn:
    :param sql:
    :param chunksize:
    :return:

    :raise: sqlite3.DatabaseError
    """
    cursor = conn.execute(sql)
    res = cursor.fetchmany(chunksize)
    while len(res) > 0:
        yield res
        res = cursor.fetchmany(chunksize)
