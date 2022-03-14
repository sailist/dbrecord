import json
import sqlite3

from joblib import hash


def mark_in_database(database, tag):
    conn = sqlite3.connect(database)
    sql = f'''CREATE TABLE {tag} (ID INTEGER PRIMARY KEY  AUTOINCREMENT)'''
    try:
        conn.execute(sql)
    except sqlite3.Error as e:
        return e
    return True


def check_database_exists(database, table):
    sql = f"SELECT 1 FROM {table}"
    conn = sqlite3.connect(database)
    try:
        conn.execute(sql)
        conn.commit()
    except sqlite3.Error as e:
        # print(e)
        return False
    return True


def create_database(database: str):
    conn = sqlite3.connect(database)
    sql = f'''CREATE TABLE DICT
                       (ID INTEGER PRIMARY KEY  AUTOINCREMENT,
                       INTHASH INTEGER  NOT NULL,
                       KEY VARCHAR UNIQUE NOT NULL,
                       VALUE VARCHAR  NOT NULL
                       );
                       CREATE INDEX type on DICT (INTHASH);'''
    try:
        conn.executescript(sql)
    finally:
        return conn


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
        yield from res
        res = cursor.fetchmany(chunksize)


def fetchall(conn, sql):
    """

    :param conn:
    :param sql:
    :param chunksize:
    :return:

    :raise: sqlite3.DatabaseError
    """
    cursor = conn.execute(sql)
    return cursor.fetchall()
