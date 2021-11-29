import pickle
import sqlite3

from .utils import construct_tuple, NoneWrap


def de_nonewrap(value):
    if isinstance(value, NoneWrap):
        return None
    return value


class PList:
    def __init__(self, db_file):
        self._conn = None
        self._db = None
        self.db_file = db_file

    def __getstate__(self):
        return {'db_file': self.db_file, '_db': None, '_conn': None}

    def __len__(self):
        from dbrecord.summary import count_table
        self.reconn()
        return count_table(self.conn, 'DICT')

    @property
    def conn(self):
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_file)
        return self._conn

    @property
    def db(self):
        if self._db is None:
            self._db = self.conn.cursor()
        return self._db

    def reconn(self):
        print('reconnect...')
        self._conn = None
        self._db = None

    def raw_gets(self, ids):
        if isinstance(ids, int):
            ids = [ids]
        ids = [i + 1 for i in ids]
        ids_ = construct_tuple(*ids)

        sql = f'select key,value from DICT where id in {ids_}'

        try:
            res = self.db.execute(sql)
            ress = res.fetchall()
        except sqlite3.DatabaseError as e:
            self.reconn()
            return self.raw_gets(ids)

        ress = [(key, de_nonewrap(pickle.loads(value))) for key, value in ress]
        return ress

    def gets(self, ids, return_type='raw'):
        ress = self.raw_gets(ids)

        if return_type in {'pandas', 'pd'}:
            import pandas as pd
            key = [i[0] for i in ress]
            value = [i[1] for i in ress]
            ress = pd.DataFrame([{'key': key}, {'value': value}])
        elif return_type in {'lkv'}:  # list with dict-wrapped result
            ress = [{'key': key, 'value': value} for key, value in ress]
        elif return_type in {'ldict'}:
            ress = [{key: value} for key, value in ress]
        elif return_type in {'dict'}:
            ress = {key: value for key, value in ress}
        elif return_type in {'raw', 'list'}:
            pass

        return ress

    def __getitem__(self, item):
        return self.gets(item, 'raw')
