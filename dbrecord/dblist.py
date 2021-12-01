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
        self.db_file = db_file

    def __getstate__(self):
        """Connetion object cannot be pickled, so we need to return None when __getstate__ be called. """
        return {'db_file': self.db_file, '_conn': None}

    def __getitem__(self, item):
        return self.gets(item, 'value')

    def __len__(self):
        from dbrecord.summary import count_table
        self.reconnect()
        return count_table(self.conn, 'DICT')

    @property
    def conn(self):
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_file)
        return self._conn

    def reconnect(self):
        if self._conn is not None:
            self._conn.close()
        self._conn = None

    def raw_gets(self, ids):
        if isinstance(ids, int):
            ids = [ids]
        ids = [i + 1 for i in ids]
        ids_ = construct_tuple(*ids)

        sql = f'select key,value from DICT where id in {ids_}'

        try:
            res = self.conn.execute(sql)
            ress = res.fetchall()
        except sqlite3.DatabaseError as e:
            self.reconnect()
            return self.raw_gets(ids)

        ress = [(key, de_nonewrap(pickle.loads(value))) for key, value in ress]
        return ress

    def gets(self, ids, return_type='value'):
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
        elif return_type in {'value'}:
            ress = [value for _, value in ress]
        elif return_type in {'raw'}:
            pass

        return ress
