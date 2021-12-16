import pickle
import sqlite3
from .utils import construct_tuple, NoneWrap


def window(seq, n: int, strid: int = 1, drop_last: bool = False):
    for i in range(0, len(seq), strid):
        res = seq[i:i + n]
        if drop_last and len(res) < n:
            break
        yield res


def de_nonewrap(value):
    if isinstance(value, NoneWrap):
        return None
    return value


class PList:
    def __init__(self, db_file, chunk_size=500,
                 multi_thread=False, num_workers=8):
        self._conn = None
        self.db_file = db_file
        self.chunk_size = chunk_size

        # TODO
        self.multi_thread = multi_thread
        self.num_workers = num_workers

    def __getstate__(self):
        """Connetion object cannot be pickled, so we need to return None when __getstate__ be called. """
        return {'db_file': self.db_file, '_conn': None}

    def __getitem__(self, item):
        return self.gets(item, 'value')

    def iter_by_type(self, return_type):
        for ids in window(range(len(self)), self.chunk_size, self.chunk_size):
            yield from self.gets(list(ids), return_type=return_type)

    def __iter__(self):
        yield from self.iter_by_type('value')

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
