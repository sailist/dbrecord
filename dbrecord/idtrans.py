import pickle
import sqlite3

from .utils import construct_tuple


class IDTrans:
    def __init__(self, db_file):
        self._conn = None
        self._db = None
        self.db_file = db_file

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
        self._conn = None
        self._db = None

    def raw_gets(self,ids):
        if isinstance(ids, int):
            ids = [ids]
        ids_ = construct_tuple(*ids)

        sql = f'select key,value from DICT where id in {ids_}'

        try:
            res = self.db.execute(sql)
        except sqlite3.DatabaseError:
            self.reconn()
            return self(ids)

        ress = res.fetchall()
        return ress
    def gets(self, ids):
        if isinstance(ids, int):
            ids = [ids]
        ids_ = construct_tuple(*ids)

        sql = f'select key,value from DICT where id in {ids_}'

        try:
            res = self.db.execute(sql)
        except sqlite3.DatabaseError:
            self.reconn()
            return self(ids)

        ress = res.fetchall()

        ress = [{key: pickle.loads(value)} for key, value in ress]

        return ress

    def __getitem__(self, item):
        return self.gets(item)


class BatchIDSTrans(IDTrans):
    def __call__(self, ids):
        if isinstance(ids, int):
            ids = [ids]
        ids_ = construct_tuple(*ids)

        sql = f'select key,value from DICT where id in {ids_}'

        try:
            res = self.db.execute(sql)
        except sqlite3.DatabaseError:
            self.reconn()
            return self(ids)

        ress = res.fetchall()

        ress = [{key: pickle.loads(value)} for key, value in ress]

        return ress
