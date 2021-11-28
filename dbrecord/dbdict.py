import os
import pickle
import sqlite3
import time
from collections import OrderedDict
from typing import Any

from utils import ContainsWrap, NoneType, NoneWrap, none, contrain, inthash
from .idtrans import BatchIDSTrans
from .utils import construct_tuple


def create_database(database):
    conn = sqlite3.connect(database)
    sql = f'''CREATE TABLE DICT
                       (ID INTEGER PRIMARY KEY  AUTOINCREMENT,
                       INTHASH       INTEGER  NOT NULL,
                       KEY VARCHAR NOT NULL,
                       VALUE VARCHAR  NOT NULL
                       );
                       CREATE INDEX type on DICT (INTHASH);'''
    try:
        conn.executescript(sql)
    except:
        pass
    return conn


class PDict():
    def __init__(self,
                 database,
                 apply_memory=False, memory_size=100000,
                 apply_disk=True, cache_size=10000, cache_time=-1,
                 verbose=0):
        assert cache_size > 0 or cache_time > 0
        assert apply_memory or apply_disk
        self.apply_memory = apply_memory
        self.apply_disk = apply_disk

        if apply_disk:
            self.database = database
            self.conn = create_database(database)
            self.cache_size = cache_size
            self.cache_time = cache_time
            self.cache = []
        if apply_memory:
            self.memory = OrderedDict()
            self.memory_size = memory_size if self.apply_disk else -1
            self.queue = list()

        self.last_flush_time = time.time()
        self.verbose = verbose

    def __setitem__(self, key, value):
        hash_key, dump_key, dump_value = self._get_dump_key_value_in_sql(key, value)
        if self.apply_memory:
            self._memory_store(dump_key, value)
        if self.apply_disk:
            self._disk_store(hash_key, dump_key, dump_value)

    def __getitem__(self, key):
        if isinstance(key, str):
            return self.get(key)
        elif isinstance(key, (tuple, list)):
            return self.gets(*key)
        else:
            raise NotImplementedError()

    def __contains__(self, key):
        res = self.get(key, ContainsWrap())
        return not isinstance(res, ContainsWrap)

    def __len__(self):
        from dbrecord.summary import count_table
        self.flush()
        if self.apply_disk:
            return count_table(self.conn, 'DICT')
        else:
            return len(self.memory)

    def __iter__(self):
        yield from self.keys()

    def execute(self, sql):
        try:
            return self.conn.execute(sql)
        except sqlite3.DatabaseError:
            self.reconnect()
            self.execute(sql)

    def reconnect(self):
        self.conn.close()
        self.conn = create_database(self.database)

    def _get_dump_key_value_in_sql(self, key, value=none):
        assert isinstance(key, str), f'key must be string type, got {type(key)}'

        hash_key = inthash(key)
        dump_key = str(key)
        dump_value = None
        if not isinstance(value, NoneType):
            if value is None:
                value = NoneWrap()
            dump_value = pickle.dumps(value)
        return hash_key, dump_key, dump_value

    def _memory_store(self, dump_key, value):
        if dump_key in self.memory:
            return
        self.memory[dump_key] = value
        self.queue.append(dump_key)
        if len(self.queue) > self.memory_size and self.memory_size > 0:
            key = self.queue.pop(0)
            self.memory.pop(key)

    def _memory_get(self, dump_key):
        if dump_key in self.memory:
            return self.memory[dump_key]
        return none

    def _check_flush(self):
        return (len(self.cache) >= self.cache_size and self.cache_size > 0) or (
                time.time() - self.last_flush_time > self.cache_time and self.cache_time > 0)

    def _disk_store(self, hash_key, dump_key, dump_value):
        self.cache.append([hash_key, dump_key, dump_value])
        if self._check_flush():
            # self.print(f'flush {len(self.cache)}')
            self.flush()

    def _disk_get(self, hash_key, dump_key):
        sql = f'select value from DICT where inthash = {hash_key} and key = "{dump_key}"'
        res = self.execute(sql).fetchall()
        if len(res) > 0:
            res = res[-1]
        else:
            res = None

        if res is None:
            value = NoneType()
        else:
            value = pickle.loads(res[0])
            if isinstance(value, NoneWrap):
                value = None
        return value

    def _disk_gets(self, hash_key, dump_key):
        hash_key_ = construct_tuple(*hash_key)
        dump_key_ = construct_tuple(*dump_key)

        sql = f'select key, value from DICT where inthash in {hash_key_} and key in {dump_key_}'

        res = self.execute(sql).fetchall()

        def _loads(val):
            if isinstance(val, NoneWrap):
                return None
            return pickle.loads(val)

        res = {key: _loads(value) for key, value in res}

        values = []
        for key in dump_key:
            values.append(res.get(key, none))

        return values

    def flush(self):
        if not self.apply_disk:
            return
        if len(self.cache) == 0:
            return
        sql = f'insert into DICT (INTHASH, KEY, VALUE) values (?,?,?);'
        try:
            self.conn.executemany(sql, self.cache)
            self.conn.commit()
            self.cache.clear()
            self.last_flush_time = time.time()
        except sqlite3.DatabaseError:
            self.reconnect()
            self.flush()

    def gets(self, *keys: str):
        """
        :param keys:
        :return: return a list which has the same elements as keys
            if key exists in dict, the corrsponding elements will be its value,
            else, will be an instance of class dbrecord.dbdict.NoneType

        """
        self.flush()
        values = [NoneType() for _ in range(len(keys))]
        chunk_keys = [self._get_dump_key_value_in_sql(key) for key in keys]
        if self.apply_memory:
            for i, key in enumerate(keys):
                hash_key, dump_key, _ = chunk_keys[i]
                values[i] = self._memory_get(dump_key)
        if self.apply_disk:
            none_ids = [i for i, value in enumerate(values) if isinstance(value, NoneType)]
            hash_keys = [chunk_keys[i][0] for i in none_ids]
            dump_keys = [chunk_keys[i][1] for i in none_ids]
            disk_values = self._disk_gets(hash_keys, dump_keys)
            for i, disk_value in enumerate(disk_values):
                reid = none_ids[i]
                values[reid] = disk_value

        values = [i for i in values]
        return values

    def get(self, key, default: Any = none):
        self.flush()
        hash_key, dump_key, _ = self._get_dump_key_value_in_sql(key)
        if self.apply_memory:
            value = self._memory_get(dump_key)
            if not isinstance(value, NoneType):
                return value
        if self.apply_disk:
            value = self._disk_get(hash_key, dump_key)
            if not isinstance(value, NoneType):
                if self.apply_memory:
                    self._memory_store(dump_key, value)
                return value

        value = default
        if not isinstance(default, NoneType):
            return value

        raise KeyError(key)

    def keys(self):
        if self.apply_disk:
            self.flush()
            cursor = self.execute('select key from DICT')
            res = cursor.fetchmany(500)
            while len(res) > 0:
                for key, in res:
                    yield key
                res = cursor.fetchmany(500)
        else:
            return self.memory.keys()

    def values(self):
        if self.apply_disk:
            self.flush()
            cursor = self.execute('select value from DICT')
            res = cursor.fetchmany(500)
            while len(res) > 0:
                for value, in res:
                    yield pickle.loads(value)
                res = cursor.fetchmany(500)
        else:
            return self.memory.values()

    def items(self):
        if self.apply_disk:
            self.flush()
            cursor = self.execute('select key, value from DICT')
            res = cursor.fetchmany(500)
            while len(res) > 0:
                for key, value in res:
                    yield key, pickle.loads(value)
                res = cursor.fetchmany(500)
        else:
            return self.memory.items()

    def update(self, E, **F):
        raise NotImplementedError()

    def clear(self):
        if self.apply_disk:
            self.conn.close()
            os.remove(self.database)
        if self.apply_memory:
            self.memory.clear()
            self.queue.clear()

    def setdefault(self, k, value):
        res = self.get(k, contrain)
        if isinstance(res, ContainsWrap):
            self[k] = value
            return value
        return res

    def print(self, *info, verbose=1):
        print(*info)

    def to_list(self):
        assert self.apply_disk
        self.flush()
        plist = BatchIDSTrans(self.database)
        return plist
