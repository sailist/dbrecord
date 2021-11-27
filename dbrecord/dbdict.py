import json
import time
from contextlib import contextmanager
import sqlite3
from joblib import hash
import pickle
from typing import Any
from collections import OrderedDict

import os


def inthash(item):
    return int(hash(item)[:8], 16)


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


class ContainsWrap():
    pass


class NoneType():
    pass


class NoneWrap():
    pass


none = NoneType()


class PDict():
    def __init__(self, database,
                 memory_size=100000,
                 apply_memory=False,
                 apply_disk=True,
                 cache_size=10000, cache_time=-1, verbose=0):
        assert cache_size > 0 or cache_time > 0
        assert apply_memory or apply_disk
        self.apply_memory = apply_memory
        self.apply_disk = apply_disk

        if apply_disk:
            self.database = database
            self.conn = create_database(database)
            self.cache_size = cache_size
            self.cache_time = cache_time
            self.last_flush_time = time.time()
            self.cache = []
        if apply_memory:
            self.memory = OrderedDict()
            self.memory_size = memory_size
            self.queue = list()

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
        return count_table(self.conn, 'DICT')

    def __iter__(self):
        pass

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
        if len(self.queue) > self.memory_size:
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
        res = self.conn.execute(sql).fetchall()
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
        hash_key_ = self._construct_tuple(*hash_key)
        dump_key_ = self._construct_tuple(*dump_key)

        sql = f'select key, value from DICT where inthash in {hash_key_} and key in {dump_key_}'
        res = self.conn.execute(sql).fetchall()

        def _loads(val):
            if isinstance(val, NoneWrap):
                return None
            return pickle.loads(val)

        res = {key: _loads(value) for key, value in res}

        values = []
        for key in dump_key:
            values.append(res.get(key, none))

        return values

    def _construct_tuple(self, *dump_keys):
        return json.dumps(dump_keys).replace('[', '(').replace(']', ')')

    def flush(self):
        if len(self.cache) == 0:
            return
        sql = f'insert into DICT (INTHASH, KEY, VALUE) values (?,?,?);'
        self.conn.executemany(sql, self.cache)
        self.conn.commit()
        self.cache.clear()

    @contextmanager
    def immediately(self):
        yield
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
        pass

    def values(self):
        pass

    def update(self, E, **F):
        raise NotImplementedError()

    def clear(self):
        self.conn.close()
        os.remove(self.database)
        self.memory.clear()
        self.queue.clear()
        raise NotImplementedError()

    def setdefault(self, k, value):
        pass

    def print(self, *info, verbose=1):
        print(*info)

    def to_list(self):
        from dbrecord.idtrans import BatchIDSTrans
        self.flush()
        plist = BatchIDSTrans(self.database, 'DICT', ['id', 'inthash', 'key', 'value'])
        return plist
