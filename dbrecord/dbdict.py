import os
import pickle
import sqlite3
import time
from collections import OrderedDict
from collections.abc import MutableMapping
from typing import Any

from .dblist import PList
from .utils import ContainsWrap, NoneType, NoneWrap, none, contain, inthash, DelType
from .utils import construct_tuple


def create_database(database: str):
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


class PDict(MutableMapping):
    """
    dict object that
    """

    def __init__(self,
                 database,
                 apply_memory=False, memory_size: int = 100000,
                 cache_size=10000, cache_time=-1,
                 verbose=0, chunk_size=500):
        assert cache_size > 0 or cache_time > 0
        self.apply_memory = apply_memory

        self.database = database
        self._conn = None
        self.cache_size = cache_size
        self.cache_time = cache_time
        self.cache = []
        self._cache_len = None
        self.chunk_size = chunk_size
        if apply_memory:
            assert memory_size >= 0
            self.memory = OrderedDict()
            self.memory_size = memory_size
            self.queue = list()

        self.last_flush_time = time.time()
        self.verbose = verbose

    def __getstate__(self):
        state = self.__dict__
        state['_conn'] = None
        return state

    def __setitem__(self, key, value):
        hash_key, dump_key, dump_value = self._get_dump_key_value_in_sql(key, value)
        if self.apply_memory:
            self._memory_store(dump_key, value)
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
        return not isinstance(res, (ContainsWrap))

    def __len__(self):
        if len(self.cache) == 0 and self._cache_len is not None:
            return self._cache_len
        self.flush()
        from dbrecord.summary import count_table
        self._cache_len = count_table(self.conn, 'DICT')
        return self._cache_len

    def __iter__(self):
        yield from self.keys()

    def __delitem__(self, k) -> None:
        self[k] = ContainsWrap()
        if self.apply_memory:
            self._memory_store(k, ContainsWrap())

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
        self.flush()
        sql = f'select value from DICT where inthash = {hash_key} and key = "{dump_key}"'
        res = self._fetchall(sql)
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
        self.flush()

        hash_key_ = construct_tuple(*hash_key)
        dump_key_ = construct_tuple(*dump_key)

        sql = f'select key, value from DICT where inthash in {hash_key_} and key in {dump_key_}'

        res = self._fetchall(sql)

        def _loads(val):
            val = pickle.loads(val)
            if isinstance(val, NoneWrap):
                return None
            return val

        res = {key: _loads(value) for key, value in res}

        values = []
        for key in dump_key:
            values.append(res.get(key, none))

        return values

    def _fetchall(self, sql):
        try:
            return self.conn.execute(sql).fetchall()
        except sqlite3.DatabaseError:
            self.reconnect()
            return self._fetchall(sql)

    def _fetchmany(self, sql, chunksize):
        try:
            cursor = self.conn.execute(sql)
            res = cursor.fetchmany(chunksize)
            while len(res) > 0:
                yield res
                res = cursor.fetchmany(chunksize)
        except sqlite3.DatabaseError:
            self.reconnect()
            yield from self._fetchmany(sql, chunksize)

    @property
    def conn(self):
        if self._conn is None:
            self._conn = create_database(self.database)
        return self._conn

    def execute(self, sql):
        try:
            return self.conn.execute(sql)
        except sqlite3.DatabaseError:
            self.reconnect()
            return self.execute(sql)

    def reconnect(self):
        if self._conn is not None:
            self._conn.close()
        self._conn = None

    def flush(self):
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

    def close(self):
        """close the connection"""
        self.flush()
        self.conn.close()
        self._conn = None

    def gets(self, *keys: str):
        """
        :param keys:
        :return: return a list which has the same elements as keys
            if key exists in dict, the corrsponding elements will be its value,
            else, will be an instance of class dbrecord.dbdict.NoneType

        """
        values = [NoneType() for _ in range(len(keys))]
        chunk_keys = [self._get_dump_key_value_in_sql(key) for key in keys]
        if self.apply_memory:
            for i, key in enumerate(keys):
                hash_key, dump_key, _ = chunk_keys[i]
                values[i] = self._memory_get(dump_key)

        none_ids = [i for i, value in enumerate(values) if isinstance(value, (NoneType, ContainsWrap))]
        hash_keys = [chunk_keys[i][0] for i in none_ids]
        dump_keys = [chunk_keys[i][1] for i in none_ids]
        disk_values = self._disk_gets(hash_keys, dump_keys)
        for i, disk_value in enumerate(disk_values):
            if isinstance(disk_value, ContainsWrap):
                continue
            reid = none_ids[i]
            values[reid] = disk_value

        values = [i for i in values]
        return values

    def get(self, key, default: Any = none):
        hash_key, dump_key, _ = self._get_dump_key_value_in_sql(key)
        if self.apply_memory:
            value = self._memory_get(dump_key)
            if not isinstance(value, (NoneType, ContainsWrap)):
                return value

        value = self._disk_get(hash_key, dump_key)
        if not isinstance(value, (NoneType, ContainsWrap)):
            if self.apply_memory:
                self._memory_store(dump_key, value)
            return value

        value = default
        if not isinstance(default, NoneType):
            return value

        raise KeyError(key)

    def keys(self):
        self.flush()
        for res in self._fetchmany('select key from DICT', self.chunk_size):
            for key, in res:
                yield key

    def values(self):
        self.flush()
        for res in self._fetchmany('select value from DICT', self.chunk_size):
            for value, in res:
                yield pickle.loads(value)

    def items(self):
        self.flush()
        cursor = self.execute('select key, value from DICT')
        res = cursor.fetchmany(self.chunk_size)
        while len(res) > 0:
            for key, value in res:
                yield key, pickle.loads(value)
            res = cursor.fetchmany(self.chunk_size)

    def update(self, E, **F):
        raise NotImplementedError()

    def clear(self):
        """
        Clear cache, memory bank, and remove the database file.
        :return:
        """
        self._cache_len = None
        self.cache.clear()
        self.conn.close()
        os.remove(self.database)
        if self.apply_memory:
            self.memory.clear()
            self.queue.clear()

    def setdefault(self, k, default):
        res = self.get(k, contain)
        if isinstance(res, ContainsWrap):
            # Value will become default value if get method returns ContainWrap instance.
            self[k] = default
            res = default
        return res

    def to_list(self):
        """

        :return: a PList instance
        """
        self.flush()
        plist = PList(self.database)
        return plist

    def compress(self):
        self.flush()
        new_database = f"{self.database}.compress"

        new_dic = PDict(new_database, apply_memory=False, cache_size=max(len(self) // 10, 500))

        for key_chunk in self._fetchmany('select key from DICT group by key', chunksize=self.chunk_size):
            key_chunk = [i[0] for i in key_chunk]
            for k, v in zip(key_chunk, self.gets(*key_chunk)):
                if isinstance(v, NoneType):
                    continue
                new_dic[k] = v
        new_dic.flush()
        self.clear()
        new_dic.close()
        import shutil
        shutil.move(new_database, self.database)
