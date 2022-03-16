import os
import sqlite3
import warnings
# try:
#     import quickle as pickle
# except:
#     warnings.warn('')
import pickle
import time
import threading
from typing import (overload, Tuple, Union, List, Any, Dict, Set)
from .serilize_backend import get_backend_dumps, get_backend_loads
from .summary import count_table
from .utils import create_database, inthash, NoneWrap, fetchall, construct_tuple, fetchmany, mark_in_database, \
    check_database_exists

default_config = {
    'cache_size': 500,
    'chunk_size': 1000,
}


class SqliteInterface:
    @overload
    def __init__(self,
                 database: str,
                 cache_size: int = 500, chunk_size: int = 1000,
                 backend='pickle',
                 backend_dump=None,
                 backend_load=None,
                 **kwargs):
        ...

    def __init__(self, database, **kwargs):
        super().__init__()
        self._database = database
        self._conn = None
        self._map_cache = {}
        self._index_cache = {}
        self._props = {}

        self._props['tid'] = threading.currentThread().ident

        self.loads_fn = kwargs.get('backend_load', 'pickle')
        self.dumps_fn = kwargs.get('backend_dump', 'pickle')
        if isinstance(self.loads_fn, str):
            self.loads_fn = get_backend_loads(self.loads_fn)

        if isinstance(self.dumps_fn, str):
            self.dumps_fn = get_backend_dumps(self.dumps_fn)

        config = default_config.copy()
        config.update(kwargs)
        self._config = config

    def __str__(self) -> str:
        return super().__str__()

    def __delitem__(self, v):
        warnings.warn('Delete operation is not allowed and will have no effect.')

    def __getitem__(self, item: Union[str, int, slice]):
        if isinstance(item, str):
            return self.disk_key_gets([item])
        elif isinstance(item, int):
            return self.disk_index_gets([item])
        elif isinstance(item, slice):
            return self.disk_slice_gets(item)
        raise NotImplementedError()

    def __len__(self) -> int:
        if len(self._map_cache) > 0:
            self.flush()
            if 'cached_len' in self._props:
                self._props.pop('cached_len')
        return self.cached_len

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_conn'] = None
        print('state')
        return state

    @property
    def tid(self):
        return self._props['tid']

    @property
    def is_list(self):
        if 'is_list' not in self._props:
            self._props['is_list'] = check_database_exists(self._database, 'list')
        return self._props['is_list']

    @property
    def is_dict(self):
        if 'is_dict' not in self._props:
            self._props['is_dict'] = check_database_exists(self._database, 'map')
        return self._props['is_dict']

    @property
    def cached_len(self):
        if 'cached_len' not in self._props:
            self._props['cached_len'] = count_table(self.conn, 'DICT')
        return self._props['cached_len']

    @property
    def cache_size(self):
        return self._config['cache_size']

    @property
    def chunk_size(self):
        return self._config['chunk_size']

    def _get_dump_value(self, value: Any):
        dump_value = self.dumps_fn(value)
        return dump_value

    def _get_hash_key(self, key: str):
        assert isinstance(key, str), f'key must be string type, got {type(key)}'
        hash_key = inthash(key)
        return hash_key

    def _get_dump_key_value_in_sql(self, key, value):
        return self._get_hash_key(key), key, self._get_dump_value(value)

    def deserilize(self, val):
        val = self.loads_fn(val)
        if isinstance(val, NoneWrap):
            return None
        return val

    def disk_index_set(self, index: int, value):
        assert isinstance(index, int) and index >= 0
        self.flush()
        if index >= len(self):
            raise IndexError(f'DB size is {len(self)}, but got {index}.')
        # key = str(len(self._map_cache))
        dump_value = self._get_dump_value(value)
        sql = f'UPDATE DICT set  VALUE = ? where id = ?;'
        self.conn.execute(sql, (dump_value, index + 1))
        self.conn.commit()

    def disk_set(self, key, value):
        self._map_cache[key] = self._get_dump_key_value_in_sql(key, value)
        self._check_flush()
        if not self.is_dict:
            mark_in_database(self._database, 'map')
            self._props.pop('is_dict')

    def disk_append(self, value):
        key = f"{time.time_ns()}"
        self._map_cache[key] = self._get_dump_key_value_in_sql(key, value)
        self._check_flush()
        if not self.is_list:
            mark_in_database(self._database, 'list')
            self._props.pop('is_list')

    def _check_flush(self):
        if len(self._map_cache) > self.cache_size:
            self.flush()

    def flush(self):
        if len(self._map_cache) == 0:
            return

        sql = f'insert or IGNORE into DICT (INTHASH, KEY, VALUE) values (?,?,?);'

        print(os.getpid(), self.tid, threading.current_thread().ident)
        self.conn.executemany(sql, list(self._map_cache.values()))
        self.conn.commit()
        self._map_cache.clear()

    def cache_key_get(self, key):
        return self._map_cache.get(key, None)

    def disk_key_gets(self, keys: Union[List[str], Set[str], Tuple[str]]) -> Tuple[Dict, Set]:
        self.flush()
        keys = set(keys)
        hash_key = {self._get_hash_key(key) for key in keys}

        hash_key_ = construct_tuple(*hash_key)
        dump_key_ = construct_tuple(*keys)

        sql = f'select key, value from DICT where inthash in {hash_key_} and key in {dump_key_}'
        res = fetchall(self.conn, sql)

        res = {key: self.deserilize(value) for key, value in res}

        missing = {key for key in keys if key not in res}

        return res, missing

    def disk_slice_gets(self, sl: slice):
        self.flush()
        return SliceView(self, sl)

    def disk_index_gets(self, indexs: List[int]):
        self.flush()
        indexs = set(indexs)

        indexs_ = construct_tuple(*indexs)

        sql = f'select id, key, value from DICT where (id-1) in {indexs_}'
        res = fetchall(self.conn, sql)

        def _loads(val):
            val = self.loads_fn(val)
            if isinstance(val, NoneWrap):
                return None
            return val

        res = {rid - 1: (key, _loads(value)) for rid, key, value in res}

        missing = {idx for idx in indexs if idx not in res}

        return res, missing

    @property
    def conn(self):
        if self._conn is None:
            self._conn = create_database(self._database)
        return self._conn

    def reconnect(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except sqlite3.ProgrammingError:
                pass
        self._conn = None

    def clear(self):
        self.conn.close()
        self._conn = None
        self._props.clear()

        os.remove(self._database)

    def close(self):
        self.conn.close()
        self._props.clear()

    def __iter__(self):
        self.flush()
        for rid, key, value in self.iter_columns('id', 'key', 'value'):
            value = self.deserilize(value)
            yield rid - 1, key, value

    def iter_columns(self, *columns):
        self.flush()
        columns = ', '.join(columns)
        for line in fetchmany(self.conn, f'select {columns} from DICT', self.chunk_size):
            yield line


class SliceView:
    def __init__(self, db: SqliteInterface, sl: slice):
        self.db = db
        self.sl = sl
        self.start = self.sl.start if self.sl.start is not None else 0
        self.stop = self.sl.stop if self.sl.stop is not None else len(self.db)
        self.step = self.sl.step if self.sl.step is not None else 1

    def __len__(self):
        return (self.stop - self.start) // self.step

    def __getitem__(self, item: Union[int, slice]):
        if isinstance(item, int):
            offset_index = range(len(self.db))[self.sl][item]
            return self.db[offset_index]
        elif isinstance(item, slice):
            return SliceView(self.db, item)
        raise NotImplementedError()

    def __iter__(self):
        for idx in range(self.start, self.stop, self.step):
            yield self.db[idx]

    def __str__(self):
        return f"SliceView({self.db},{self.sl})"

    def tolist(self):
        return list(self)
