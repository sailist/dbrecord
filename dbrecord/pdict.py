import warnings
from collections.abc import MutableMapping
from typing import Any, List

from .interface import SqliteInterface
from .utils import none, NoneType, ContainsWrap


class PDict(SqliteInterface, MutableMapping):
    def __setitem__(self, k: str, v):
        self.disk_set(k, v)

    def __delitem__(self, v):
        warnings.warn('Delete operation is not allowed and will have no effect.')

    def __getitem__(self, k: str):
        res, missing = self.disk_key_gets([k])
        if k in res:
            return res[k]
        raise KeyError(k)

    def __contains__(self, o: str) -> bool:
        return (self.get(o, ContainsWrap) != ContainsWrap)

    def __iter__(self):
        return self.keys()

    def tolist(self):
        from dbrecord import PList
        return PList(self._database, **self._config)

    def setdefault(self, key: str, default):
        res = self.cache_key_get(key)
        if res is not None:
            return self.deserilize(res[-1])

        res, missing = self.disk_key_gets([key])
        if len(res) == 0:
            self.disk_set(key, default)
            return default
        else:
            return res[key]

    def get(self, key: str, default: Any = none):
        res = self.cache_key_get(key)
        if res is None:
            res, missing = self.disk_key_gets([key])
            if key in res:
                return res[key]
            elif isinstance(default, NoneType):
                raise KeyError(key)
            else:
                return default
        else:
            return self.deserilize(res[-1])

    def gets(self, keys: List[str]):
        res, missing = self.disk_key_gets(keys)
        return res, missing

    def items(self):
        for key, value in self.iter_columns('key', 'value'):
            value = self.deserilize(value)
            yield key, value

    def keys(self):
        for key, in self.iter_columns('key'):
            yield key

    def values(self):
        for value, in self.iter_columns('value'):
            value = self.deserilize(value)
            yield value
