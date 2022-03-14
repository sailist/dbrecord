import warnings
from collections.abc import MutableSequence
from typing import Iterable, Any, Union, List

from .interface import SqliteInterface


class PList(SqliteInterface, MutableSequence):

    def todict(self):
        from dbrecord import PDict
        return PDict(self._database, **self._config)

    def reverse(self) -> None:
        warnings.warn('Reverse is not allowed and has not effect.')

    def insert(self, index: int, value) -> None:
        warnings.warn('Insert is not allowed and has not effect.')

    def append(self, value) -> None:
        self.disk_append(value)

    def extend(self, values: Iterable) -> None:
        [self.disk_append(v) for v in values]

    def __setitem__(self, k: int, v):
        self.disk_index_set(k, v)

    def remove(self, value) -> None:
        warnings.warn('Remove operation is not allowed and will have no effect.')

    def index(self, value: Any, start=None, stop=None) -> int:
        warnings.warn('Index operation is not allowed and will always return -1.')
        return -1

    def pop(self, index: int):
        warnings.warn('Pop operation will only return the index.')
        return self[index]

    def __delitem__(self, v):
        self.remove(v)

    def __getitem__(self, idx: Union[int, slice]):
        if isinstance(idx, int):
            res, missing = self.disk_index_gets([idx])
            if idx in res:
                return res[idx][-1]
            raise IndexError(idx)
        elif isinstance(idx, slice):
            return self.disk_slice_gets(idx)

    def __iter__(self):
        for value, in self.iter_columns('value'):
            value = self.deserilize(value)
            yield value

    def select(self, indexs: List[int]):
        res, missing = self.disk_index_gets(indexs)

        values = [res[idx][1] for idx in indexs if idx in res]
        return values, missing
