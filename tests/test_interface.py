import copy
import sqlite3

from dbrecord import SqliteInterface, SliceView


def test_basic():
    dic = SqliteInterface('./disk.sqlite')
    assert not dic.is_dict and not dic.is_list
    dic.disk_set('1', 1)
    assert dic.is_dict and not dic.is_list
    assert dic.disk_key_gets(['1', '2']) == ({'1': 1}, {'2'})
    dic.disk_index_set(0, 2)
    assert dic.is_dict and not dic.is_list
    assert dic.disk_index_gets([0, 1]) == ({0: ('1', 2)}, {1})

    for i in range(10):
        dic.disk_append(i)
    assert dic.is_dict and dic.is_list
    assert len(dic) == 11

    assert (list([dic.deserilize(i) for i, in dic.iter_columns('value')])
            == [2] + list(range(10)))

    assert dic[0] == ({0: ('1', 2)}, set())
    assert dic['1'] == ({'1': 2}, set())
    assert isinstance(dic[0:1], SliceView)

    dic.clear()
    assert (len(dic) == 0)

    for i in range(10):
        dic.disk_set(str(i), i)

    for id, key, value in dic:
        assert str(id) == key
        assert id == value

    dic.close()

    for i in range(10):
        dic.disk_set(str(i), -i)
    try:
        exception = False
        dic.flush()
    except sqlite3.ProgrammingError:
        exception = True

    assert exception
    dic.reconnect()
    try:
        exception = True
        dic.flush()
    except sqlite3.ProgrammingError:
        exception = False

    assert exception
    assert len(dic) == 10

    cdic = copy.copy(dic)
    assert len(cdic) == 10
