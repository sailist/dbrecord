import os

from dbrecord import PDict


def test_disk():
    dic = PDict('./disk.sqlite', apply_memory=False, apply_disk=True)
    dic['a'] = 1
    assert 'a' in dic
    dic['b'] = None
    dic.flush()

    dic = PDict('./disk.sqlite', apply_memory=False, apply_disk=True)

    assert dic['b'] is None
    assert len(dic) == 2
    os.remove('./disk.sqlite')


def test_plist():
    dic = PDict('./disk.sqlite', apply_memory=False, apply_disk=True)
    dic['a'] = 1
    dic['b'] = None
    plist = dic.to_list()
    assert len(plist[range(10)]) == 2
    assert plist[range(10)] == [('a', 1), ('b', None)]
    dic.flush()
    os.remove('./disk.sqlite')


def test_disk2():
    dic = PDict('./disk.sqlite', apply_memory=False, apply_disk=True)
    for i in range(50000):
        dic[f'{i}'] = i

    assert len(dic) == 50000

    dic = PDict('./disk.sqlite', apply_memory=False, apply_disk=True)
    assert len(dic) == 50000
    for i, val in enumerate(dic[[f'{i}' for i in range(50000)]]):
        assert i == val

    os.remove('./disk.sqlite')


def test_memory():
    dic = PDict('./disk.sqlite', apply_memory=True, apply_disk=False)
    dic['a'] = 1
    assert 'a' in dic
    dic['b'] = None

    assert dic['b'] is None
    assert len(dic) == 2
    dic = PDict('./disk.sqlite', apply_memory=True, apply_disk=False)
    assert len(dic) == 0

    assert not os.path.exists('./disk.sqlite')


def test_dic_imp():
    dic = PDict('./disk.sqlite', apply_memory=True, apply_disk=True)
    assert dic.setdefault('1', 1) == 1
    assert dic.setdefault('1', 2) == 1

    dic['2'] = 2
    assert list(dic.keys()) == ['1', '2']
    assert list(dic.values()) == [1, 2]
    assert list(dic.items()) == [('1', 1), ('2', 2)]

    os.remove('./disk.sqlite')
