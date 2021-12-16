import os

from dbrecord import PDict, NoneType


def test_disk():
    dic = PDict('./disk.sqlite', apply_memory=False)
    dic['a'] = 1
    assert 'a' in dic
    dic['b'] = None
    dic.flush()

    dic = PDict('./disk.sqlite', apply_memory=False)

    assert dic['b'] is None
    assert dic.gets('a', 'b') == [1, None]
    assert len(dic) == 2
    os.remove('./disk.sqlite')


def test_del():
    dic = PDict('./disk.sqlite', apply_memory=False)
    dic['a'] = 1
    assert 'a' in dic
    dic['b'] = None
    dic.flush()

    del dic['b']
    assert dic.get('b', default=2) == 2
    assert dic.gets('b') == [NoneType()]

    os.remove('./disk.sqlite')


def test_plist():
    dic = PDict('./disk.sqlite', apply_memory=False)
    dic['a'] = 1
    dic['b'] = None
    plist = dic.to_list()
    assert len(plist[range(10)]) == 2
    assert plist[range(10)] == [1, None]
    dic.flush()
    os.remove('./disk.sqlite')


def test_disk2():
    dic = PDict('./disk.sqlite', apply_memory=False)
    for i in range(50000):
        dic[f'{i}'] = i

    assert len(dic) == 50000

    dic = PDict('./disk.sqlite', apply_memory=False)
    assert len(dic) == 50000
    for i, val in enumerate(dic[[f'{i}' for i in range(50000)]]):
        assert i == val

    os.remove('./disk.sqlite')


def test_dic_imp():
    dic = PDict('./disk.sqlite', apply_memory=True)
    assert dic.setdefault('1', 1) == 1
    assert dic.setdefault('1', 2) == 1

    dic['2'] = 2
    assert list(dic.keys()) == ['1', '2']
    assert list(dic.values()) == [1, 2]
    assert list(dic.items()) == [('1', 1), ('2', 2)]

    os.remove('./disk.sqlite')


def test_compress():
    if os.path.exists('1.temp'):
        os.remove('1.temp')
    d = PDict('1.temp')
    d['1'] = 1
    d['1'] = 2
    d['1'] = 3
    d['2'] = 4
    d['3'] = 5
    d['2'] = 6
    d.flush()
    assert len(d) == 6
    d.compress()
    nd = PDict('1.temp')
    assert len(nd) == 3
    os.remove('./1.temp')
