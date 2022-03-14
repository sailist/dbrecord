from dbrecord import PDict


def test_disk():
    dic = PDict('./disk.sqlite')
    dic['a'] = 1
    assert 'a' in dic
    dic['b'] = None
    dic.flush()

    dic = PDict('./disk.sqlite')

    assert dic['b'] is None
    assert len(dic) == 2


def test_none():
    dic = PDict('./disk.sqlite')
    dic['b'] = None
    dic.flush()

    assert dic.get('b', default=2) is None
    assert dic.get('c', default=2) == 2
    res, missing = dic.gets(['b', 'll'])
    assert isinstance(res, dict)
    assert 'b' in res
    assert res['b'] is None

    assert 'll' in missing
    assert isinstance(missing, set)


def test_disk2():
    dic = PDict('./disk.sqlite')
    for i in range(5000):
        dic[f'{i}'] = i

    assert len(dic) == 5000

    dic = PDict('./disk.sqlite')
    assert len(dic) == 5000
    res, missing = dic.gets([f'{i}' for i in range(5000)])
    assert len(missing) == 0
    for i in range(5000):
        assert res[f'{i}'] == i


def test_dic():
    dic = PDict('./disk.sqlite', apply_memory=True)
    assert dic.setdefault('1', 1) == 1
    assert dic.setdefault('1', 2) == 1
    assert dic.get('1', 3) == 1
    assert dic.pop('1') == 1
    assert '1' in dic
    assert dic.is_dict and not dic.is_list

    dic['2'] = 2

    for i, k in enumerate(dic, start=1):
        assert k == str(i)

    assert list(dic.keys()) == ['1', '2']
    assert list(dic.values()) == [1, 2]
    assert list(dic.items()) == [('1', 1), ('2', 2)]
