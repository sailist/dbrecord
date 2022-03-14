from dbrecord import PList


def test_basic():
    lis = PList('./disk.sqlite')
    for i in range(20):
        lis.append(i)

    lsl = lis[2:16:2]
    assert list(lsl) == list(range(20)[2:16:2])
    assert list(lsl[0:20]) == list(range(20))
    assert lsl[0] == lis[2]
    assert len(lsl) == (14 // 2)

    try:
        _ = lsl[8]
        assert False
    except IndexError:
        pass


    for i, j in zip(lis, range(20)):
        assert i == j

    res, missing = lis.select([1, 1, 2, 3])
    assert (res, missing) == ([1, 1, 2, 3], set())

    try:
        _ = lis[100]
        assert False
    except IndexError:
        pass
