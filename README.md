# dbrecord

dbrecord provides a light-weight, sqlite based, persistent list and dict.

## install

```shell
pip install dbrecord -U
```

# how to use

## PDict

create a pdict

```python
from dbrecord import PDict

dic = PDict('./disk.sqlite')
for i in range(5000):
    dic[f'{i}'] = i
assert len(dic) == 5000

dic2 = PDict('./disk.sqlite')
assert len(dic2) == 5000

res, missing = dic.gets([f'{i}' for i in range(5001)])  # type: dict, set
assert len(missing) == 1
print(missing)
for i in range(5000):
    assert res[f'{i}'] == i
```

## PList

```python
from dbrecord import PList

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
```

# With other backend

```python


```

# benchmark

see logs in [benchmark.md](./benchmark.md) or run

```shell
git clone https://github.com/sailist/dbrecord
cd dbrecord
cd example
python benchmark.py
```

# Reference

- https://gist.github.com/joseafga/ff798d340d79107ace14fd232abc4376
- [sqlitedict](https://github.com/piskvorky/sqlitedict)