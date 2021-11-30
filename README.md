# dbrecord

sqlite based kv database using for big data

## install

install by pipy

```shell
pip install dbrecord -U
```

install from github

```
pip install git+https://github.com/sailist/dbrecord
pip install git+https://github.com/sailist/dbrecord --user
```

# how to use

## PDict

create a pdict

```python
from dbrecord import PDict

dic = PDict('temp.sqlite', memory_size=100000, apply_memory=True, cache_size=50000)
# dic = PDict('temp.sqlite', apply_memory=False, cache_size=50000)
dic['a'] = 1
dic['b'] = 2
assert dic['a'] == 1
assert dic['b'] == 2
dic.flush()

dic_b = PDict('temp.sqlite', memory_size=100000, apply_memory=True, cache_size=50000)
assert dic['a'] == 1
assert dic['b'] == 2

# dic.setdefault('a', 2)

assert dic.gets('a', 'b') == dic['a', 'b'] == [1, 2]
assert dic['a'] == 1
dic_b.clear()  # remove the database file

```

## PList

```python
from dbrecord import PDict, PList

dic = PDict('temp.sqlite', memory_size=100000, apply_memory=True, cache_size=50000)
# dic = PDict('temp.sqlite', apply_memory=False, cache_size=50000)
dic['a'] = 1
dic['b'] = 2
assert dic['a'] == 1
assert dic['b'] == 2
dic.flush()

lis = PList('temp.sqlite')
assert lis.gets(range(10), return_type='raw') == [('a', 1), ('b', 2)]
```

# benchmark

see logs in [benchmark.md](./benchmark.log) or run

```shell
git clone https://github.com/sailist/dbrecord
cd dbrecord
cd example
python speed_test.py
```

# Reference

- https://gist.github.com/joseafga/ff798d340d79107ace14fd232abc4376

# TODO

- [ ] other dict methods like `update`, `clear`

# See also

- [lumo](https://github.com/pytorch-lumo/lumo)
- [pytorch.DataLoader](https://github.com/pytorch-lumo/pytorch.DataLoader)