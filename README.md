# dbrecord

sqlite based kv database using for big data

## install

install from github

```
pip install git+https://github.com/sailist/dbrecord
```

## how to use

create a pdict

```python
from dbrecord import PDict

dic = PDict('temp.sqlite', memory_size=100000, apply_memory=True, cache_size=50000)
# dic = PDict('temp.sqlite', apply_memory=False, cache_size=50000)
```

save/load

```python
# use it as 
dic['a'] = 1
dic['b'] = 2
print(dic['a'])
print(dic['b'])
```

## benchmark

see logs in [benchmark.md](./benchmark.md) or

```shell
git clone https://github.com/sailist/dbrecord
cd dbrecord
cd example
python speed_test.py
```


# TODO

 - [ ] other dict methods like `update`, `clear`
 - [ ] dataset operations
 - [ ] document
