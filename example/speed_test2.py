import os
import random
import time

from dbrecord import PDict
from joblib import hash


def save_and_load(test_size, chunk=1):
    start = time.time()
    dic = PDict('./temp.sqlite', apply_memory=False, cache_size=test_size // 2)
    for i in range(test_size):
        dic[str(i)] = hash(i + 1)
    dic.flush()

    mid = time.time()
    print(f'save {test_size} taske', mid - start)
    print(f'per save', (mid - start) / test_size)
    dic_size = len(dic)
    print('dic size', dic_size)

    import numpy as np
    for i in range(test_size // chunk):
        ids = np.random.randint(1, dic_size - 1, chunk)
        dic[[str(i.item()) for i in ids]]

    end = time.time()
    print(f'load {test_size} by ids with chunk {chunk} takse', end - mid)
    print(f'per load ', (end - mid) / test_size)
    os.remove('temp.sqlite')
    print()


if __name__ == '__main__':
    save_and_load(test_size=500)
    save_and_load(test_size=500, chunk=10)
    save_and_load(test_size=5000)
    save_and_load(test_size=5000, chunk=10)
    save_and_load(test_size=50000)
    save_and_load(test_size=50000, chunk=10)
    save_and_load(test_size=500000)
    save_and_load(test_size=500000, chunk=10)
    save_and_load(test_size=1000000)
    save_and_load(test_size=1000000, chunk=10)
