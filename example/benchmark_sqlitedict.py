import random
import time

from sqlitedict import SqliteDict
import os


def save_and_load(test_size, chunk=1):
    if os.path.exists('temp.sqlite'):
        os.remove('temp.sqlite')

    start = time.time()
    dic = SqliteDict("temp.sqlite")
    for i in range(test_size):
        dic[str(i)] = hash(i + 1)
    dic.commit()

    mid = time.time()
    print(f'save {test_size} taske', mid - start)
    print(f'per save', (mid - start) / test_size)
    dic_size = len(dic)
    print('dic size', dic_size)

    for i in range(test_size):
        pid = random.randint(1, dic_size - 1)
        assert dic[str(pid)] == hash(pid + 1)

    mid2 = time.time()
    print(f'load {test_size} by k-v takse', mid2 - mid)
    print(f'per load ', (mid2 - mid) / test_size)

    # plist = dic.tolist()
    import numpy as np
    for i in range(chunk):
        ids = np.random.randint(1, dic_size - 1, test_size // chunk).tolist()
        [dic.get(str(i)) for i in ids]

    end = time.time()
    print(f'load {test_size} by ids with chunk {chunk} takse', end - mid2)
    print(f'per load ', (end - mid2) / test_size)
    os.remove('temp.sqlite')
    print()


if __name__ == '__main__':
    save_and_load(test_size=500)
    save_and_load(test_size=500, chunk=10)
    save_and_load(test_size=5000)
    save_and_load(test_size=5000, chunk=10)
    save_and_load(test_size=50000)
    save_and_load(test_size=50000, chunk=10)
    # save_and_load(test_size=500000)
    # save_and_load(test_size=500000, chunk=10)
    # save_and_load(test_size=1000000)
    # save_and_load(test_size=1000000, chunk=10)
