from dbrecord import PDict

import os
from tqdm import tqdm


def main():
    if os.path.exists('temp.sqlite'):
        os.remove('temp.sqlite')
    dic = PDict('temp.sqlite', cache_size=500)
    for i in tqdm(range(50000)):
        dic[str(i)] = i

    dic.flush()

    from concurrent.futures import ProcessPoolExecutor, as_completed

    executor = ProcessPoolExecutor(5)
    res = []
    for i in range(500):
        res.append(executor.submit(dic.get, str(i)))
    for r in as_completed(res):
        print(r.result())

    from concurrent.futures import ThreadPoolExecutor, as_completed

    executor = ThreadPoolExecutor(5)
    for i in range(1000):
        res.append(executor.submit(dic.__setitem__, f's{i}', str(i)))
    for r in as_completed(res):
        pass

    print(len(dic))
    dic.flush()

    res = []
    for i in range(500):
        res.append(executor.submit(dic.get, str(i)))
    for r in as_completed(res):
        pass

    os.remove('temp.sqlite')


if __name__ == '__main__':
    main()
