from dbrecord import PDict

import os
from tqdm import tqdm


def main():
    if os.path.exists('temp.sqlite'):
        os.remove('temp.sqlite')
    dic = PDict('temp.sqlite', cache_size=10000)
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

    os.remove('temp.sqlite')

if __name__ == '__main__':
    main()
