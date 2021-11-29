import os

from dbrecord import PDict, PList

if __name__ == '__main__':
    dic = PDict('temp1.sqlite')

    print(dic.setdefault('1', 2))
    print(dic.setdefault('1', 3))
    print(dic.setdefault('3', 3))
    dic.flush()

    ids = PList('temp1.sqlite')
    print(len(ids))
    plist = dic.to_list()
    print(ids[range(10)])
    os.remove('temp1.sqlite')
