import os

from dbrecord import PDict
from dbrecord.idtrans import BatchIDSTrans

if __name__ == '__main__':
    dic = PDict('temp1.sqlite')

    print(dic.setdefault('1', 2))
    print(dic.setdefault('1', 3))
    print(dic.setdefault('3', 3))
    dic.flush()

    ids = BatchIDSTrans('temp1.sqlite')

    plist = dic.to_list()
    print(ids(range(10)))
    os.remove('temp1.sqlite')
