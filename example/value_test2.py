import os
import random
import time

from dbrecord import PDict
from joblib import hash

if __name__ == '__main__':
    dic = PDict('temp1.sqlite')
    dic['a'] = 1
    print(dic['a', 'b'])
    dic.gets()
    # plist = dic.to_list()
    # print(plist(1))
    os.remove('temp1.sqlite')
