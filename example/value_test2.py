import os
import random
import time

from dbrecord import PDict
from joblib import hash

if __name__ == '__main__':
    dic = PDict('temp1.sqlite')
    dic['a'] = 1
    print(dic['a', 'b'])
    print(dic['a'])
    os.remove('temp1.sqlite')
