import os

import pytest


@pytest.fixture(scope='function', autouse=True)
def db():
    if os.path.exists('./disk.sqlite'):
        os.remove('./disk.sqlite')
    try:
        yield
    finally:
        if os.path.exists('./disk.sqlite'):
            os.remove('./disk.sqlite')
