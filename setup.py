from setuptools import setup, find_packages

"""
python3 setup.py sdist bdist_wheel; 
python3 setup.py sdist bdist_wheel; sudo pip install dist/$(python3 install.py);
python3 setup.py sdist bdist_wheel; pip install dist/$(python3 install.py) --user
python3 setup.py sdist bdist_wheel; pip install dist/$(python3 install.py) 
python3 setup.py sdist bdist_wheel; pip3 install dist/$(python3 install.py) 
sudo pip install dist/$(python3 install.py);
pip install dist/$(python3 install.py) --user
"""

setup(
    name='dbrecord',
    version='1.1.0',
    description='sqlite based kv database using for big data IO.',
    url='https://github.com/pytorch-lumo/dbrecord',
    author='sailist',
    author_email='sailist@outlook.com',
    license='Apache License 2.0',
    include_package_data=True,
    install_requires=[
        'joblib'
    ],
    classifiers=[

    ],
    keywords='dbrecord dataset pytorch',
    packages=find_packages('.', exclude=('tests', 'example')),
    entry_points={
    },
)
