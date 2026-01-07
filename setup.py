# perm_files/titan_sdk/setup.py
from setuptools import setup, find_packages

setup(
    name='titan_sdk',
    version='0.1',
    packages=find_packages(),
    py_modules=['titan_sdk'],
    install_requires=[
        'requests'
    ],
)