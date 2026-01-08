#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

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
    license="Apache-2.0",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
    ],
)