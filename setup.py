#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from setuptools import setup, find_packages


def find_version(fname):
    """Attempts to find the version number in the file names fname.
    Raises RuntimeError if not found.
    """
    version = ''
    with open(fname, 'r') as fp:
        reg = re.compile(r'__version__ = [\'"]([^\'"]*)[\'"]')
        for line in fp:
            m = reg.match(line)
            if m:
                version = m.group(1)
                break
    if not version:
        raise RuntimeError('Cannot find version information')
    return version


__version__ = find_version('dynamongo/__init__.py')


def read(fname):
    with open(fname) as fp:
        content = fp.read()
    return content


setup(
    name='dynamongo',
    version=__version__,
    description=(
        'A lightweight library for interacting with AWS dynamoDB in a pythonic way,'
        'inspired by pymongo'
    ),
    long_description=read('README.rst'),
    author='Musyoka Morris',
    author_email='musyokamorris@gmail.com',
    url='https://github.com/musyoka-morris/dynamongo',
    packages=find_packages(exclude=('test*', 'examples')),
    package_dir={'dynamongo': 'dynamongo'},
    include_package_data=True,
    license='MIT',
    zip_safe=False,
    keywords=(
        'aws', 'dynamo', 'dynamodb', 'orm'
        'serialization', 'deserialization', 'validation'
    ),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: PyPy',
        "Topic :: Software Development :: Libraries"
    ],
    test_suite='tests',
    project_urls={
        'Bug Reports': 'https://github.com/musyoka-morris/dynamongo/issues'
    },
    install_requires=[
        'boto3',
        'botocore',
        'schematics',
        'arrow',
        'validators',
        'voluptuous',
        'inflection',
        'bloop'
    ]
)

