#!/usr/bin/env python
# -*- coding:utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="aucommon",
    version="0.1.0",
    packages=find_packages(),
    zip_safe=False,

    description="AuCommon",
    long_description="Audio Tools for Python 2 / 3",
    author="coppla",
    author_email="januszry@gmail.com",

    license="GPL",
    keywords=("utils"),
    platforms="Independant",
    url="",
    entry_points={'console_scripts': [
        'auprober=aucommon.auprobe:main',
        'wavanalyze=aucommon.wavfile:main',
        ]},

    install_requires=["requests>=2.3.0",
                      "ujson>=1.33",
                      "hexdump>=3.2"])
