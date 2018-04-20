#!/usr/bin/env python
# encoding: utf-8

from setuptools import setup

setup(
    name='app',
    version='0.0.1',
    author='Anthony Miyaguchi',
    author_email='amiyaguchi@mozilla.com',
    description='BigQuery and DataProc interop test',
    install_requires=[
        "pyspark",
        "click",
        "numpy",
        "google-cloud"
    ],
    packages=['app'],
)