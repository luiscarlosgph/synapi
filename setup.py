#!/usr/bin/env python

import setuptools
import unittest

setuptools.setup(name='synapi',
    version='0.0.1',
    description='Package for dealing with Synapse datasets in an easy way.',
    author='Luis Carlos Garcia Peraza Herrera',
    author_email='luiscarlos.gph@gmail.com',
    license='MIT',
    packages=[
        'synapi',
    ],
    package_dir={
        'synapi': 'src',
    },
    #test_suite='tests',
    install_requires=[
      'synapseclient',
    ],
)
