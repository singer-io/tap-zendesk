#!/usr/bin/env python

from setuptools import setup

setup(name='tap-zendesk',
      version='1.4.31',
      description='Singer.io tap for extracting data from the Zendesk API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_zendesk'],
      install_requires=[
          # Currently this refers to our fork. This will only work if we
          # specifically install the correct version of our fork before
          # trying to install this package.
          'singer-python==5.7.*',
          'zenpy==2.0.12',
      ],
      extras_require={
          'dev': [
              'ipython',
              'ipdb',
              'pylint',
              'nose',
              'nose-watch',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-zendesk=tap_zendesk:main
      ''',
      packages=['tap_zendesk'],
      include_package_data=True,
)
