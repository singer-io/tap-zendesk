#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-zendesk',
      version='2.8.0',
      description='Singer.io tap for extracting data from the Zendesk API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_zendesk'],
      install_requires=[
          'singer-python==6.8.0',
          'zenpy==2.0.57',
          'backoff==2.2.1',
          'requests==2.34.2',
          'aiohttp==3.14.0'
      ],
      extras_require={
          'dev': [
              'ipdb',
          ],
          'test': [
              'pylint==3.0.3',
              'nose2',
              'pytest'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-zendesk=tap_zendesk:main
      ''',
      packages=find_packages(),
      include_package_data=True,
)
