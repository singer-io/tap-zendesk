#!/usr/bin/env python

from setuptools import setup

setup(name='tap-zendesk',
      version='2.6.4',
      description='Singer.io tap for extracting data from the Zendesk API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_zendesk'],
      install_requires=[
          'singer-python==6.0.1',
          'zenpy==2.0.24',
          'backoff==2.2.1',
          'requests==2.32.4',
          'aiohttp==3.11.11'
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
      packages=['tap_zendesk'],
      include_package_data=True,
)
