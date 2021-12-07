#!/usr/bin/env python

from setuptools import setup

setup(name='tap-zendesk',
      version='1.7.4',
      description='Singer.io tap for extracting data from the Zendesk API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_zendesk'],
      install_requires=[
          'singer-python==5.12.2',
          'zenpy==2.0.24',
          'backoff==1.8.0',
          'requests==2.25.1',
      ],
      extras_require={
          'dev': [
              'ipdb',
          ],
          'test': [
              'pylint==2.8.3',
              'nose',
              'nose-watch',
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
