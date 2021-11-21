#! /usr/bin/python3

from distutils.core import setup, Extension
import re

with open('debian/changelog') as changelog:
	name, version = re.match(r'(\S+) \((\S+)\)', changelog.readline()).group(1, 2)

setup(
	name = name,
	version = version,
	ext_modules = [Extension('rsync_fetch', ['rsync-fetch.c'], libraries=['avl'])],
)
