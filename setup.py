#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    # TODO: put package requirements here
]

setup(
    name='rakaia',
    version='0.0.1',
    description="Rakaia: a braided river",
    long_description=readme,
    author="Nathaniel Smith",
    author_email='njs@pobox.com',
    url='https://github.com/njsmith/rakaia',
    packages=[
        'rakaia',
    ],
    package_dir={'rakaia': 'rakaia'},
    include_package_data=True,
    install_requires=requirements,
    license="XX FIXME",
    zip_safe=False,
    keywords='rakaia',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: XX FIXME',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
)
