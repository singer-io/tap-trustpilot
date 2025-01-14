#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-trustpilot",
    version="0.2.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_trustpilot"],
    install_requires=[
        "singer-python",
        "requests",
    ],
    # requires following addition packages for code check quality
    extras_require={
        'dev': [
            'pylint',
            'ipdb',
            'nose'
        ]
    },
    entry_points="""
    [console_scripts]
    tap-trustpilot=tap_trustpilot:main
    """,
    packages=["tap_trustpilot"],
    package_data = {
        "schemas": ["tap_trustpilot/schemas/*.json"]
    },
    include_package_data=True,
)
