"""Installation script."""
from os import path
from io import open
from setuptools import find_packages, setup

HERE = path.abspath(path.dirname(__file__))

with open(path.join(HERE, 'README.rst'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read().strip()

setup(
    name='cumulus',
    description='Manage a cloud of clusters',
    long_description=LONG_DESCRIPTION,
    url='https://github.com/bouthilx/cumulus.git',
    author='Xavier Bouthillier',
    license='MIT',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Utilities',
        'Topic :: Scientific/Engineering',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    scripts=['cumulus/bin/cumulus-particle'],
    entry_points={
        "console_scripts": [
            "cumulus = cumulus.bin.cumulus:main",
            "cumulus-socket = cumulus.bin.cumulus_socket:main"
        ]
    },
    packages=find_packages(exclude=['tests']),
    install_requires=['numpy', 'pymongo', 'configobj', 'GitPython',
                      'virtualenv-clone', 'lockfile', 'paramiko'],
    extras_require={
        'test': ['nosetests'],
        'doc': ['sphinx', 'sphinx-autobuild']}
)
