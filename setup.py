#!/usr/bin/env python

from os import path

from setuptools import setup

packages = [
    'paramsurvey_multimpi',
]

test_requirements = ['pytest', 'coverage', 'pytest-cov', 'pytest-sugar', 'coveralls', 'pyfakefs', 'requests_mock', 'pylint', 'flake8']

requires = [
    'paramsurvey>0.4.16',
    'aiohttp',
    'aiohttp-rpc',
    'requests',
    'psutil'
]

extras_require = {
    'ray': ['ray>=1', 'paramsurvey[ray]'],
    'test': test_requirements,  # setup no longer tests, so make them an extra
}

setup_requires = ['setuptools_scm']

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    description = f.read()

setup(
    name='paramsurvey_multimpi',
    use_scm_version=True,
    description='Code to assist running multi-node MPI jobs within paramsurvey',
    long_description=description,
    long_description_content_type='text/markdown',
    author='Greg Lindahl and others',
    author_email='lindahl@pbm.com',
    url='https://github.com/Smithsonian/cloud-corr-mri',
    packages=packages,
    python_requires=">=3.6.*",
    extras_require=extras_require,
    setup_requires=setup_requires,
    install_requires=requires,
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Operating System :: POSIX :: Linux',
        'Environment :: MacOS X',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        #'Programming Language :: Python :: 3.5',  # ray no longer supports py3.5, also setuptools_scm problem
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        #'Programming Language :: Python :: 3.10',  # awaiting a ray wheel, https://github.com/ray-project/ray/issues/19116
        'Programming Language :: Python :: 3 :: Only',
    ],
)
