#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import (
    setup,
    find_packages,
)

extras_require = {
    'test': [
        "hypothesis==4.44.4",
        "pytest==5.3.0",
        "pytest-xdist",
        "tox>=2.9.1,<3",
    ],
    'test-asyncio': [
        "pytest-asyncio>=0.10.0,<0.11",
    ],
    'test-trio': [
        "pytest-trio==0.5.2",
    ],
    'lint': [
        "black==19.3b",
        "flake8==3.7.9",
        "isort>=4.2.15,<5",
        "mypy==0.740",
        "pydocstyle>=3.0.0,<4",
    ],
    'doc': [
        "Sphinx>=1.6.5,<2",
        "sphinx_rtd_theme>=0.1.9",
    ],
    'dev': [
        "bumpversion>=0.5.3,<1",
        "pytest-watch>=4.1.0,<5",
        "wheel",
        "twine",
        "ipython",
    ],
}

extras_require['dev'] = (
    extras_require['dev'] +  # noqa: W504
    extras_require['test'] +  # noqa: W504
    extras_require['lint'] +  # noqa: W504
    extras_require['doc']
)


with open('./README.md') as readme:
    long_description = readme.read()


setup(
    name='async-service',
    # *IMPORTANT*: Don't manually change the version here. Use `make bump`, as described in readme
    version='0.1.0-alpha.1',
    description="""async-service: Lifecycle management for async applications""",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='The Ethereum Foundation',
    author_email='snakecharmers@ethereum.org',
    url='https://github.com/ethereum/async-service',
    include_package_data=True,
    install_requires=[
        "async-generator>=1.10,<2",
        "trio>=0.13,<14",
        "trio-typing>=0.3,<0.4",
    ],
    python_requires='>=3.6, <4',
    extras_require=extras_require,
    py_modules=['async_service'],
    license="MIT",
    zip_safe=False,
    keywords='ethereum',
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
