# async-service

[![Join the conversation on Discord](https://img.shields.io/discord/809793915578089484?color=blue&label=chat&logo=discord&logoColor=white)](https://discord.gg/GHryRvPB84)
[![Build Status](https://circleci.com/gh/ethereum/async-service.svg?style=shield)](https://circleci.com/gh/ethereum/async-service)
[![PyPI version](https://badge.fury.io/py/async-service.svg)](https://badge.fury.io/py/async-service)
[![Python versions](https://img.shields.io/pypi/pyversions/async-service.svg)](https://pypi.python.org/pypi/async-service)
[![Docs build](https://readthedocs.org/projects/async-service/badge/?version=latest)](https://async-service.readthedocs.io/en/latest/?badge=latest)

Lifecycle management for async applications

Read more in the [documentation on ReadTheDocs](https://async-service.readthedocs.io/). [View the change log](https://async-service.readthedocs.io/en/latest/release_notes.html).

## Quickstart

```sh
python -m pip install async-service
```

## Developer Setup

If you would like to hack on async-service, please check out the [Snake Charmers
Tactical Manual](https://github.com/ethereum/snake-charmers-tactical-manual)
for information on how we do:

- Testing
- Pull Requests
- Documentation

We use [pre-commit](https://pre-commit.com/) to maintain consistent code style. Once
installed, it will run automatically with every commit. You can also run it manually
with `make lint`. If you need to make a commit that skips the `pre-commit` checks, you
can do so with `git commit --no-verify`.

### Development Environment Setup

You can set up your dev environment with:

```sh
git clone git@github.com:ethereum/async-service.git
cd async-service
virtualenv -p python3 venv
. venv/bin/activate
python -m pip install -e ".[dev]"
pre-commit install
```

### Release setup

To release a new version:

```sh
make release bump=$$VERSION_PART_TO_BUMP$$
```

#### How to bumpversion

The version format for this repo is `{major}.{minor}.{patch}` for stable, and
`{major}.{minor}.{patch}-{stage}.{devnum}` for unstable (`stage` can be alpha or beta).

To issue the next version in line, specify which part to bump,
like `make release bump=minor` or `make release bump=devnum`. This is typically done from the
main branch, except when releasing a beta (in which case the beta is released from main,
and the previous stable branch is released from said branch).

If you are in a beta version, `make release bump=stage` will switch to a stable.

To issue an unstable version when the current version is stable, specify the
new version explicitly, like `make release bump="--new-version 4.0.0-alpha.1 devnum"`
