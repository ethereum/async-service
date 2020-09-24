Release Notes
=============

.. towncrier release notes start

Async_Service 0.1.0-alpha.10 (2020-09-24)
-----------------------------------------

Features
~~~~~~~~

- Turn off verbose logging about task lifecycle by default. To re-enable it, set the environment
  variable ``ASYNC_SERVICE_VERBOSE_LOG=1``. (`#75 <https://github.com/ethereum/async-service/issues/75>`__)
- In py3.8, annotate asyncio tasks with a name, so that asyncio logs show more than
  _run_and_manage_task() when there's an issue like a coro that takes too long. (`#76 <https://github.com/ethereum/async-service/issues/76>`__)
- Raise an exception when more than 1000 child tasks are concurrently running. It slows down the event
  loop too much. (`#77 <https://github.com/ethereum/async-service/issues/77>`__)


Internal Changes - for async-service Contributors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Pull in updates from project template, for latest release notes, Makefile, etc. (`#78 <https://github.com/ethereum/async-service/issues/78>`__)


v0.1.0-alpha.1
--------------

- Launched repository, claimed names for pip, RTD, github, etc
