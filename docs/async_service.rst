Services
========

This library provides strong lifecycle management for asynchronous applications.

All application logic must be encapslated in a
:class:`~async_service.base.Service` class which implements a
:meth:`~async_service.base.Service.run` method.


.. code-block:: python

    from async_service import Service, run_asyncio_service

    class MyApplication(Service):
        def run(self):
            print("I'm a service")

    await run_asyncio_service(MyApplication())


You can also run services in the background while you do other things.

.. code-block:: python

    from async_service import Service, background_asyncio_service

    class MyApplication(Service):
        def run(self):
            print("I'm a service")

    async with background_asyncio_service(MyApplication()):
        # do things while it runs
        ...
    # service will be finished here.


Lifecycle of a Service
----------------------

Each service has a well defined lifecycle.


.. code-block:: none

    +---------+
    | STARTED |
    +---------+
         |
         v
    +---------+
    | RUNNING |
    +---------+
         |
         v
    +----------+
    | STOPPING |
    +----------+
         |
         v
    +----------+
    | FINISHED |
    +----------+

* Started:
    - The :meth:`~async_service.base.Service.run` method has been scheduled to run.
* Running:
    - The service has started and is still running (has not been cancelled and
      has not finished)
* Stopping:
    - The service is in the process of stopping.  This can be triggered either
      by a call to :meth:`~async_service.abc.ManagerAPI.cancel` or by the
      service exiting naturally.  At this stage tasks may still be running in
      the background but will soon be forcibly cancelled.
* Finished
    - The service has stopped.  All background tasks have either completed or
      been cancelled.


Managers
--------

The :class:`~async_service.abc.ManagerAPI` is responsible for running a service
and managing the service lifecycle.  It also exposes all of the APIs for
inspecting a running service or waiting for the service to reach a specific
state.


.. code-block:: python

    from async_service import background_asyncio_service

    from my_application import MyApplicationService

    async with background_asyncio_service(MyApplicationService()) as manager:
        # wait for the service to be started
        await manager.wait_started()

        # check if the service has started
        if manager.is_started:
            ...

        # check if the service is running
        if manager.is_running:
            ...

        # check if the service has been cancelled
        if manager.is_cancelled:
            ...

        # check if the service is stopping
        if manager.is_stopping:
            ...

        # wait for the process of shutting down the service to begin
        await manager.wait_stopping()

        # check if the service is finished
        if manager.is_finished:
            ...

        # wait for the service to finishe completely
        await manager.wait_finished()


The :class:`~async_service.abc.ManagerAPI` also allows you to control the service.


.. code-block:: python

    from async_service import background_asyncio_service

    from my_application import MyApplicationService

    async with background_asyncio_service(MyApplicationService()) as manager:
        # Cancel the service
        manager.cancel()

        # Cancel the service AND wait for it to be finished
        await manager.stop()


Tasks
-----

Asynchrounous applications will typically need to run multiple things
concurrently which implies running things in the *background*.

This is done using the :attr:`~async_service.abc.ServiceAPI.manager`
attribute which exposes the :meth:`~async_service.abc._InternalManagerAPI.run_task`
method.

.. code-block:: python

    from async_service import Service, run_asyncio_service

    async def fetch_url(url):
        ...

    class MyService(Service):

        async def run(self):
            for url in URLS_TO_FETCH:
                self.manager.run_task(fetch_url, url)

The example above shows a service that concurrently fetches multiple URLS
concurrently.  These *tasks* will be scheduled and run in the background.  The
service will run until all of the background tasks are finished or the service
encounters an error in one of the tasks.

If a task raises an exception it will trigger cancellation of the service.
Upon exiting, all errors that were encountered while running the service will
be re-raised.

Daemon Tasks
~~~~~~~~~~~~

A *"Daemon"* tasks is one that is intended to run for the full lifecycle of the
service.  This can be done by passing ``daemon=True`` into the call to
:meth:`~async_service.abc._InternalManagerAPI.run_task`.

.. code-block:: python

    from async_service import Service, run_asyncio_service

    class MyService(Service):
        async def do_long_running_thing(self):
            while True:
                ...

        async def run(self):
            self.manager.run_task(self.do_long_running_thing, daemon=True)


Alternatively you can use :meth:`~async_service.abc._InternalManagerAPI.run_daemon_task`.

A *"Daemon"* task which finishes before the service is stopping will trigger
cancellation and result in the
:class:`~async_service.exceptions.DaemonTaskExit` exception to be raised.

Child Services
--------------

Child services are like tasks, except that they are other services that you
want to run within a running service.


.. code-block:: python

    from async_service import Service, run_asyncio_service

    class ChildService(Service):
        async def run(self):
            ...

    class ParentService(Service):
        async def run(self):
            child_manager = self.manager.run_child_service(ChildService())

Child services are run using the
:meth:`~async_service.abc._InternalManagerAPI.run_child_service` method which
returns the manager for the child service.

There is also a
:meth:`~async_service.abc._InternalManagerAPI.run_daemon_child_service`
method behaves the same as
:meth:`~async_service.abc._InternalManagerAPI.run_daemon_task` in that if the
child service finishes before the parent service has finished, it will raise a
:class:`~async_service.exceptions.DaemonTaskExit` exception.


External Service APIs
---------------------

Sometimes you may want to expose an API from a
:class:`~async_service.base.Service` for external callers such that the call
should only work if the service is running, and calls should fail or be
terminated if the service is cancelled or finishes.

This can be done with the :func:`~async_service.asyncio.external_api`
decorator.

.. code-block:: python

    from async_service import Service, background_asyncio_service
    from async_service.asyncio import external_api

    class MyService(Service):
        async def run(self):
            ...

        @external_api
        async def get_thing(self):
            ...

    service = MyService()

    # this will fail because the service isn't running yet
    await service.get_thing()

    async with background_asyncio_service(service) as manager:
        thing = await service.get_thing()

        # now cancel the service
        manager.cancel()

        # this will fail
        thing = await service.get_thing()

.. note:: The :func:`~async_service.asyncio.external_api` can only be used on coroutine functions.
