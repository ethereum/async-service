Guides
======

Services
~~~~~~~~

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


You can also run services in the background while we do other things.

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
    | FINISHED |
    +----------+

* Started:
    - The :meth:`~async_service.abc.ServiceAPI.run` method has been scheduled to run.
* Running:
    - The service has started and is still running (has not been cancelled and
      has not finished)
* Finished
    - The service has stopped.  All background tasks have either completed or
      been cancelled.


Cancellation
------------

Calling :meth:`~async_service.abc.ManagerAPI.cancel` will trigger cancellation
of the service and all child tasks and child services.  A service that has been
cancelled will still register as "running" until all child tasks have been
cancelled and the service registers as "finished".


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

        # check if the service is finished
        if manager.is_finished:
            ...

        # wait for the service to finishe completely
        await manager.wait_finished()


The :class:`~async_service.abc.ManagerAPI` also allows us to control the service.


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

This is done using the :attr:`~async_service.base.Service.manager`
attribute which exposes the :meth:`~async_service.abc.InternalManagerAPI.run_task`
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

For slighly nicer logging output we can provide a ``name`` as a keyword
argument to `~async_service.abc._InternalManagerAPI.run_task` which will be
used in logging messages.


Daemon Tasks
~~~~~~~~~~~~

A *"Daemon"* tasks is one that is intended to run for the full lifecycle of the
service.  This can be done by passing ``daemon=True`` into the call to
:meth:`~async_service.abc.InternalManagerAPI.run_task`.

.. code-block:: python

    from async_service import Service, run_asyncio_service

    class MyService(Service):
        async def do_long_running_thing(self):
            while True:
                ...

        async def run(self):
            # The following two statements are equivalent.
            self.manager.run_task(self.do_long_running_thing, daemon=True)
            self.manager.run_daemon_task(self.do_long_running_thing)


Alternatively we can use :meth:`~async_service.abc._InternalManagerAPI.run_daemon_task`.

A *"Daemon"* task which finishes before the service is shuts down will trigger
cancellation and result in the
:class:`~async_service.exceptions.DaemonTaskExit` exception to be raised.



Child Services
--------------

Child services are like tasks, except that they are other services that we
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
:meth:`~async_service.abc.InternalManagerAPI.run_child_service` method which
returns the manager for the child service.

There is also a
:meth:`~async_service.abc.InternalManagerAPI.run_daemon_child_service`
method behaves the same as
:meth:`~async_service.abc.InternalManagerAPI.run_daemon_task` in that if the
child service finishes before the parent service has finished, it will raise a
:class:`~async_service.exceptions.DaemonTaskExit` exception.


Task Shutdown
-------------

.. note:: This behavior is currently only guaranteed when using the ``asyncio`` based service manager.


As a service spawns background tasks, the manager keeps track of them as a
DAG_.  The **root** of the DAG is always the
:meth:`~async_service.abc.ServiceAPI.run` method with each new background task
being a child of whatever parent coroutine spawned it.

When the service is cancelled, these tasks are cancelled by traversing the task
DAG starting at the leaves and working up towards the root.  This provides a
guarantee that if the ``run()`` method spawns multiple backound tasks, that
the background tasks will be cancelled before the ``run()`` method is
cancelled.


.. _DAG: https://en.wikipedia.org/wiki/Directed_acyclic_graph


External Service APIs
---------------------

Sometimes we may want to expose an API from a
:class:`~async_service.base.Service` for external callers such that the call
should only work if the service is running, and calls should fail or be
terminated if the service is cancelled or finishes.

This can be done with the :func:`~async_service.asyncio.external_api`
decorator.

.. code-block:: python

    from async_service import Service, background_asyncio_service, external_asyncio_api

    class MyService(Service):
        async def run(self):
            ...

        @external_asyncio_api
        async def get_thing(self):
            ...

    service = MyService()

    # this will fail because the service isn't running yet
    await service.get_thing()

    async with background_asyncio_service(service) as manager:
        thing = await service.get_thing()

        # now cancel the service
        manager.cancel()

        # this will fail because the service is cancelled.
        thing = await service.get_thing()

.. note:: The :func:`~async_service.external_asyncio_api` can only be used on coroutine functions.


When a method decorated with :func:`~async_service.external_asyncio_api` fails
it raises an :class:`async_service.exceptions.ServiceCancelled` exception.


Cleanup logic
-------------

In the case that we need to run some logic **after** the service has finished
running but **before** the service has registered as finished we can do so with
the following patterns.  However, special care and consideration should be
taken as the following patterns can result in the application hanging when we
try to shut it down.

The basic idea is to use a ``try/finally`` expression in our main
``Service.run()`` method.  Since services track and shutdown their tasks using
a DAG, the code in the ``finally`` block is guaranteed to run after everything
else has stopped.

.. code-block:: python

    from async_service import Service

    class CleanupService(Service):
        async def run(self) -> None:
            try:
                ...  # do main service logic here
            finally:
                ...  # do cleanup logic here


For those running under ``trio`` it is worth noting that if the cleanup logic
needs to ``await`` anything we will probably need to shield it from further
cancellations.


.. code-block:: python

    
    from async_service import Service

    class CleanupService(Service):
        async def run(self) -> None:
            try:
                ...  # do main service logic here
            finally:
                with trio.CancelScope(shield=True):
                    ...  # do cleanup logic here


It is relatively trivial to implement a reusable pattern for doing cleanup.


.. code-block:: python

    
    from async_service import Service

    class CleanupService(Service):
        async def run(self) -> None:
            try:
                ...  # do main service logic here
            except:
                await self.on_error()
                raise
            else:
                await self.on_success()
            finally:
                await self.on_finally()

        async def on_success(self) -> None:
            pass
        
        async def on_error(self) -> None:
            pass

        async def on_finally(self) -> None:
            pass


Stats
-----

The :class:`~async_service.abc.ManagerAPI` exposes a
:meth:`~async_service.abc.ManagerAPI.stats` method which returns a
:class:`~async_service.stats.Stats` object with basic stats about the running
service.

.. code-block:: python

    async with background_asyncio_service(MyService) as manager:
        stats = manager.stats

        print(f"Total running tasks: {stats.total_count}")
        print(f"Finished tasks: {stats.finished_count}")
        print(f"Pending tasks: {stats.pending_count}")
