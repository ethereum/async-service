import functools
import sys
from typing import Any, AsyncIterator, Awaitable, Callable, cast, Dict, List, Tuple

from async_generator import asynccontextmanager
import trio
import trio_typing

from ._utils import get_task_name, iter_dag
from .abc import ManagerAPI, ServiceAPI
from .base import BaseManager
from .exceptions import DaemonTaskExit, LifecycleError
from .typing import EXC_INFO


class TrioManager(BaseManager):
    # The nursery running our root task. If any tasks spawn other tasks, we create separate,
    # nested nurseries for them so that they are terminated in the correct order.  This nursery is
    # cancelled if the service is cancelled but allowed to exit normally if the service exits.
    _root_nursery: trio_typing.Nursery

    # A mapping of parent to list of children which tracks the hierarchy of
    # service tasks.
    _service_task_dag: Dict[trio.hazmat.Task, List[trio.hazmat.Task]]

    # Individual nurseries for each child task.
    _task_cancel_scopes: Dict[
        trio.hazmat.Task,
        Tuple[trio.CancelScope, trio.Event],
    ]

    def __init__(self, service: ServiceAPI) -> None:
        super().__init__(service)

        # events
        self._cancelled = trio.Event()
        self._started = trio.Event()
        self._stopping = trio.Event()
        self._finished = trio.Event()

        # task dag tracking
        self._service_task_dag = {}
        self._task_cancel_scopes = {}

        # child service tracking
        self._child_managers = []

        # locks
        self._run_lock = trio.Lock()

    #
    # System Tasks
    #
    async def _handle_cancelled(self) -> None:
        """
        When cancellation is requested this triggers the cancellation of all
        background tasks.
        """
        self.logger.debug("%s: _handle_cancelled waiting for cancellation", self)
        await self._cancelled.wait()
        self.logger.debug("%s: _handle_cancelled triggering task DAG cancellation", self)

        for task in iter_dag(self._service_task_dag.copy()):
            scope, done = self._task_cancel_scopes[task]
            scope.cancel()
            await done.wait()

        # Ensure all of the child services that were running will also register
        # as being cancelled.
        for child_manager in self._child_managers:
            child_manager.cancel()

    async def _handle_run(self) -> None:
        """
        Run and monitor the actual :meth:`ServiceAPI.run` method.

        In the event that it throws an exception the service will be cancelled.
        """
        done = trio.Event()
        current_task = trio.hazmat.current_task()
        self._task_cancel_scopes[current_task] = (self._root_nursery.cancel_scope, done)
        self._service_task_dag[current_task] = []

        try:
            await self._service.run()
        except Exception as err:
            self.logger.debug(
                "%s: _handle_run got error, storing exception and setting cancelled: %s",
                self,
                err,
            )
            self._errors.append(cast(EXC_INFO, sys.exc_info()))
            self.cancel()
        else:
            # NOTE: Any service which uses daemon tasks will need to trigger
            # cancellation in order for the service to exit since this code
            # path does not trigger task cancellation.  It might make sense to
            # trigger cancellation if all of the running tasks are daemon
            # tasks.
            self.logger.debug(
                "%s: _handle_run exited cleanly, waiting for full stop...", self
            )
        finally:
            done.set()

    @classmethod
    async def run_service(cls, service: ServiceAPI) -> None:
        manager = cls(service)
        await manager.run()

    async def run(self) -> None:
        if self._run_lock.locked():
            raise LifecycleError(
                "Cannot run a service with the run lock already engaged.  Already started?"
            )
        elif self.is_started:
            raise LifecycleError("Cannot run a service which is already started.")

        try:
            async with self._run_lock:
                async with trio.open_nursery() as system_nursery:
                    system_nursery.start_soon(self._handle_cancelled)

                    try:
                        async with trio.open_nursery() as nursery:
                            self._root_nursery = nursery

                            nursery.start_soon(self._handle_run)

                            self._started.set()

                            # ***BLOCKING HERE***
                            # The code flow will block here until the background tasks have
                            # completed or cancellation occurs.
                    except Exception:
                        # Exceptions from any tasks spawned by our service will be caught by trio
                        # and raised here, so we store them to report together with any others we
                        # have already captured.
                        self._errors.append(cast(EXC_INFO, sys.exc_info()))
                    finally:
                        system_nursery.cancel_scope.cancel()

        finally:
            # We need this inside a finally because a trio.Cancelled exception may be raised
            # here and it wouldn't be swalled by the 'except Exception' above.
            self._stopping.set()
            self.logger.debug("%s stopping", self)
            self._finished.set()
            self.logger.debug("%s finished", self)

        # This is outside of the finally block above because we don't want to suppress
        # trio.Cancelled or trio.MultiError exceptions coming directly from trio.
        if self.did_error:
            raise trio.MultiError(
                tuple(
                    exc_value.with_traceback(exc_tb)
                    for _, exc_value, exc_tb in self._errors
                )
            )

    #
    # Event API mirror
    #
    @property
    def is_started(self) -> bool:
        return self._started.is_set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled.is_set()

    @property
    def is_stopping(self) -> bool:
        return self._stopping.is_set() and not self.is_finished

    @property
    def is_finished(self) -> bool:
        return self._finished.is_set()

    #
    # Control API
    #
    def cancel(self) -> None:
        if not self.is_started:
            raise LifecycleError("Cannot cancel as service which was never started.")
        self._cancelled.set()

    #
    # Wait API
    #
    async def wait_started(self) -> None:
        await self._started.wait()

    async def wait_stopping(self) -> None:
        await self._stopping.wait()

    async def wait_finished(self) -> None:
        await self._finished.wait()

    async def _run_and_manage_task(
        self,
        async_fn: Callable[..., Awaitable[Any]],
        *args: Any,
        daemon: bool,
        name: str,
        parent: trio.hazmat.Task,
        task_status=trio.TASK_STATUS_IGNORED,
    ) -> None:
        current_task = trio.hazmat.current_task()
        done = trio.Event()
        self._service_task_dag[current_task] = []
        self._service_task_dag[parent].append(current_task)
        self.logger.info("Starting task %s, child of %s", current_task, parent)

        with trio.CancelScope() as cancel_scope:
            self._task_cancel_scopes[current_task] = (cancel_scope, done)
            task_status.started()

            self.logger.debug("running task '%s[daemon=%s]'", name, daemon)
            try:
                await async_fn(*args)
            except Exception as err:
                self.logger.debug(
                    "task '%s[daemon=%s]' exited with error: %s",
                    name,
                    daemon,
                    err,
                    exc_info=True,
                )
                self._errors.append(cast(EXC_INFO, sys.exc_info()))
                self.cancel()
            else:
                self.logger.debug("task '%s[daemon=%s]' finished.", name, daemon)
                if daemon:
                    self.logger.debug(
                        "daemon task '%s' exited unexpectedly.  Cancelling service: %s",
                        name,
                        self,
                    )
                    self.cancel()
                    raise DaemonTaskExit(f"Daemon task {name} exited")
        done.set()

    def run_task(
        self,
        async_fn: Callable[..., Awaitable[Any]],
        *args: Any,
        daemon: bool = False,
        name: str = None,
    ) -> None:
        task_name = get_task_name(async_fn, name)
        current_task = trio.hazmat.current_task()
        partial_fn = functools.partial(self._run_and_manage_task, daemon=daemon, name=task_name,
                                       parent=current_task)
        self._root_nursery.start_soon(
            partial_fn,
            async_fn,
            *args,
            name=task_name,
        )

    def run_child_service(
        self, service: ServiceAPI, daemon: bool = False, name: str = None
    ) -> ManagerAPI:
        child_manager = type(self)(service)
        task_name = get_task_name(service, name)
        self.run_task(child_manager.run, daemon=daemon, name=task_name)
        self._child_managers.append(child_manager)
        return child_manager


@asynccontextmanager
async def background_trio_service(service: ServiceAPI) -> AsyncIterator[ManagerAPI]:
    """
    Run a service in the background.

    The service is running within the context
    block and will be properly cleaned up upon exiting the context block.
    """
    async with trio.open_nursery() as nursery:
        manager = TrioManager(service)
        nursery.start_soon(manager.run)
        await manager.wait_started()
        try:
            yield manager
        finally:
            await manager.stop()
