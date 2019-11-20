import asyncio
import functools
import sys
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    List,
    Set,
    TypeVar,
    cast,
)

from async_generator import asynccontextmanager
from trio import MultiError

from async_service._utils import iter_dag

from .abc import ManagerAPI, ServiceAPI
from .base import BaseManager
from .exceptions import DaemonTaskExit, LifecycleError, ServiceCancelled
from .typing import EXC_INFO


class AsyncioManager(BaseManager):
    # Tracking of the system level background tasks.
    _system_tasks: Set["asyncio.Future[None]"]

    # Tracking of the background tasks that the service has initiated.
    _service_task_dag: Dict["asyncio.Future[Any]", List["asyncio.Future[Any]"]]

    def __init__(
        self, service: ServiceAPI, loop: asyncio.AbstractEventLoop = None
    ) -> None:
        super().__init__(service)

        self._loop = loop

        # events
        self._started = asyncio.Event()
        self._cancelled = asyncio.Event()
        self._stopping = asyncio.Event()
        self._finished = asyncio.Event()

        # locks
        self._run_lock = asyncio.Lock()

        # task tracking
        self._service_task_dag = {}
        self._system_tasks = set()

    #
    # System Tasks
    #
    async def _handle_cancelled(self) -> None:
        """
        When cancellation is requested this triggers the cancellation of all
        background tasks.
        """
        self.logger.debug("%s: _handle_cancelled waiting for cancellation", self)
        await self.wait_cancelled()
        self.logger.debug("%s: _handle_cancelled triggering task cancellation", self)

        # TODO: need new comment here explaining the way we iterate in
        # dependency first order.
        for task in iter_dag(self._service_task_dag):
            if not task.done():
                task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                self._errors.append(cast(EXC_INFO, sys.exc_info()))

    async def _handle_stopping(self) -> None:
        """
        Once the `_stopping` event is set this triggers cancellation of the system tasks.
        """
        self.logger.debug("%s: _handle_stopping waiting for stopping", self)
        await self.wait_stopping()
        self.logger.debug(
            "%s: _handle_stopping triggering system task cancellations", self
        )

        # trigger cancellation of all of the system tasks
        for task in self._system_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                self._errors.append(cast(EXC_INFO, sys.exc_info()))

    @classmethod
    async def run_service(
        cls, service: ServiceAPI, loop: asyncio.AbstractEventLoop = None
    ) -> None:
        manager = cls(service, loop=loop)
        await manager.run()

    async def run(self) -> None:
        if self._run_lock.locked():
            raise LifecycleError(
                "Cannot run a service with the run lock already engaged.  Already started?"
            )
        elif self.is_started:
            raise LifecycleError("Cannot run a service which is already started.")

        async with self._run_lock:
            try:
                handle_cancelled_task = asyncio.ensure_future(
                    self._handle_cancelled(), loop=self._loop
                )
                handle_stopping_task = asyncio.ensure_future(
                    self._handle_stopping(), loop=self._loop
                )

                self._system_tasks.add(handle_cancelled_task)
                self._system_tasks.add(handle_stopping_task)

                self.run_task(self._service.run, _root=True)

                self._started.set()

                # block here until all service tasks have finished
                await self._wait_service_tasks()
            finally:
                # signal that the service is stopping
                self._stopping.set()
                self.logger.debug("%s stopping", self)

            # block here until all system tasks have finished.
            await asyncio.wait(
                (handle_cancelled_task, handle_stopping_task),
                return_when=asyncio.ALL_COMPLETED,
            )

        self._finished.set()
        self.logger.debug("%s finished", self)

        # If an error occured, re-raise it here
        if self.did_error:
            raise MultiError(
                tuple(
                    exc_value.with_traceback(exc_tb)
                    for _, exc_value, exc_tb in self._errors
                )
            )

    async def _wait_service_tasks(self) -> None:
        while True:
            done, pending = await asyncio.wait(
                tuple(self._service_task_dag.keys()), return_when=asyncio.ALL_COMPLETED
            )
            if all(task.done() for task in self._service_task_dag.keys()):
                break

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

    async def wait_cancelled(self) -> None:
        await self._cancelled.wait()

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
    ) -> None:
        try:
            await async_fn(*args)
        except asyncio.CancelledError:
            raise
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

    def run_task(
        self,
        async_fn: Callable[..., Awaitable[Any]],
        *args: Any,
        daemon: bool = False,
        name: str = None,
        _root: bool = False,
    ) -> None:
        current_task = asyncio.Task.current_task()
        if not _root and current_task not in self._service_task_dag:
            raise Exception(f"TODO: unknown task {current_task}")
        task = asyncio.ensure_future(
            self._run_and_manage_task(
                async_fn, *args, daemon=daemon, name=name or repr(async_fn)
            ),
            loop=self._loop,
        )
        self._service_task_dag[task] = []
        if not _root:
            self._service_task_dag[current_task].append(task)

    def run_child_service(
        self, service: ServiceAPI, daemon: bool = False, name: str = None
    ) -> ManagerAPI:
        child_manager = type(self)(service, loop=self._loop)
        self.run_task(child_manager.run, daemon=daemon, name=name or repr(service))
        return child_manager


TReturn = TypeVar("TReturn")
TFunc = TypeVar("TFunc", bound=Callable[..., Coroutine[Any, Any, TReturn]])


def external_api(func: TFunc) -> TFunc:
    @functools.wraps(func)
    async def inner(self: ServiceAPI, *args: Any) -> TReturn:
        if self.manager.is_stopping:
            raise ServiceCancelled(
                f"Cannot access external API {func}.  Service is already cancelled"
            )

        func_task: "asyncio.Future[TReturn]" = asyncio.ensure_future(func(self, *args))
        service_stopping_task = asyncio.ensure_future(self.manager.wait_stopping())

        done, pending = await asyncio.wait(
            (func_task, service_stopping_task), return_when=asyncio.FIRST_COMPLETED
        )

        if func_task.done():
            service_stopping_task.cancel()
            try:
                await service_stopping_task
            except asyncio.CancelledError:
                pass
            return await func_task
        elif service_stopping_task.done():
            func_task.cancel()
            try:
                await func_task
            except asyncio.CancelledError:
                pass
            raise ServiceCancelled(
                f"Cannot access external API {func}.  Service is already cancelled"
            )
        else:
            raise Exception("Code path should be unreachable")

    return cast(TFunc, inner)


@asynccontextmanager
async def background_asyncio_service(
    service: ServiceAPI, loop: asyncio.AbstractEventLoop = None
) -> AsyncIterator[ManagerAPI]:
    """
    Run a service in the background.

    The service is running within the context block and will be properly
    cleaned up upon exiting the context block.
    """
    manager = AsyncioManager(service, loop=loop)
    task = asyncio.ensure_future(manager.run(), loop=loop)

    try:
        await manager.wait_started()

        try:
            yield manager
        finally:
            await manager.stop()

        assert not manager.did_error, "ARST ARST ARST"

    finally:
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        if manager.did_error:
            # TODO: better place for this.
            raise MultiError(
                tuple(
                    exc_value.with_traceback(exc_tb)
                    for _, exc_value, exc_tb in manager._errors
                )
            )
