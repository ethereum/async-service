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
    Set,
    TypeVar,
    cast,
)

from async_generator import asynccontextmanager
from trio import MultiError

from ._utils import get_task_name, iter_dag
from .abc import ManagerAPI, ServiceAPI
from .asyncio_compat import get_current_task
from .base import BaseManager
from .exceptions import DaemonTaskExit, LifecycleError, ServiceCancelled
from .typing import EXC_INFO


class AsyncioManager(BaseManager):
    # Tracking of the system level background tasks.
    _system_tasks: Set["asyncio.Future[None]"]

    # Tracking of the background tasks that the service has initiated.
    _service_task_dag: Dict["asyncio.Future[Any]", Set["asyncio.Future[Any]"]]

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
        await self._cancelled.wait()
        self.logger.debug("%s: _handle_cancelled triggering task cancellation", self)

        # Here we iterate over the task DAG such that we cancel the most deeply
        # nested tasks first ensuring that each has finished completely before
        # we move onto cancelling the parent tasks.
        #
        # We have to make a copy of the task dag because it is possible that
        # there is a task which has just been scheduled. In this case the new
        # task will end up being cancelled as part of it's parent task's cancel
        # scope, **or** if it was scheduled by an external API call it will be
        # cancelled as part of the global task nursery's cancellation.
        tasks_to_cancel = tuple(iter_dag(self._service_task_dag))
        for task in tasks_to_cancel:
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

                self._started.set()

                self.run_task(self._service.run)

                # block here until all service tasks have finished
                await self._wait_service_tasks()
            finally:
                # None of the statements above are supposed to raise exceptions (as they all rely
                # on _run_and_manage_task() to swallow and store them in self._errors, but in case
                # they do we probably have a bug so we just mark the service as stopping and
                # return.
                self._stopping.set()
                self.logger.debug("%s: stopping", self)

            # block here until all system tasks have finished.
            await asyncio.wait(
                (handle_cancelled_task, handle_stopping_task),
                return_when=asyncio.ALL_COMPLETED,
            )

        self._finished.set()
        self.logger.debug("%s: finished", self)

        # Above we rely on run_task() and handle_cancelled()/handle_stopping() to run the
        # service/tasks and swallow/collect exceptions so that they can be reported all together
        # here.
        if self.did_error:
            raise MultiError(
                tuple(
                    exc_value.with_traceback(exc_tb)
                    for _, exc_value, exc_tb in self._errors
                )
            )

    async def _wait_service_tasks(self) -> None:
        done = asyncio.Event()
        while True:
            # Each time we `await` it is possible that new tasks could be
            # added.  For this reason we have to recompute the set of tasks
            # inside of the loop.
            pending_service_tasks = tuple(
                task for task in self._service_task_dag if not task.done()
            )
            if pending_service_tasks:
                # In the event that there are any *pending* tasks, we await the
                # first one as an optimization.  We could await all of them,
                # but since we would end up having to recompute the set of
                # pending tasks anyways, we simplify things by only awaiting
                # the first one which is less load on the event loop.
                pending_service_tasks[0].add_done_callback(lambda fut: done.set())
                await done.wait()
                done.clear()
            else:
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
        self.logger.debug("%s: running task '%s[daemon=%s]'", self, name, daemon)
        # mypy thinks this is an `Optional[asyncio.Task[Any]]` which causes
        # some problems lower down when it is used to lookup dependencies in
        # the task dag.
        task = cast("asyncio.Future[Any]", get_current_task())

        try:
            await async_fn(*args)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            self.logger.debug(
                "%s: task '%s[daemon=%s]' exited with error: %s",
                self,
                name,
                daemon,
                err,
                exc_info=True,
            )
            self._errors.append(cast(EXC_INFO, sys.exc_info()))
            self.cancel()
        else:
            if daemon:
                self.logger.debug(
                    "%s: daemon task '%s' exited unexpectedly.  Cancelling service",
                    self,
                    name,
                )
                self.cancel()
                raise DaemonTaskExit(f"Daemon task {name} exited")

            self.logger.debug(
                "%s: task '%s[daemon=%s]' cleaning up", self, name, daemon
            )
            # Now we wait for all of the child tasks to be complete.
            child_done = asyncio.Event()
            for child_task in tuple(self._service_task_dag[task]):
                if not child_task.done():
                    child_task.add_done_callback(lambda fut: child_done.set())
                    await child_done.wait()
                    child_done.clear()

            self.logger.debug("%s: task '%s[daemon=%s]' finished.", self, name, daemon)

    def _cleanup_task(self, task: "asyncio.Future[Any]") -> None:
        self._service_task_dag.pop(task)
        for dependencies in self._service_task_dag.values():
            dependencies.discard(task)
        self._done_task_count += 1

    def run_task(
        self,
        async_fn: Callable[..., Awaitable[Any]],
        *args: Any,
        daemon: bool = False,
        name: str = None,
    ) -> None:
        if not self.is_running:
            raise LifecycleError(
                "Tasks may not be scheduled if the service is not running"
            )

        task_name = get_task_name(async_fn, name)

        if self.is_running and self.is_cancelled:
            self.logger.debug(
                "%s: service is in the process of being cancelled. Not running task "
                "%s[daemon=%s]",
                self,
                task_name,
                daemon,
            )
            return

        task = asyncio.ensure_future(
            self._run_and_manage_task(async_fn, *args, daemon=daemon, name=task_name),
            loop=self._loop,
        )
        task.add_done_callback(self._cleanup_task)
        self._service_task_dag[task] = set()
        self._total_task_count += 1

        parent_task = get_current_task()
        if parent_task in self._service_task_dag:
            self.logger.debug(
                "%s: new child task %s -> %s[daemon=%s] added to DAG",
                self,
                parent_task,
                task_name,
                daemon,
            )
            self._service_task_dag[parent_task].add(task)
        else:
            self.logger.debug(
                "%s: new root task %s[daemon=%s] added to DAG", task_name, daemon
            )

    def run_child_service(
        self, service: ServiceAPI, daemon: bool = False, name: str = None
    ) -> ManagerAPI:
        child_manager = type(self)(service, loop=self._loop)
        task_name = get_task_name(service, name)
        self.run_task(child_manager.run, daemon=daemon, name=task_name)
        return child_manager


@asynccontextmanager
async def cleanup_tasks(*tasks: "asyncio.Future[Any]") -> AsyncIterator[None]:
    """
    Context manager that ensures that all tasks are properly cancelled and awaited.

    The order in which tasks are cleaned is such that the first task will be
    the last to be cancelled/awaited.

    This function **must** be called with at least one task.
    """
    try:
        task = tasks[0]
    except IndexError:
        raise TypeError("cleanup_tasks must be called with at least one task")

    try:
        if len(tasks) > 1:
            async with cleanup_tasks(*tasks[1:]):
                yield
        else:
            yield
    finally:
        if not task.done():
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass


TFunc = TypeVar("TFunc", bound=Callable[..., Coroutine[Any, Any, Any]])


def external_api(func: TFunc) -> TFunc:
    @functools.wraps(func)
    async def inner(self: ServiceAPI, *args: Any, **kwargs: Any) -> Any:
        if not hasattr(self, "manager"):
            raise ServiceCancelled(
                f"Cannot access external API {func}.  Service has not been run."
            )

        manager = self.get_manager()

        if not manager.is_running:
            raise ServiceCancelled(
                f"Cannot access external API {func}.  Service is not running: "
                f"started={manager.is_started}  running={manager.is_running} "
                f"stopping={manager.is_stopping}  finished={manager.is_finished}"
            )

        func_task: "asyncio.Future[Any]" = asyncio.ensure_future(
            func(self, *args, **kwargs)
        )
        service_stopping_task = asyncio.ensure_future(manager.wait_stopping())

        done, pending = await asyncio.wait(
            (func_task, service_stopping_task), return_when=asyncio.FIRST_COMPLETED
        )
        async with cleanup_tasks(*done, *pending):
            if func_task.done():
                return await func_task
            elif service_stopping_task.done():
                raise ServiceCancelled(
                    f"Cannot access external API {func}.  Service is not running: "
                    f"started={manager.is_started}  running={manager.is_running} "
                    f"stopping={manager.is_stopping}  finished={manager.is_finished}"
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
        async with cleanup_tasks(task):
            await manager.wait_started()

            try:
                yield manager
            finally:
                await manager.stop()
    finally:
        if manager.did_error:
            # TODO: better place for this.
            raise MultiError(
                tuple(
                    exc_value.with_traceback(exc_tb)
                    for _, exc_value, exc_tb in manager._errors
                )
            )
