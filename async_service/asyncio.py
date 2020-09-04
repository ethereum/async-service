import asyncio
import functools
import sys
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Optional,
    Set,
    TypeVar,
    cast,
)

from async_generator import asynccontextmanager
from trio import MultiError

from ._utils import get_task_name
from .abc import ManagerAPI, ServiceAPI, TaskAPI, TaskWithChildrenAPI
from .asyncio_compat import get_current_task
from .base import BaseChildServiceTask, BaseFunctionTask, BaseManager
from .exceptions import DaemonTaskExit, LifecycleError
from .typing import EXC_INFO


class FunctionTask(BaseFunctionTask):
    asyncio_task: "asyncio.Future[Any]"

    #
    # Core Task API
    #
    async def run(self) -> None:
        try:
            await self._async_fn(*self._async_fn_args)
            if self.daemon:
                raise DaemonTaskExit(f"Daemon task {self} exited")

            while self.children:
                await tuple(self.children)[0].wait_done()
        finally:
            if self.parent is not None:
                self.parent.discard_child(self)

    async def cancel(self) -> None:
        async with cleanup_tasks(self.asyncio_task):
            for task in tuple(self.children):
                await task.cancel()

    @property
    def is_done(self) -> bool:
        return self.asyncio_task.done()

    _wait_done_event: Optional[asyncio.Event] = None

    async def wait_done(self) -> None:
        if self.asyncio_task.done():
            return

        if self._wait_done_event is None:
            # lazily register an event so we can wait for the task to be done.
            # We do this instead of directly awaiting the `asyncio_task` so
            # that we don't have to deal with exception handling.
            wait_done_event = asyncio.Event()
            self._wait_done_event = wait_done_event
            self.asyncio_task.add_done_callback(lambda fut: wait_done_event.set())
        await self._wait_done_event.wait()


class ChildServiceTask(BaseChildServiceTask):
    asyncio_task: "asyncio.Future[Any]"

    def __init__(
        self,
        name: str,
        daemon: bool,
        parent: Optional[TaskWithChildrenAPI],
        child_service: ServiceAPI,
        loop: Optional[asyncio.AbstractEventLoop],
    ) -> None:
        super().__init__(name, daemon, parent)

        self._child_service = child_service
        self.child_manager = AsyncioManager(child_service, loop)

    async def cancel(self) -> None:
        async with cleanup_tasks(self.asyncio_task):
            if self.child_manager.is_started:
                await self.child_manager.stop()


class AsyncioManager(BaseManager):
    _asyncio_tasks: Set["asyncio.Future[Any]"]

    def __init__(
        self, service: ServiceAPI, loop: asyncio.AbstractEventLoop = None
    ) -> None:
        super().__init__(service)

        self._loop = loop

        # events
        self._started = asyncio.Event()
        self._cancelled = asyncio.Event()
        self._finished = asyncio.Event()

        # locks
        self._run_lock = asyncio.Lock()

        # task tracking
        self._asyncio_tasks = set()

    #
    # System Tasks
    #
    async def _handle_cancelled(self) -> None:
        """
        When cancellation is requested this triggers the cancellation of all
        background tasks.
        """
        try:
            await self._real_handle_cancelled()
        except asyncio.CancelledError:
            raise
        except BaseException:
            self.logger.exception(
                "%s: unexpected error when cancelling tasks, service may not terminate",
                self,
            )
            raise

    async def _real_handle_cancelled(self) -> None:
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
        for task in tuple(self._root_tasks):
            msg = "%s: triggering cancellation of root task %s" % (self, task.name)
            if isinstance(task, TaskWithChildrenAPI):
                msg += " and all its children (%s)" % (
                    [child.name for child in task.children]
                )
            self.logger.debug(msg)
            try:
                await task.cancel()
            except Exception as e:
                # We log this because it may prevent us from reaching the end of run(), where
                # self._errors would be raised, and in that case we'd never see this error.
                self.logger.debug(
                    "Error cancelling task %s: %s. %s may fail to terminate",
                    task.name,
                    e,
                    self,
                )
                self._errors.append(cast(EXC_INFO, sys.exc_info()))
            else:
                self.logger.debug("%s: cancelled %s", self, task.name)

        for asyncio_task in tuple(self._asyncio_tasks):
            if not asyncio_task.done():
                raise LifecycleError("Should have already been completed")

            try:
                await asyncio_task
            except asyncio.CancelledError:
                pass

    @classmethod
    async def run_service(
        cls, service: ServiceAPI, loop: asyncio.AbstractEventLoop = None
    ) -> None:
        manager = cls(service, loop=loop)
        await manager.run()

    async def _wait_all_tasks_done(self) -> None:
        # In order to not have `asyncio` issue warnings we need to ensure that
        # all of the tasks we schedule with `ensure_future` get awaited.  Since
        # we don't care about the return values for these we can simply monitor
        # the set of pending tasks for ones that are already done and await
        # them to appease `asyncio`.
        while self._asyncio_tasks:
            done_tasks = tuple(task for task in self._asyncio_tasks if task.done())
            for task in done_tasks:
                self.logger.debug("%s: waiting for %s to finish", self, task)
                try:
                    await task
                except asyncio.CancelledError:
                    # suppressing the exception here is *ok* because the
                    # `CancelledError` is not important to the task lifecycle.
                    pass
                finally:
                    # Remove the finished tasks from tracking so that our
                    # memory footprint remains minimal.
                    self._asyncio_tasks.discard(task)

            if self._asyncio_tasks:
                await asyncio.wait(
                    self._asyncio_tasks, return_when=asyncio.FIRST_COMPLETED
                )

    async def run(self) -> None:
        if self._run_lock.locked():
            raise LifecycleError(
                "Cannot run a service with the run lock already engaged.  Already started?"
            )
        elif self.is_started:
            raise LifecycleError("Cannot run a service which is already started.")

        async with self._run_lock:
            handle_cancelled_task = asyncio.ensure_future(
                self._handle_cancelled(), loop=self._loop
            )

            async with cleanup_tasks(handle_cancelled_task):
                self._started.set()

                self.run_task(self._service.run)

                # This is hack to get the task stats correct.  We don't want to
                # count the `Service.run` method as a task.  This is still
                # imperfect as it will still count as a completed task when it
                # finishes.
                self._total_task_count = 0

                await self._wait_all_tasks_done()

        self._finished.set()
        self.logger.debug("%s: finished", self)

        # Above we rely on run_task() and handle_cancelled() to run the
        # service/tasks and swallow/collect exceptions so that they can be
        # reported all together here.
        if self.did_error:
            raise MultiError(
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

    async def wait_finished(self) -> None:
        await self._finished.wait()

    def _find_parent_task(
        self, asyncio_task: "asyncio.Future[Any]"
    ) -> Optional[TaskWithChildrenAPI]:
        """
        Find the :class:`async_service.asyncio.FunctionTask` instance that corresponds to
        the given :class:`asyncio.Task` instance.
        """
        for task in FunctionTask.iterate_tasks(*self._root_tasks):
            if asyncio_task is task.asyncio_task:
                return task

        else:
            # In the case that no tasks match we assume this is a new `root`
            # task and return `None` as the parent.
            return None

    def _schedule_task(self, task: TaskAPI) -> None:
        # No clean way to inform `mypy` without a `cast` that
        # `task.asyncio_task` is an allowed property
        task.asyncio_task = asyncio.ensure_future(  # type: ignore
            self._run_and_manage_task(task), loop=self._loop
        )
        self._asyncio_tasks.add(task.asyncio_task)  # type: ignore

    def _get_current_task(self) -> "asyncio.Future[Any]":
        current_asyncio_task = get_current_task()
        if current_asyncio_task is None:
            raise LifecycleError("Invariant: current asyncio task is None")
        return current_asyncio_task

    def run_task(
        self,
        async_fn: Callable[..., Awaitable[Any]],
        *args: Any,
        daemon: bool = False,
        name: str = None,
    ) -> None:
        current_asyncio_task = self._get_current_task()

        task = FunctionTask(
            name=get_task_name(async_fn, name),
            daemon=daemon,
            parent=self._find_parent_task(current_asyncio_task),
            async_fn=async_fn,
            async_fn_args=args,
        )
        self._common_run_task(task)

    def run_child_service(
        self, service: ServiceAPI, daemon: bool = False, name: str = None
    ) -> ManagerAPI:
        current_asyncio_task = self._get_current_task()

        task = ChildServiceTask(
            name=get_task_name(service, name),
            daemon=daemon,
            parent=self._find_parent_task(current_asyncio_task),
            child_service=service,
            loop=self._loop,
        )

        self._common_run_task(task)
        return task.child_manager


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
            raise LifecycleError(
                f"Cannot access external API {func}.  Service has not been run."
            )

        manager = self.get_manager()

        if not manager.is_running:
            raise LifecycleError(
                f"Cannot access external API {func}.  Service {self} is not running: "
            )

        func_task: "asyncio.Future[Any]" = asyncio.ensure_future(
            func(self, *args, **kwargs)
        )
        service_finished_task = asyncio.ensure_future(manager.wait_finished())

        done, pending = await asyncio.wait(
            (func_task, service_finished_task), return_when=asyncio.FIRST_COMPLETED
        )
        async with cleanup_tasks(*done, *pending):
            if func_task.done():
                return await func_task
            elif service_finished_task.done():
                raise LifecycleError(
                    f"Cannot access external API {func}.  Service {self} is not running: "
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
