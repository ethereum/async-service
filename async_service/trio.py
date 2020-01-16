import functools
import sys
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Hashable,
    Optional,
    Set,
    Tuple,
    TypeVar,
    cast,
)
import uuid

from async_generator import asynccontextmanager
import trio
import trio_typing

from ._utils import get_task_name, iter_dag
from .abc import ManagerAPI, ServiceAPI
from .base import BaseManager
from .exceptions import DaemonTaskExit, LifecycleError, ServiceCancelled
from .typing import EXC_INFO


class _Task(Hashable):
    _trio_task: Optional[trio.hazmat.Task] = None
    _cancel_scope: trio.CancelScope

    def __init__(
        self,
        name: str,
        daemon: bool,
        parent: Optional["_Task"],
        trio_task: trio.hazmat.Task = None,
    ) -> None:
        # For hashable interface.
        self._id = uuid.uuid4()

        # meta
        self.name = name
        self.daemon = daemon

        # We use an event to manually track when the child task is "done".
        # This is because trio has no API for awaiting completion of a task.
        self.done = trio.Event()

        # Each task gets its own `CancelScope` which is how we can manually
        # control cancellation order of the task DAG
        self.cancel_scope = trio.CancelScope()

        self.parent = parent

        self._trio_task = trio_task

    def __hash__(self) -> int:
        return hash(self._id)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, _Task):
            return False
        return self._id == other._id

    def __str__(self) -> str:
        return f"{self.name}[daemon={self.daemon}]"

    @property
    def has_trio_task(self) -> bool:
        return self._trio_task is not None

    @property
    def trio_task(self) -> trio.hazmat.Task:
        if self._trio_task is None:
            raise LifecycleError("Trio task not set yet")
        return self._trio_task

    @trio_task.setter
    def trio_task(self, value: trio.hazmat.Task) -> None:
        if self._trio_task is not None:
            raise LifecycleError(f"Task already set: {self._trio_task}")
        self._trio_task = value


class TrioManager(BaseManager):
    # A nursery for sub tasks and services.  This nursery is cancelled if the
    # service is cancelled but allowed to exit normally if the service exits.
    _task_nursery: trio_typing.Nursery

    _service_task_dag: Dict[_Task, Set[_Task]]

    def __init__(self, service: ServiceAPI) -> None:
        super().__init__(service)

        # events
        self._started = trio.Event()
        self._cancelled = trio.Event()
        self._stopping = trio.Event()
        self._finished = trio.Event()

        # locks
        self._run_lock = trio.Lock()

        # DAG tracking
        self._service_task_dag = {}

    #
    # System Tasks
    #
    async def _handle_cancelled(self) -> None:
        self.logger.debug("%s: _handle_cancelled waiting for cancellation", self)
        await self._cancelled.wait()
        self.logger.debug("%s: _handle_cancelled triggering task cancellation", self)

        # Here we iterate over the task DAG such that we cancel the most deeply
        # nested tasks first ensuring that each has finished completely before
        # we move onto cancelling the parent tasks.
        #
        # The `_service_task_dag` changes size as each task completes itself
        # and removes itself from the dag.  For this reason we compute the
        # cancellation order up front and then iterate over it so that we
        # ensure we have a reference to all of the tasks in the DAG.
        tasks_to_cancel = tuple(iter_dag(self._service_task_dag))
        for task in tasks_to_cancel:
            task.cancel_scope.cancel()
            await task.done.wait()

        # This finaly cancellation of the task nursery's cancel scope ensures
        # that nothing is left behind and that the service will reliably exit.
        self._task_nursery.cancel_scope.cancel()

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
                        async with trio.open_nursery() as task_nursery:
                            self._task_nursery = task_nursery

                            self._started.set()

                            self.run_task(self._service.run)

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
            self.logger.debug("%s: stopping", self)
            self._finished.set()
            self.logger.debug("%s: finished", self)

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
        elif not self.is_running:
            return
        else:
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
        task: _Task,
    ) -> None:
        self.logger.debug("%s: task %s running", self, task)

        task.trio_task = trio.hazmat.current_task()

        try:
            with task.cancel_scope:
                try:
                    await async_fn(*args)
                except Exception as err:
                    self.logger.debug(
                        "%s: task %s exited with error: %s",
                        self,
                        task,
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
                            task,
                        )
                        self.cancel()
                        raise DaemonTaskExit(f"Daemon task {name} exited")

                    self.logger.debug("%s: task %s cleaning up.", self, task)
                    # Now we wait for all of the child tasks to complete.
                    for child_task in tuple(self._service_task_dag[task]):
                        if not child_task.done.is_set():
                            await child_task.done.wait()

                    self.logger.debug("%s: task %s finished.", self, task)
        finally:
            # mark the task as being done and then remove it from the dag and
            # any set of dependencies that it may be part of.
            task.done.set()

            self._service_task_dag.pop(task)
            for dependencies in self._service_task_dag.values():
                dependencies.discard(task)
            self._done_task_count += 1

    def _get_parent_task(self, trio_task: trio.hazmat.Task) -> Optional[_Task]:
        """
        Find the :class:`async_service.trio._Task` instance that corresponds to
        the given :class:`trio.hazmat.Task` instance.
        """
        for task in self._service_task_dag:
            # Any task that has not had its `trio_task` set can be safely
            # skipped as those are still in the process of starting up which
            # means that they cannot be the parent task since they will not
            # have had a chance to schedule an child tasks.
            if not task.has_trio_task:
                continue
            elif trio_task is task.trio_task:
                return task
        else:
            # In the case that no tasks match we assume this is a new `root`
            # task and return `None` as the parent.
            return None

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

        task = _Task(
            name=task_name,
            daemon=daemon,
            parent=self._get_parent_task(trio.hazmat.current_task()),
        )

        if task.parent is None:
            self.logger.debug("%s: new root task %s added to DAG", self, task)
        else:
            self.logger.debug(
                "%s: new child task %s -> %s added to DAG", self, task.parent, task
            )
            self._service_task_dag[task.parent].add(task)

        self._service_task_dag[task] = set()
        self._total_task_count += 1

        self._task_nursery.start_soon(
            functools.partial(
                self._run_and_manage_task, daemon=daemon, name=task_name, task=task
            ),
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
        return child_manager


TFunc = TypeVar("TFunc", bound=Callable[..., Coroutine[Any, Any, Any]])


_ChannelPayload = Tuple[Optional[Any], Optional[BaseException]]


async def _wait_stopping_or_finished(
    service: ServiceAPI,
    api_func: Callable[..., Any],
    channel: trio.abc.SendChannel[_ChannelPayload],
) -> None:
    manager = service.get_manager()

    if manager.is_stopping or manager.is_finished:
        await channel.send(
            (
                None,
                ServiceCancelled(
                    f"Cannot access external API {api_func}.  Service is not running: "
                    f"started={manager.is_started}  running={manager.is_running} "
                    f"stopping={manager.is_stopping}  finished={manager.is_finished}"
                ),
            )
        )
        return

    await manager.wait_stopping()
    await channel.send(
        (
            None,
            ServiceCancelled(
                f"Cannot access external API {api_func}.  Service is not running: "
                f"started={manager.is_started}  running={manager.is_running} "
                f"stopping={manager.is_stopping}  finished={manager.is_finished}"
            ),
        )
    )


async def _wait_api_fn(
    self: ServiceAPI,
    api_fn: Callable[..., Any],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    channel: trio.abc.SendChannel[_ChannelPayload],
) -> None:
    try:
        result = await api_fn(self, *args, **kwargs)
    except Exception:
        _, exc_value, exc_tb = sys.exc_info()
        if exc_value is None or exc_tb is None:
            raise Exception(
                "This should be unreachable but acts as a type guard for mypy"
            )
        await channel.send((None, exc_value.with_traceback(exc_tb)))
    else:
        await channel.send((result, None))


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

        channels: Tuple[
            trio.abc.SendChannel[_ChannelPayload],
            trio.abc.ReceiveChannel[_ChannelPayload],
        ] = trio.open_memory_channel(0)
        send_channel, receive_channel = channels

        async with trio.open_nursery() as nursery:
            # mypy's type hints for start_soon break with this invocation.
            nursery.start_soon(
                _wait_api_fn, self, func, args, kwargs, send_channel  # type: ignore
            )
            nursery.start_soon(_wait_stopping_or_finished, self, func, send_channel)
            result, err = await receive_channel.receive()
            nursery.cancel_scope.cancel()
        if err is None:
            return result
        else:
            raise err

    return cast(TFunc, inner)


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
