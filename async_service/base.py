import logging
from typing import Any, Awaitable, Callable, List, Type

from .abc import InternalManagerAPI, ManagerAPI, ServiceAPI
from .exceptions import LifecycleError
from .stats import Stats, TaskStats
from .typing import EXC_INFO


class Service(ServiceAPI):
    @property
    def manager(self) -> "InternalManagerAPI":
        """
        Expose the manager as a property here intead of
        :class:`async_service.abc.ServiceAPI` to ensure that anyone using
        proper type hints will not have access to this property since it isn't
        part of that API, while still allowing all subclasses of the
        :class:`async_service.base.Service` to access this property directly.
        """
        return self._manager

    def get_manager(self) -> ManagerAPI:
        try:
            return self._manager
        except AttributeError:
            raise LifecycleError(
                "Service does not have a manager assigned to it.  Are you sure "
                "it is running?"
            )


LogicFnType = Callable[..., Awaitable[Any]]


def as_service(service_fn: LogicFnType) -> Type[ServiceAPI]:
    """
    Create a service out of a simple function
    """

    class _Service(Service):
        def __init__(self, *args: Any, **kwargs: Any):
            self._args = args
            self._kwargs = kwargs

        async def run(self) -> None:
            await service_fn(self.manager, *self._args, **self._kwargs)

    _Service.__name__ = service_fn.__name__
    _Service.__doc__ = service_fn.__doc__
    return _Service


class BaseManager(InternalManagerAPI):
    logger = logging.getLogger("async_service.Manager")

    _service: ServiceAPI

    _errors: List[EXC_INFO]

    def __init__(self, service: ServiceAPI) -> None:
        if hasattr(service, "_manager"):
            raise LifecycleError("Service already has a manager.")
        else:
            service._manager = self

        self._service = service

        # errors
        self._errors = []

        # stats
        self._total_task_count = 0
        self._done_task_count = 0

    def __str__(self) -> str:
        return (
            "<Manager  "
            f"service={self._service}  "
            f"started={self.is_started}  "
            f"running={self.is_running}  "
            f"cancelled={self.is_cancelled}  "
            f"stopping={self.is_stopping}  "
            f"finished={self.is_finished}  "
            f"did_error={self.did_error}"
            ">"
        )

    #
    # Event API mirror
    #
    @property
    def is_running(self) -> bool:
        return self.is_started and not (self.is_stopping or self.is_finished)

    @property
    def did_error(self) -> bool:
        return len(self._errors) > 0

    #
    # Control API
    #
    async def stop(self) -> None:
        self.cancel()
        await self.wait_finished()

    #
    # Wait API
    #
    def run_daemon_task(
        self, async_fn: Callable[..., Awaitable[Any]], *args: Any, name: str = None
    ) -> None:

        self.run_task(async_fn, *args, daemon=True, name=name)

    def run_daemon_child_service(
        self, service: ServiceAPI, name: str = None
    ) -> ManagerAPI:
        return self.run_child_service(service, daemon=True, name=name)

    @property
    def stats(self) -> Stats:
        # The `max` call here ensures that if this is called prior to the
        # `Service.run` method starting we don't return `-1`
        total_count = max(0, self._total_task_count - 1)

        # Since we track `Service.run` as a task, the `min` call here ensures
        # that when the service is fully done that we don't represent the
        # `Service.run` method in this count.
        finished_count = min(total_count, self._done_task_count)
        return Stats(
            tasks=TaskStats(total_count=total_count, finished_count=finished_count)
        )
