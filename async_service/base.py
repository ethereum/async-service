import logging
from typing import Any, Awaitable, Callable, List, Type

from .abc import ManagerAPI, ServiceAPI, _InternalManagerAPI
from .exceptions import LifecycleError
from .typing import EXC_INFO


class Service(ServiceAPI):
    pass


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


class BaseManager(_InternalManagerAPI):
    logger = logging.getLogger("p2p.service.Manager")

    _service: ServiceAPI

    _errors: List[EXC_INFO]

    def __init__(self, service: ServiceAPI) -> None:
        if hasattr(service, "manager"):
            raise LifecycleError("Service already has a manager.")
        else:
            service.manager = self

        self._service = service

        # errors
        self._errors = []

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
