from .abc import ManagerAPI, ServiceAPI  # noqa: F401
from .asyncio import (  # noqa: F401
    AsyncioManager,
    background_asyncio_service,
    external_api as external_asyncio_api,
)
from .base import Service, as_service  # noqa: F401
from .exceptions import DaemonTaskExit, LifecycleError, ServiceCancelled  # noqa: F401
from .trio import TrioManager, background_trio_service  # noqa: F401

run_asyncio_service = AsyncioManager.run_service
run_trio_service = TrioManager.run_service
