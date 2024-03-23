from importlib.metadata import (
    version as __version,
)

from .abc import (
    ManagerAPI,
    ServiceAPI,
)
from .asyncio import (
    AsyncioManager,
    background_asyncio_service,
    external_api as external_asyncio_api,
)
from .base import (
    Service,
    as_service,
)
from .exceptions import (
    DaemonTaskExit,
    LifecycleError,
    TooManyChildrenException,
)
from .trio import (
    TrioManager,
    background_trio_service,
    external_api as external_trio_api,
)

run_asyncio_service = AsyncioManager.run_service
run_trio_service = TrioManager.run_service

__version__ = __version("async-service")
