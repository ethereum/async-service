from .abc import (  # noqa: F401
    ManagerAPI,
    ServiceAPI,
)
from .asyncio import (  # noqa: F401
    AsyncioManager,
    background_asyncio_service,
    external_api as external_asyncio_api,
)
from .base import (  # noqa: F401
    Service,
    as_service,
)
from .exceptions import (  # noqa: F401
    DaemonTaskExit,
    LifecycleError,
    TooManyChildrenException,
)
from .trio import (  # noqa: F401
    TrioManager,
    background_trio_service,
    external_api as external_trio_api,
)

run_asyncio_service = AsyncioManager.run_service
run_trio_service = TrioManager.run_service
