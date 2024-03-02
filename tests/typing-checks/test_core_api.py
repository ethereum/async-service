from async_service import (
    Service,
)
from async_service.asyncio import (
    external_api,
)


class MyService(Service):
    async def run(self) -> None:
        pass

    @external_api
    async def return_7(self) -> int:
        return 7


async def ensure_type_external_api() -> int:
    service = MyService()

    return await service.return_7()
