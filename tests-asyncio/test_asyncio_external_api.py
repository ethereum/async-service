import asyncio

import pytest

from async_service import Service, ServiceCancelled, background_asyncio_service
from async_service.asyncio import external_api


@pytest.mark.asyncio
async def test_asyncio_service_external_api_raises_ServiceCancelled():
    class ServiceTest(Service):
        async def run(self):
            await self.manager.wait_finished()

        @external_api
        async def get_7(self):
            await asyncio.sleep(1)
            return 7

    service = ServiceTest()
    async with background_asyncio_service(service) as manager:
        manager.cancel()

    with pytest.raises(ServiceCancelled):
        await service.get_7()
