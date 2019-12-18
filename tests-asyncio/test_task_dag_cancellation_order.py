import asyncio

import pytest

from async_service import background_asyncio_service
from async_service.tools._dag_test import DAGServiceTest


class AsyncioDAGServiceTest(DAGServiceTest):
    event_class = asyncio.Event

    async def yield_execution(self, count):
        for _ in range(count):
            await asyncio.sleep(0)

    async def ready_cancel(self) -> None:
        await asyncio.gather(
            *(event.wait() for event in self._child_tasks_all_ready_events.values())
        )


@pytest.mark.asyncio
async def test_asyncio_service_task_cancellation_dag_order():
    # all of the assertions happen within the body of the service.
    service = AsyncioDAGServiceTest()
    assert service.sanity_flag is False
    async with background_asyncio_service(service):
        await service.ready_cancel()
    assert service.sanity_flag is True
