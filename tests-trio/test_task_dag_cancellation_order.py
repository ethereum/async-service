import trio

import pytest

from async_service import background_trio_service
from async_service.testing import _test_service_task_cancellation_dag_order


async def wait_events(events):
    async with trio.open_nursery() as nursery:
        for event in events:
            nursery.start_soon(event.wait)


@pytest.mark.trio
async def test_trio_service_task_cancellation_dag_order():
    await _test_service_task_cancellation_dag_order(
        trio.Event, trio.sleep, trio.Cancelled, background_trio_service,
        wait_events)
