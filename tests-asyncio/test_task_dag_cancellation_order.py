import asyncio

import pytest

from async_service import background_asyncio_service
from async_service.testing import _test_service_task_cancellation_dag_order


async def wait_events(events):
    await asyncio.gather(*(event.wait() for event in events))


@pytest.mark.asyncio
async def test_asyncio_service_task_cancellation_dag_order():
    await _test_service_task_cancellation_dag_order(
        asyncio.Event, asyncio.sleep, asyncio.CancelledError, background_asyncio_service,
        wait_events)
