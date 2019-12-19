import pytest
import trio

from async_service import background_trio_service
from async_service.tools._dag_test import DAGServiceTest


class TrioDAGServiceTest(DAGServiceTest):
    event_class = trio.Event

    async def yield_execution(self, count):
        with trio.CancelScope(shield=True):
            for _ in range(count):
                await trio.hazmat.checkpoint()

    async def ready_cancel(self) -> None:
        for event in self._child_tasks_all_ready_events.values():
            await event.wait()


@pytest.mark.trio
async def test_trio_service_task_cancellation_dag_order():
    # all of the assertions happen within the body of the service.
    service = TrioDAGServiceTest()
    assert service.all_checks_passed is False
    async with background_trio_service(service):
        await service.ready_cancel()
    assert service.all_checks_passed is True
