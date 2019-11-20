import asyncio

import pytest

from async_service import Service, background_asyncio_service


@pytest.mark.asyncio
async def test_asyncio_service_task_cancellation_dag_order():
    dag = {
        0: (1, 2, 3),
        1: (),
        2: (4, 5),
        3: (),
        4: (6, 7, 8, 9, 10),
        5: (),
        6: (),
        7: (),
        8: (11,),
        9: (),
        10: (),
        11: (12,),
        12: (13,),
        13: (),
    }
    cancelled = []

    class ServiceTest(Service):
        def __init__(self):
            self._task_events = {task_id: asyncio.Event() for task_id in dag.keys()}

        async def _do_task(self, task_id):
            children = dag[task_id]
            for child_id in children:
                self.manager.run_task(self._do_task, child_id)

            # yield for a moment to give these time to start
            await asyncio.sleep(0)
            self._task_events[task_id].set()
            try:
                await self.manager.wait_finished()
            except asyncio.CancelledError:
                cancelled.append(task_id)
                raise

        async def run(self):
            await self._do_task(0)

    service = ServiceTest()
    async with background_asyncio_service(service) as manager:
        await asyncio.gather(*(event.wait() for event in service._task_events.values()))
        manager.cancel()

    assert len(cancelled) == len(dag)

    seen = set()
    for value in cancelled:
        assert value not in seen
        children = dag[value]
        assert seen.issuperset(children)
        seen.add(value)
