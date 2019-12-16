from async_generator import asynccontextmanager

from async_service import Service


async def _test_service_task_cancellation_dag_order(
        event_class, sleep_fn, cancelled_class, background_service_fn, wait_events_fn):
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
    # finished = []
    cancelled = []

    class ServiceTest(Service):
        def __init__(self):
            self._task_events = {task_id: event_class() for task_id in dag.keys()}

        async def _do_task(self, task_id):
            children = dag[task_id]
            for child_id in children:
                self.manager.run_task(self._do_task, child_id, name=f'task-{child_id}')

            # yield for a moment to give these time to start
            await sleep_fn(0)
            self._task_events[task_id].set()
            self.manager.logger.debug("Task %d started", task_id)
            try:
                await self.manager.wait_finished()
            except cancelled_class:
                self.manager.logger.debug("Task %d cancelled", task_id)
                cancelled.append(task_id)
                raise
            finally:
                # XXX: On trio, having this sleep here causes the whole service to terminate
                # before any tasks.
                # await sleep_fn(0)
                # finished.append(task_id)
                pass

        async def run(self):
            await self._do_task(0)

    service = ServiceTest()
    async with background_service_fn(service) as manager:
        await wait_events_fn(service._task_events.values())
        manager.cancel()

    # assert len(finished) == len(dag)
    assert len(cancelled) == len(dag)

    seen = set()
    for value in cancelled:
        assert value not in seen
        children = dag[value]
        assert seen.issuperset(children)
        seen.add(value)


class Resource:
    is_active = True


class CheckParentAndChildTaskTerminationOrderService(Service):
    cancelled_class = None

    async def sleep(self, delay):
        raise NotImplementedError()

    async def run(self):
        self.manager.run_task(self.setup_and_consume_res)
        # Sleep more than 0 seconds to ensure all our tasks get a chance to run.
        await self.sleep(0.1)
        self.manager.cancel()

    async def setup_and_consume_res(self):
        async with self.get_resource() as res:
            self.manager.run_task(self.consume_res, res)
            await self.sleep(0)
            await self.manager.wait_finished()

    async def consume_res(self, res):
        assert res.is_active
        try:
            await self.manager.wait_finished()
        finally:
            assert res.is_active

    @asynccontextmanager
    async def get_resource(self):
        res = Resource()
        yield res
        res.active = False
