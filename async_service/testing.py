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
                self.manager.logger.info("Task %d cancelled", task_id)
                cancelled.append(task_id)
                raise

        async def run(self):
            await self._do_task(0)

    service = ServiceTest()
    async with background_service_fn(service) as manager:
        await wait_events_fn(service._task_events.values())
        manager.cancel()

    assert len(cancelled) == len(dag)

    seen = set()
    for value in cancelled:
        assert value not in seen
        children = dag[value]
        assert seen.issuperset(children)
        seen.add(value)