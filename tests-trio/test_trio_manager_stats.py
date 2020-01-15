import pytest
import trio

from async_service import Service, background_trio_service


@pytest.mark.trio
async def test_trio_manager_stats():
    ready = trio.Event()

    class StatsTest(Service):
        async def run(self):
            # 2 that run forever
            self.manager.run_task(trio.sleep_forever)
            self.manager.run_task(trio.sleep_forever)

            # 2 that complete
            self.manager.run_task(trio.hazmat.checkpoint)
            self.manager.run_task(trio.hazmat.checkpoint)

            # 1 that spawns some children
            self.manager.run_task(self.run_with_children, 5)

        async def run_with_children(self, num_children):
            for _ in range(num_children):
                self.manager.run_task(trio.sleep_forever)
            ready.set()

    async with background_trio_service(StatsTest()) as manager:
        with trio.fail_after(1):
            await ready.wait()

        # we need to yield to the event loop a few times to allow the various
        # tasks to schedule themselves and get running.
        for _ in range(50):
            await trio.hazmat.checkpoint()

        assert manager.stats.tasks.total_count == 10
        assert manager.stats.tasks.finished_count == 2
        assert manager.stats.tasks.pending_count == 8

        # This is a simple test to ensure that finished tasks are removed from
        # tracking to prevent unbounded memory growth.
        assert len(manager._service_task_dag) == 9

    # now check after exiting
    assert manager.stats.tasks.total_count == 10
    assert manager.stats.tasks.finished_count == 10
    assert manager.stats.tasks.pending_count == 0


@pytest.mark.trio
async def test_trio_manager_stats_does_not_count_main_run_method():
    ready = trio.Event()

    class StatsTest(Service):
        async def run(self):
            self.manager.run_task(trio.sleep_forever)
            ready.set()

    async with background_trio_service(StatsTest()) as manager:
        with trio.fail_after(1):
            await ready.wait()

        # we need to yield to the event loop a few times to allow the various
        # tasks to schedule themselves and get running.
        for _ in range(10):
            await trio.hazmat.checkpoint()

        assert manager.stats.tasks.total_count == 1
        assert manager.stats.tasks.finished_count == 0
        assert manager.stats.tasks.pending_count == 1

    # now check after exiting
    assert manager.stats.tasks.total_count == 1
    assert manager.stats.tasks.finished_count == 1
    assert manager.stats.tasks.pending_count == 0
