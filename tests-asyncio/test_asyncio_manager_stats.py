import asyncio
import pytest

from async_service import (
    Service,
    background_asyncio_service,
)


@pytest.mark.asyncio
async def test_asyncio_manager_stats():
    ready = asyncio.Event()

    class StatsTest(Service):
        async def run(self):
            # 2 that run forever
            self.manager.run_task(asyncio.sleep, 100)
            self.manager.run_task(asyncio.sleep, 100)

            # 2 that complete
            self.manager.run_task(asyncio.sleep, 0)
            self.manager.run_task(asyncio.sleep, 0)

            # 1 that spawns some children
            self.manager.run_task(self.run_with_children, 4)

        async def run_with_children(self, num_children):
            for _ in range(num_children):
                self.manager.run_task(asyncio.sleep, 100)
            ready.set()

        def run_external_root(self):
            self.manager.run_task(asyncio.sleep, 0)

    service = StatsTest()
    async with background_asyncio_service(service) as manager:
        service.run_external_root()
        assert len(manager._root_tasks) == 2
        await asyncio.wait_for(ready.wait(), timeout=1)

        # we need to yield to the event loop a few times to allow the various
        # tasks to schedule themselves and get running.
        for _ in range(50):
            await asyncio.sleep(0)

        assert manager.stats.tasks.total_count == 10
        assert manager.stats.tasks.finished_count == 3
        assert manager.stats.tasks.pending_count == 7

        # This is a simple test to ensure that finished tasks are removed from
        # tracking to prevent unbounded memory growth.
        assert len(manager._root_tasks) == 1

    # now check after exiting
    assert manager.stats.tasks.total_count == 10
    assert manager.stats.tasks.finished_count == 10
    assert manager.stats.tasks.pending_count == 0


# This test accounts for a current deficiency in the stats tracking that will
# count the `Service.run` method in the statistics.
@pytest.mark.asyncio
async def test_asyncio_manager_stats_does_not_count_main_run_method():
    ready = asyncio.Event()

    class StatsTest(Service):
        async def run(self):
            self.manager.run_task(asyncio.sleep, 100)
            ready.set()

    async with background_asyncio_service(StatsTest()) as manager:
        await asyncio.wait_for(ready.wait(), timeout=1)

        # we need to yield to the event loop a few times to allow the various
        # tasks to schedule themselves and get running.
        for _ in range(10):
            await asyncio.sleep(0)

        assert manager.stats.tasks.total_count == 1
        assert manager.stats.tasks.finished_count == 0
        assert manager.stats.tasks.pending_count == 1

    # now check after exiting
    assert manager.stats.tasks.total_count == 1
    assert manager.stats.tasks.finished_count == 1
    assert manager.stats.tasks.pending_count == 0
