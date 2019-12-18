from async_generator import asynccontextmanager

from async_service import Service


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
