import asyncio

import pytest

from async_service import (
    LifecycleError,
    Service,
    background_asyncio_service,
)
from async_service.asyncio import (
    external_api,
)


class ExternalAPIService(Service):
    async def run(self):
        await self.manager.wait_finished()

    @external_api
    async def get_7(self, wait_return=None, signal_event=None):
        if signal_event is not None:
            signal_event.set()
        if wait_return is not None:
            await wait_return.wait()
        return 7


@pytest.mark.asyncio
async def test_asyncio_service_external_api_fails_before_start():
    service = ExternalAPIService()

    # should raise if the service has not yet been started.
    with pytest.raises(LifecycleError):
        await service.get_7()


@pytest.mark.asyncio
async def test_asyncio_service_external_api_works_while_running():
    service = ExternalAPIService()

    async with background_asyncio_service(service):
        assert await service.get_7() == 7


@pytest.mark.asyncio
async def test_asyncio_service_external_api_race_condition_done_and_cancelled():
    service = ExternalAPIService()

    async with background_asyncio_service(service) as manager:
        task = asyncio.ensure_future(service.get_7())

        # wait for the task to be done (but don't fetch the result yet)
        await asyncio.wait((task,))

        # now cancel the service
        manager.cancel()

        # Since the task was already done it should return the result even
        # though the service was just canceled
        assert await task == 7


@pytest.mark.asyncio
async def test_asyncio_service_external_api_raises_when_cancelled():
    service = ExternalAPIService()

    async with background_asyncio_service(service) as manager:
        # an event to ensure that we are indeed within the body of the
        is_within_fn = asyncio.Event()
        trigger_return = asyncio.Event()

        task = asyncio.ensure_future(
            service.get_7(wait_return=trigger_return, signal_event=is_within_fn)
        )

        # ensure we're within the body of the task.
        await is_within_fn.wait()

        # now cancel the service and trigger the return of the function.
        manager.cancel()

        # Since the task is in the middle of executing it wn't be done and thus
        # this should fail.  This should be hitting the `asyncio.wait(...)`
        # mechanism.
        with pytest.raises(LifecycleError):
            assert await task == 7

        # A direct call should also fail.  This *should* be hitting the early
        # return mechanism.
        with pytest.raises(LifecycleError):
            assert await service.get_7()


@pytest.mark.asyncio
async def test_asyncio_service_external_api_raises_when_finished():
    service = ExternalAPIService()

    async with background_asyncio_service(service) as manager:
        pass

    assert manager.is_finished
    # A direct call should also fail.  This *should* be hitting the early
    # return mechanism.
    with pytest.raises(LifecycleError):
        assert await service.get_7()


@pytest.mark.asyncio
async def test_asyncio_external_api_call_that_schedules_task():
    done = asyncio.Event()

    class MyService(Service):
        async def run(self):
            await self.manager.wait_finished()

        @external_api
        async def do_scheduling(self):
            self.manager.run_task(self.set_done)

        async def set_done(self):
            done.set()

    service = MyService()
    async with background_asyncio_service(service):
        await service.do_scheduling()
        await asyncio.wait_for(done.wait(), timeout=1)
