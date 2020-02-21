import asyncio

import pytest
from trio import MultiError

from async_service import (
    AsyncioManager,
    DaemonTaskExit,
    LifecycleError,
    Service,
    as_service,
    background_asyncio_service,
)


class WaitCancelledService(Service):
    async def run(self) -> None:
        await self.manager.wait_finished()


async def do_service_lifecycle_check(
    manager, manager_run_fn, trigger_exit_condition_fn, should_be_cancelled
):
    assert manager.is_started is False
    assert manager.is_running is False
    assert manager.is_cancelled is False
    assert manager.is_finished is False

    asyncio.ensure_future(manager_run_fn())

    await asyncio.wait_for(manager.wait_started(), timeout=0.1)

    assert manager.is_started is True
    assert manager.is_running is True
    assert manager.is_cancelled is False
    assert manager.is_finished is False

    # trigger the service to exit
    trigger_exit_condition_fn()

    await asyncio.wait_for(manager.wait_finished(), timeout=0.1)

    if should_be_cancelled:
        assert manager.is_started is True
        # We cannot determine whether the service should be running at this
        # stage because a service is considered running until it is marked as
        # finished.  Since it may be cancelled but still not finished we
        # can't know.
        assert manager.is_cancelled is True
        # The service will either still register as *running* if it is in the
        # process of shutting down, or *finished* if it has finished shutting
        # down.
        assert manager.is_running is True or manager.is_finished is True
        # We cannot determine whether a service should be finished at this
        # stage as it could have exited cleanly and is now finished or it
        # might be doing some cleanup after which it will register as being
        # finished.

    assert manager.is_started is True
    assert manager.is_running is False
    assert manager.is_cancelled is should_be_cancelled
    assert manager.is_finished is True


def test_service_manager_initial_state():
    service = WaitCancelledService()
    manager = AsyncioManager(service)

    assert manager.is_started is False
    assert manager.is_running is False
    assert manager.is_cancelled is False
    assert manager.is_finished is False


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_clean_exit():
    trigger_exit = asyncio.Event()

    @as_service
    async def ServiceTest(manager):
        await trigger_exit.wait()

    service = ServiceTest()
    manager = AsyncioManager(service)

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=manager.run,
        trigger_exit_condition_fn=trigger_exit.set,
        should_be_cancelled=False,
    )


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_external_cancellation():
    @as_service
    async def ServiceTest(manager):
        while True:
            await asyncio.sleep(0)

    service = ServiceTest()
    manager = AsyncioManager(service)

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=manager.run,
        trigger_exit_condition_fn=manager.cancel,
        should_be_cancelled=True,
    )


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_exception():
    trigger_error = asyncio.Event()

    @as_service
    async def ServiceTest(manager):
        await trigger_error.wait()
        raise RuntimeError("Service throwing error")

    service = ServiceTest()
    manager = AsyncioManager(service)

    async def do_service_run():
        with pytest.raises(RuntimeError, match="Service throwing error"):
            await manager.run()

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=do_service_run,
        trigger_exit_condition_fn=trigger_error.set,
        should_be_cancelled=True,
    )


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_task_exception():
    trigger_error = asyncio.Event()

    @as_service
    async def ServiceTest(manager):
        async def task_fn():
            await trigger_error.wait()
            raise RuntimeError("Service throwing error")

        manager.run_task(task_fn)

    service = ServiceTest()
    manager = AsyncioManager(service)

    async def do_service_run():
        with pytest.raises(RuntimeError, match="Service throwing error"):
            await manager.run()

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=do_service_run,
        trigger_exit_condition_fn=trigger_error.set,
        should_be_cancelled=True,
    )


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_daemon_task_exit():
    trigger_error = asyncio.Event()

    @as_service
    async def ServiceTest(manager):
        async def daemon_task_fn():
            await trigger_error.wait()

        manager.run_daemon_task(daemon_task_fn)

    service = ServiceTest()
    manager = AsyncioManager(service)

    async def do_service_run():
        with pytest.raises(DaemonTaskExit, match="Daemon task"):
            await manager.run()

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=do_service_run,
        trigger_exit_condition_fn=trigger_error.set,
        should_be_cancelled=True,
    )


@pytest.mark.asyncio
async def test_error_in_service_run():
    class ServiceTest(Service):
        async def run(self):
            self.manager.run_daemon_task(self.manager.wait_finished)
            raise ValueError("Exception inside run()")

    with pytest.raises(ValueError):
        await AsyncioManager.run_service(ServiceTest())


@pytest.mark.asyncio
async def test_daemon_task_finishes_leaving_children():
    class ServiceTest(Service):
        async def sleep_and_fail(self):
            await asyncio.sleep(1)
            raise AssertionError(
                "This should not happen as the task should be cancelled"
            )

        async def buggy_daemon(self):
            self.manager.run_task(self.sleep_and_fail)

        async def run(self):
            self.manager.run_daemon_task(self.buggy_daemon)

    with pytest.raises(DaemonTaskExit):
        await AsyncioManager.run_service(ServiceTest())


@pytest.mark.asyncio
async def test_multierror_in_run():
    # This test should cause ServiceTest to raise a trio.MultiError containing two exceptions --
    # one raised inside its run() method and another raised by the daemon task exiting early.
    trigger_error = asyncio.Event()

    class ServiceTest(Service):
        async def run(self):
            ready = asyncio.Event()
            self.manager.run_task(self.error_fn, ready)
            await ready.wait()
            trigger_error.set()
            raise RuntimeError("Exception inside Service.run()")

        async def error_fn(self, ready):
            ready.set()
            await trigger_error.wait()
            raise ValueError("Exception inside error_fn")

    with pytest.raises(MultiError) as exc_info:
        await AsyncioManager.run_service(ServiceTest())

    exc = exc_info.value
    assert len(exc.exceptions) == 2
    assert isinstance(exc.exceptions[0], RuntimeError)
    assert isinstance(exc.exceptions[1], ValueError)


@pytest.mark.asyncio
async def test_asyncio_service_background_service_context_manager():
    service = WaitCancelledService()

    async with background_asyncio_service(service) as manager:
        # ensure the manager property is set.
        assert hasattr(service, "manager")
        assert service.manager is manager

        assert manager.is_started is True
        assert manager.is_running is True
        assert manager.is_cancelled is False
        assert manager.is_finished is False

    assert manager.is_started is True
    assert manager.is_running is False
    assert manager.is_cancelled is True
    assert manager.is_finished is True


@pytest.mark.asyncio
async def test_asyncio_service_manager_stop():
    service = WaitCancelledService()

    async with background_asyncio_service(service) as manager:
        assert manager.is_started is True
        assert manager.is_running is True
        assert manager.is_cancelled is False
        assert manager.is_finished is False

        await manager.stop()

        assert manager.is_started is True
        assert manager.is_running is False
        assert manager.is_cancelled is True
        assert manager.is_finished is True


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_task():
    task_event = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def task_fn():
            task_event.set()

        manager.run_task(task_fn)
        await manager.wait_finished()

    async with background_asyncio_service(RunTaskService()):
        await asyncio.wait_for(task_event.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_task_waits_for_task_completion():
    task_event = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def task_fn():
            await asyncio.sleep(0.01)
            task_event.set()

        manager.run_task(task_fn)
        # the task is set to run in the background but then  the service exits.
        # We want to be sure that the task is allowed to continue till
        # completion unless explicitely cancelled.

    async with background_asyncio_service(RunTaskService()):
        await asyncio.wait_for(task_event.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_task_can_still_cancel_after_run_finishes():
    task_event = asyncio.Event()
    service_finished = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def task_fn():
            # this will never complete
            await task_event.wait()

        manager.run_task(task_fn)
        # the task is set to run in the background but then  the service exits.
        # We want to be sure that the task is allowed to continue till
        # completion unless explicitely cancelled.
        service_finished.set()

    async with background_asyncio_service(RunTaskService()) as manager:
        await asyncio.wait_for(service_finished.wait(), timeout=0.01)

        # show that the service hangs waiting for the task to complete.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(manager.wait_finished(), timeout=0.01)

        # trigger cancellation and see that the service actually stops
        manager.cancel()
        await asyncio.wait_for(manager.wait_finished(), timeout=0.01)


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_task_reraises_exceptions():
    task_event = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def task_fn():
            await task_event.wait()
            raise Exception("task exception in run_task")

        manager.run_task(task_fn)
        await asyncio.wait_for(asyncio.sleep(100), timeout=1)

    with pytest.raises(BaseException, match="task exception in run_task"):
        async with background_asyncio_service(RunTaskService()) as manager:
            task_event.set()
            await manager.wait_finished()
            pass


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_daemon_task_cancels_if_exits():
    task_event = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def daemon_task_fn():
            await task_event.wait()

        manager.run_daemon_task(daemon_task_fn, name="daemon_task_fn")
        await asyncio.wait_for(asyncio.sleep(100), timeout=1)

    with pytest.raises(
        DaemonTaskExit, match=r"Daemon task daemon_task_fn\[daemon=True\] exited"
    ):
        async with background_asyncio_service(RunTaskService()) as manager:
            task_event.set()
            await manager.wait_finished()


@pytest.mark.asyncio
async def test_asyncio_service_manager_propogates_and_records_exceptions():
    @as_service
    async def ThrowErrorService(manager):
        raise RuntimeError("this is the error")

    service = ThrowErrorService()
    manager = AsyncioManager(service)

    assert manager.did_error is False

    with pytest.raises(RuntimeError, match="this is the error"):
        await manager.run()

    assert manager.did_error is True


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_clean_exit_with_child_service():
    trigger_exit = asyncio.Event()

    @as_service
    async def ChildServiceTest(manager):
        await trigger_exit.wait()

    @as_service
    async def ServiceTest(manager):
        child_manager = manager.run_child_service(ChildServiceTest())
        await child_manager.wait_started()

    service = ServiceTest()
    manager = AsyncioManager(service)

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=manager.run,
        trigger_exit_condition_fn=trigger_exit.set,
        should_be_cancelled=False,
    )


@pytest.mark.asyncio
async def test_asyncio_service_with_daemon_child_service():
    ready = asyncio.Event()

    @as_service
    async def ChildServiceTest(manager):
        await manager.wait_finished()

    @as_service
    async def ServiceTest(manager):
        child_manager = manager.run_daemon_child_service(ChildServiceTest())
        await child_manager.wait_started()
        ready.set()
        await manager.wait_finished()

    service = ServiceTest()
    async with background_asyncio_service(service):
        await ready.wait()


@pytest.mark.asyncio
async def test_asyncio_service_with_daemon_child_task():
    ready = asyncio.Event()
    started = asyncio.Event()

    async def _task():
        started.set()
        await asyncio.sleep(100)

    @as_service
    async def ServiceTest(manager):
        manager.run_daemon_task(_task)
        await started.wait()
        ready.set()
        await manager.wait_finished()

    service = ServiceTest()
    async with background_asyncio_service(service):
        await ready.wait()


@pytest.mark.asyncio
async def test_asyncio_service_with_async_generator():
    is_within_agen = asyncio.Event()

    async def do_agen():
        while True:
            yield

    @as_service
    async def ServiceTest(manager):
        async for _ in do_agen():  # noqa: F841
            await asyncio.sleep(0)
            is_within_agen.set()

    async with background_asyncio_service(ServiceTest()) as manager:
        await is_within_agen.wait()
        manager.cancel()


@pytest.mark.asyncio
async def test_asyncio_service_disallows_task_scheduling_when_not_running():
    class ServiceTest(Service):
        async def run(self):
            await self.manager.wait_finished()

        def do_schedule(self):
            self.manager.run_task(asyncio.sleep, 1)

    service = ServiceTest()

    async with background_asyncio_service(service):
        service.do_schedule()

    with pytest.raises(LifecycleError):
        service.do_schedule()


@pytest.mark.asyncio
async def test_asyncio_service_allows_task_scheduling_after_cancel():
    # We need to ensure that task scheduling after a call to cancel still
    # works.  The scheduled tasks won't end up running but we can't prevent
    # this race condition from occuring during shutdown.
    @as_service
    async def ServiceTest(manager):
        manager.cancel()
        manager.run_task(asyncio.sleep, 1)

    await AsyncioManager.run_service(ServiceTest())


@pytest.mark.asyncio
async def test_asyncio_service_with_try_finally_cleanup():
    ready_cancel = asyncio.Event()

    class TryFinallyService(Service):
        cleanup_up = False

        async def run(self) -> None:
            try:
                ready_cancel.set()
                await self.manager.wait_finished()
            finally:
                self.cleanup_up = True

    service = TryFinallyService()
    async with background_asyncio_service(service) as manager:
        await ready_cancel.wait()
        assert not service.cleanup_up
        manager.cancel()
    assert service.cleanup_up


@pytest.mark.asyncio
async def test_asyncio_service_with_try_finally_cleanup_with_await():
    ready_cancel = asyncio.Event()

    class TryFinallyService(Service):
        cleanup_up = False

        async def run(self) -> None:
            try:
                ready_cancel.set()
                await self.manager.wait_finished()
            finally:
                await asyncio.sleep(0)
                self.cleanup_up = True

    service = TryFinallyService()
    async with background_asyncio_service(service) as manager:
        await ready_cancel.wait()
        assert not service.cleanup_up
        manager.cancel()
    assert service.cleanup_up
