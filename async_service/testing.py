from abc import abstractmethod
import asyncio
from typing import Any, Type, Union

import trio

from async_service import Service


class Resource:
    is_active = None
    was_checked = False

    def __str__(self) -> str:
        return f"<Resource[active={self.is_active}]>"

    async def __aenter__(self) -> 'Resource':
        self.is_active = True
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.is_active = False


class CheckParentAndChildTaskTerminationOrderService(Service):
    event_class: Union[Type[asyncio.Event], Type[trio.Event]]
    is_active = None
    did_check_is_active = False

    def __init__(self, parent_yield_count: int, child_yield_count: int) -> None:
        self._parent_yield_count = parent_yield_count
        self._child_yield_count = child_yield_count
        self._ready_to_cancel_a = self.event_class()
        self._ready_to_cancel_b = self.event_class()

    @abstractmethod
    async def yield_control(self, count: int) -> None:
        ...

    async def run(self) -> None:
        self.manager.run_task(self.do_simple_try_finally)
        self.manager.run_task(self.do_async_resource_check)
        await self._ready_to_cancel_a.wait()
        await self._ready_to_cancel_b.wait()
        self.manager.cancel()

    #
    # These two parent and child tasks ensure that a simple `try/finally` in a
    # parent task will only execute it's `finally` block **after** the child
    # task has finished.
    #
    async def do_simple_try_finally(self) -> None:
        self.is_active = True

        try:
            self.manager.run_task(self.child_of_simple_try_finally)
            await self.manager.wait_finished()
        finally:
            await self.yield_control(self._parent_yield_count)
            self.is_active = False
            assert self.did_check_is_active

    async def child_of_simple_try_finally(self) -> None:
        assert self.is_active
        try:
            self._ready_to_cancel_a.set()
            await self.manager.wait_finished()
        finally:
            await self.yield_control(self._child_yield_count)
            assert self.is_active
            self.did_check_is_active = True

    #
    #
    #
    async def do_async_resource_check(self) -> None:
        resource = Resource()
        try:
            async with resource:
                assert resource.is_active
                self.manager.run_task(self.child_resource_consumer, resource)
                await self.manager.wait_finished()
        finally:
            await self.yield_control(self._parent_yield_count)
            assert not resource.is_active
            assert resource.was_checked

    async def child_resource_consumer(self, resource: Resource) -> None:
        assert resource.is_active
        try:
            self._ready_to_cancel_b.set()
            await self.manager.wait_finished()
        finally:
            await self.yield_control(self._child_yield_count)
            assert resource.is_active
            resource.was_checked = True
