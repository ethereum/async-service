from abc import abstractmethod
import asyncio
import logging
import random
from typing import Any, Dict, Tuple, Type, Union

import trio

from async_service import Service

DAG = Dict[int, Tuple[int, ...]]

DEFAULT_DAG: DAG = {
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


class Resource:
    is_active = None
    was_checked = False

    async def __aenter__(self) -> "Resource":
        self.is_active = True
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.is_active = False


class DAGServiceTest(Service):
    """
    This service is for testing whether the task DAG gets shutdown in the
    correct order.
    """

    all_checks_passed = False

    logger = logging.getLogger("async_service.testing.DAGServiceTest")

    event_class: Union[Type[trio.Event], Type[asyncio.Event]]

    @abstractmethod
    async def yield_execution(self, count: int) -> None:
        ...

    @abstractmethod
    async def ready_cancel(self) -> None:
        ...

    def __init__(self, dag: DAG = None):
        if dag is None:
            self._dag = DEFAULT_DAG
        else:
            self._dag = dag

        # A set of tasks that signal that all of the dag child tasks are at a
        # state in which they can be cancelled.
        self._child_tasks_all_ready_events = {
            task_id: self.event_class() for task_id in self._dag.keys()
        }
        self._task_resources = {task_id: Resource() for task_id in self._dag.keys()}

    async def _do_task(self, task_id: int, resource: Resource) -> None:
        self.logger.info("task-%d: START", task_id)
        assert not resource.is_active
        assert not resource.was_checked
        try:
            async with resource:
                assert resource.is_active
                # run the children of this task
                self.manager.run_task(
                    self._do_child, task_id, resource, name=f"child-{task_id}"
                )
                await self.manager.wait_finished()
        finally:
            self.logger.info("task-%d: EXITING", task_id)
            # we yield execution a random number of times to ensure that the
            # order that these tasks were cued doesn't effect the desired
            # functionality.
            await self.yield_execution(random.randint(0, 100))
            assert not resource.is_active
            assert resource.was_checked
            self.logger.info("task-%d: FINISH", task_id)

    async def _do_child(self, task_id: int, resource: Resource) -> None:
        self.logger.info("child-%d: START", task_id)
        assert resource.is_active
        assert not resource.was_checked

        try:
            await self._run_children(task_id)
            self._child_tasks_all_ready_events[task_id].set()
            self.logger.info("child-%d: RUNNING", task_id)
            await self.manager.wait_finished()
        finally:
            self.logger.info("child-%d: EXITING", task_id)
            # we yield execution a random number of times to ensure that the
            # order that these tasks were cued doesn't effect the desired
            # functionality.
            await self.yield_execution(random.randint(0, 100))
            assert resource.is_active
            resource.was_checked = True
            self.logger.info("child-%d: FINISHED", task_id)

    async def _run_children(self, task_id: int) -> None:
        child_tasks = self._dag[task_id]
        for child_id in child_tasks:
            resource = self._task_resources[child_id]
            self.manager.run_task(
                self._do_task, child_id, resource, name=f"task-{task_id}"
            )
            is_running_event = self._child_tasks_all_ready_events[child_id]
            await is_running_event.wait()

    async def run(self) -> None:
        # pre-run sanity checks
        assert all(
            resource.is_active is None for resource in self._task_resources.values()
        )
        assert all(
            resource.was_checked is False for resource in self._task_resources.values()
        )
        resource = self._task_resources[0]

        try:
            await self._do_task(0, resource)
        finally:
            # post-run sanity checks
            for task_id, resource in self._task_resources.items():
                if resource.is_active is not False:
                    raise AssertionError(
                        f"Resource for task-{task_id} was not `False`: {resource.is_active!r}"
                    )
            for task_id, resource in self._task_resources.items():
                if resource.was_checked is not True:
                    raise AssertionError(f"Resource for task-{task_id} was not checked")
            self.all_checks_passed = True
