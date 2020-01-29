import pytest

from async_service._utils import get_task_name
from async_service.abc import ServiceAPI


async def async_fn_for_test():
    pass


class NoStrOrRepr(ServiceAPI):
    def get_manager(self):
        pass

    async def run(self):
        pass


class HasStrNotRepr(ServiceAPI):
    def get_manager(self):
        pass

    def __str__(self):
        return "custom-str"

    async def run(self):
        pass


class HasReprNotStr(ServiceAPI):
    def get_manager(self):
        pass

    def __repr__(self):
        return "custom-repr"

    async def run(self):
        pass


class HasStrAndRepr(ServiceAPI):
    def get_manager(self):
        pass

    def __str__(self):
        return "custom-str"

    def __repr__(self):
        return "custom-repr"

    async def run(self):
        pass


@pytest.mark.parametrize(
    "value,explicit_name,expected_name",
    (
        (async_fn_for_test, None, "async_fn_for_test"),
        (async_fn_for_test, "explicit_0", "explicit_0"),
        (NoStrOrRepr(), None, "NoStrOrRepr"),
        (NoStrOrRepr(), "explicit_1", "explicit_1"),
        (HasStrNotRepr(), None, "custom-str"),
        (HasStrNotRepr(), "explicit_2", "explicit_2"),
        (HasReprNotStr(), None, "custom-repr"),
        (HasReprNotStr(), "explicit_3", "explicit_3"),
        (HasStrAndRepr(), None, "custom-str"),
        (HasStrAndRepr(), "explicit_4", "explicit_4"),
    ),
)
def test_get_task_name(value, explicit_name, expected_name):
    task_name = get_task_name(value, explicit_name)
    assert task_name == expected_name
