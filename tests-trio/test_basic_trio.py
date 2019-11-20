import trio

import pytest


@pytest.mark.trio
async def test_basic_trio():
    await trio.sleep(0)
    assert True
