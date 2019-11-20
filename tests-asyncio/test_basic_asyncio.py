import asyncio

import pytest


@pytest.mark.asyncio
async def test_basic_asyncio():
    await asyncio.sleep(0)
    assert True
