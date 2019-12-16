import asyncio
import sys

if sys.version_info >= (3, 7):
    # This API isn't available in 3.6
    get_current_task = asyncio.current_task
else:
    # This API is deprecated in 3.7+
    get_current_task = asyncio.Task.current_task
