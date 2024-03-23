from types import (
    TracebackType,
)
from typing import (
    Any,
    Awaitable,
    Callable,
    Tuple,
    Type,
)

EXC_INFO = Tuple[Type[Exception], Exception, TracebackType]

AsyncFn = Callable[..., Awaitable[Any]]
