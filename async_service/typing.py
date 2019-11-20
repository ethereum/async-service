from types import TracebackType
from typing import Tuple, Type

EXC_INFO = Tuple[Type[BaseException], BaseException, TracebackType]
