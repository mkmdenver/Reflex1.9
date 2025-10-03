# common/utils.py
from __future__ import annotations
import functools
import random
import time
from typing import Callable, TypeVar, Any, Iterable, Generator, Optional

F = TypeVar("F", bound=Callable[..., Any])

def retry(
    *,
    attempts: int = 3,
    base_delay: float = 0.25,
    max_delay: float = 2.0,
    jitter: float = 0.25,
    retry_on: tuple[type[BaseException], ...] = (Exception,)
) -> Callable[[F], F]:
    def deco(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for i in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except retry_on as e:
                    if i >= attempts:
                        raise
                    time.sleep(min(delay + random.random() * jitter, max_delay))
                    delay = min(delay * 2, max_delay)
        return wrapper  # type: ignore
    return deco

def chunked(iterable: Iterable[Any], size: int) -> Generator[list[Any], None, None]:
    buf: list[Any] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def coalesce(*values: Optional[Any], default: Any = None) -> Any:
    for v in values:
        if v is not None:
            return v
    return default

def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))
