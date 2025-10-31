"""Simple asyncio-aware helpers for running blocking work in an executor."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, TypeVar

T = TypeVar("T")


async def run_in_executor(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run ``func`` in a thread pool and await the result."""

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as executor:
        return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))
