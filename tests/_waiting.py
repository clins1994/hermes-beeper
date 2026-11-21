"""Poll a condition on the event loop instead of sleeping for a fixed time."""

import asyncio
import time


async def async_wait_until(predicate, timeout: float = 5.0, interval: float = 0.01, what: str | None = None):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what or predicate}")
        await asyncio.sleep(interval)
