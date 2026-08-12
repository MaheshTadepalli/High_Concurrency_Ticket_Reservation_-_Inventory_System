from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

import asyncpg

T = TypeVar("T")

# PostgreSQL: 40001 serialization_failure, 40P01 deadlock_detected
RETRYABLE_SQLSTATES = {"40001", "40P01"}


class ConflictError(Exception):
    """Optimistic lock lost or contended write."""


class SoldOutError(Exception):
    """Not enough inventory remaining."""


class NotFoundError(Exception):
    """Entity missing."""


class IdempotencyConflictError(Exception):
    """Same idempotency key used with different payload."""


async def with_deadlock_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 5,
    base_delay_ms: float = 10.0,
) -> T:
    """Retry on PostgreSQL deadlock / serialization failures with jittered backoff."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return await operation()
        except asyncpg.DeadlockDetectedError:
            if attempt >= max_attempts:
                raise
        except asyncpg.SerializationError:
            if attempt >= max_attempts:
                raise
        except asyncpg.PostgresError as exc:
            if getattr(exc, "sqlstate", None) not in RETRYABLE_SQLSTATES:
                raise
            if attempt >= max_attempts:
                raise

        delay = (base_delay_ms / 1000.0) * (2 ** (attempt - 1))
        delay += random.uniform(0, delay * 0.25)
        await asyncio.sleep(delay)
