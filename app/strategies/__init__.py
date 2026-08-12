"""Reservation concurrency strategies.

Each strategy returns (reservation_row, available_after).

naive       – classic TOCTOU race (read available, then write). Unsafe under load.
atomic      – single UPDATE ... WHERE available >= qty  (preferred default).
optimistic  – version column compare-and-swap; retry on conflict.
pessimistic – SELECT ... FOR UPDATE row lock; serializes writers.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from app.errors import ConflictError, NotFoundError, SoldOutError


async def _insert_reservation(
    conn: asyncpg.Connection,
    *,
    inventory_id: UUID,
    user_id: str,
    quantity: int,
    idempotency_key: str,
    strategy: str,
    expires_at: datetime,
) -> asyncpg.Record:
    return await conn.fetchrow(
        """
        INSERT INTO reservations (
            inventory_id, user_id, quantity, status,
            idempotency_key, strategy, expires_at
        )
        VALUES ($1, $2, $3, 'pending', $4, $5, $6)
        RETURNING *
        """,
        inventory_id,
        user_id,
        quantity,
        idempotency_key,
        strategy,
        expires_at,
    )


async def reserve_naive(
    conn: asyncpg.Connection,
    *,
    inventory_id: UUID,
    user_id: str,
    quantity: int,
    idempotency_key: str,
    expires_at: datetime,
) -> tuple[asyncpg.Record, int]:
    """UNSAFE: classic TOCTOU — compute new stock in-app, write absolute value.

    Under concurrency two workers can both read available=1, both decide to
    write available=0, and both insert a reservation → oversell.
    """
    inv = await conn.fetchrow(
        "SELECT id, available FROM ticket_inventory WHERE id = $1",
        inventory_id,
    )
    if inv is None:
        raise NotFoundError("inventory not found")
    if inv["available"] < quantity:
        raise SoldOutError("not enough tickets")

    new_available = int(inv["available"]) - quantity
    await conn.execute(
        """
        UPDATE ticket_inventory
        SET available = $2,
            updated_at = NOW()
        WHERE id = $1
        """,
        inventory_id,
        new_available,
    )

    reservation = await _insert_reservation(
        conn,
        inventory_id=inventory_id,
        user_id=user_id,
        quantity=quantity,
        idempotency_key=idempotency_key,
        strategy="naive",
        expires_at=expires_at,
    )
    return reservation, new_available

async def reserve_atomic(
    conn: asyncpg.Connection,
    *,
    inventory_id: UUID,
    user_id: str,
    quantity: int,
    idempotency_key: str,
    expires_at: datetime,
) -> tuple[asyncpg.Record, int]:
    """SAFE: decrement only if enough stock remains — one atomic statement."""
    row = await conn.fetchrow(
        """
        UPDATE ticket_inventory
        SET available = available - $2,
            version = version + 1,
            updated_at = NOW()
        WHERE id = $1
          AND available >= $2
        RETURNING available
        """,
        inventory_id,
        quantity,
    )
    if row is None:
        exists = await conn.fetchval(
            "SELECT 1 FROM ticket_inventory WHERE id = $1",
            inventory_id,
        )
        if not exists:
            raise NotFoundError("inventory not found")
        raise SoldOutError("not enough tickets")

    reservation = await _insert_reservation(
        conn,
        inventory_id=inventory_id,
        user_id=user_id,
        quantity=quantity,
        idempotency_key=idempotency_key,
        strategy="atomic",
        expires_at=expires_at,
    )
    return reservation, int(row["available"])


async def reserve_optimistic(
    conn: asyncpg.Connection,
    *,
    inventory_id: UUID,
    user_id: str,
    quantity: int,
    idempotency_key: str,
    expires_at: datetime,
) -> tuple[asyncpg.Record, int]:
    """SAFE: compare-and-swap on version. Caller retries ConflictError."""
    inv = await conn.fetchrow(
        "SELECT id, available, version FROM ticket_inventory WHERE id = $1",
        inventory_id,
    )
    if inv is None:
        raise NotFoundError("inventory not found")
    if inv["available"] < quantity:
        raise SoldOutError("not enough tickets")

    row = await conn.fetchrow(
        """
        UPDATE ticket_inventory
        SET available = available - $2,
            version = version + 1,
            updated_at = NOW()
        WHERE id = $1
          AND version = $3
          AND available >= $2
        RETURNING available
        """,
        inventory_id,
        quantity,
        inv["version"],
    )
    if row is None:
        # Lost the race — another writer bumped version first
        raise ConflictError("optimistic lock conflict")

    reservation = await _insert_reservation(
        conn,
        inventory_id=inventory_id,
        user_id=user_id,
        quantity=quantity,
        idempotency_key=idempotency_key,
        strategy="optimistic",
        expires_at=expires_at,
    )
    return reservation, int(row["available"])


async def reserve_pessimistic(
    conn: asyncpg.Connection,
    *,
    inventory_id: UUID,
    user_id: str,
    quantity: int,
    idempotency_key: str,
    expires_at: datetime,
) -> tuple[asyncpg.Record, int]:
    """SAFE: row lock until transaction commits. Serializes concurrent writers."""
    inv = await conn.fetchrow(
        """
        SELECT id, available
        FROM ticket_inventory
        WHERE id = $1
        FOR UPDATE
        """,
        inventory_id,
    )
    if inv is None:
        raise NotFoundError("inventory not found")
    if inv["available"] < quantity:
        raise SoldOutError("not enough tickets")

    row = await conn.fetchrow(
        """
        UPDATE ticket_inventory
        SET available = available - $2,
            version = version + 1,
            updated_at = NOW()
        WHERE id = $1
        RETURNING available
        """,
        inventory_id,
        quantity,
    )
    reservation = await _insert_reservation(
        conn,
        inventory_id=inventory_id,
        user_id=user_id,
        quantity=quantity,
        idempotency_key=idempotency_key,
        strategy="pessimistic",
        expires_at=expires_at,
    )
    return reservation, int(row["available"])


STRATEGIES: dict[str, Any] = {
    "naive": reserve_naive,
    "atomic": reserve_atomic,
    "optimistic": reserve_optimistic,
    "pessimistic": reserve_pessimistic,
}
