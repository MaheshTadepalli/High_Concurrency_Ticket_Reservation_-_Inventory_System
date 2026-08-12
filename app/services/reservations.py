from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg

from app.config import get_settings
from app.errors import (
    ConflictError,
    IdempotencyConflictError,
    NotFoundError,
    SoldOutError,
    with_deadlock_retry,
)
from app.strategies import STRATEGIES


def _row_to_dict(row: asyncpg.Record, *, available_after: int | None = None, reused: bool = False) -> dict[str, Any]:
    return {
        "id": row["id"],
        "inventory_id": row["inventory_id"],
        "user_id": row["user_id"],
        "quantity": row["quantity"],
        "status": row["status"],
        "strategy": row["strategy"],
        "idempotency_key": row["idempotency_key"],
        "expires_at": row["expires_at"],
        "created_at": row["created_at"],
        "available_after": available_after,
        "reused": reused,
    }


async def _get_by_idempotency(
    conn: asyncpg.Connection, key: str
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT * FROM reservations WHERE idempotency_key = $1",
        key,
    )


async def create_reservation(
    pool: asyncpg.Pool,
    *,
    inventory_id: UUID,
    user_id: str,
    quantity: int,
    idempotency_key: str,
    strategy: str,
) -> dict[str, Any]:
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy: {strategy}")

    settings = get_settings()
    expires_at = datetime.now(timezone.utc) + timedelta(
        seconds=settings.reservation_ttl_seconds
    )
    reserve_fn = STRATEGIES[strategy]
    # Optimistic CAS benefits from fresh transactions on conflict
    max_attempts = 12 if strategy == "optimistic" else 5

    async def attempt() -> dict[str, Any]:
        async with pool.acquire() as conn:
            # Fast path: return existing reservation for same idempotency key
            existing = await _get_by_idempotency(conn, idempotency_key)
            if existing is not None:
                if (
                    existing["inventory_id"] != inventory_id
                    or existing["user_id"] != user_id
                    or existing["quantity"] != quantity
                ):
                    raise IdempotencyConflictError(
                        "idempotency key reused with different payload"
                    )
                return _row_to_dict(existing, reused=True)

            # Isolation:
            # - atomic / naive: READ COMMITTED (default) is enough for atomic UPDATE
            # - optimistic: READ COMMITTED + version CAS
            # - pessimistic: READ COMMITTED + FOR UPDATE
            async with conn.transaction(isolation="read_committed"):
                # Re-check inside txn in case of race on unique key
                existing = await _get_by_idempotency(conn, idempotency_key)
                if existing is not None:
                    if (
                        existing["inventory_id"] != inventory_id
                        or existing["user_id"] != user_id
                        or existing["quantity"] != quantity
                    ):
                        raise IdempotencyConflictError(
                            "idempotency key reused with different payload"
                        )
                    return _row_to_dict(existing, reused=True)

                reservation, available = await reserve_fn(
                    conn,
                    inventory_id=inventory_id,
                    user_id=user_id,
                    quantity=quantity,
                    idempotency_key=idempotency_key,
                    expires_at=expires_at,
                )
                return _row_to_dict(reservation, available_after=available)

    async def attempt_with_lock_retries() -> dict[str, Any]:
        last_conflict: ConflictError | None = None
        for _ in range(max_attempts):
            try:
                return await with_deadlock_retry(attempt, max_attempts=3)
            except ConflictError as exc:
                last_conflict = exc
                continue
        assert last_conflict is not None
        raise last_conflict

    try:
        return await attempt_with_lock_retries()
    except asyncpg.UniqueViolationError:
        # Concurrent insert with same idempotency key — return the winner
        async with pool.acquire() as conn:
            existing = await _get_by_idempotency(conn, idempotency_key)
            if existing is None:
                raise
            if (
                existing["inventory_id"] != inventory_id
                or existing["user_id"] != user_id
                or existing["quantity"] != quantity
            ):
                raise IdempotencyConflictError(
                    "idempotency key reused with different payload"
                )
            return _row_to_dict(existing, reused=True)


async def confirm_reservation(
    pool: asyncpg.Pool,
    reservation_id: UUID,
    user_id: str,
) -> dict[str, Any]:
    async def attempt() -> dict[str, Any]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT * FROM reservations
                    WHERE id = $1
                    FOR UPDATE
                    """,
                    reservation_id,
                )
                if row is None:
                    raise NotFoundError("reservation not found")
                if row["user_id"] != user_id:
                    raise NotFoundError("reservation not found")
                if row["status"] == "confirmed":
                    return _row_to_dict(row, reused=True)
                if row["status"] != "pending":
                    raise SoldOutError(f"reservation is {row['status']}")
                if row["expires_at"] <= datetime.now(timezone.utc):
                    raise SoldOutError("reservation expired")

                updated = await conn.fetchrow(
                    """
                    UPDATE reservations
                    SET status = 'confirmed',
                        confirmed_at = NOW(),
                        updated_at = NOW()
                    WHERE id = $1
                    RETURNING *
                    """,
                    reservation_id,
                )
                return _row_to_dict(updated)

    return await with_deadlock_retry(attempt)


async def cancel_reservation(
    pool: asyncpg.Pool,
    reservation_id: UUID,
    user_id: str,
) -> dict[str, Any]:
    async def attempt() -> dict[str, Any]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT * FROM reservations
                    WHERE id = $1
                    FOR UPDATE
                    """,
                    reservation_id,
                )
                if row is None:
                    raise NotFoundError("reservation not found")
                if row["user_id"] != user_id:
                    raise NotFoundError("reservation not found")
                if row["status"] == "cancelled":
                    return _row_to_dict(row, reused=True)
                if row["status"] not in ("pending", "confirmed"):
                    raise SoldOutError(f"reservation is {row['status']}")

                await conn.execute(
                    """
                    UPDATE ticket_inventory
                    SET available = available + $2,
                        version = version + 1,
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    row["inventory_id"],
                    row["quantity"],
                )
                updated = await conn.fetchrow(
                    """
                    UPDATE reservations
                    SET status = 'cancelled', updated_at = NOW()
                    WHERE id = $1
                    RETURNING *
                    """,
                    reservation_id,
                )
                avail = await conn.fetchval(
                    "SELECT available FROM ticket_inventory WHERE id = $1",
                    row["inventory_id"],
                )
                return _row_to_dict(updated, available_after=int(avail))

    return await with_deadlock_retry(attempt)


async def expire_pending_reservations(pool: asyncpg.Pool) -> dict[str, int]:
    """Release inventory for pending reservations past TTL."""

    async def attempt() -> dict[str, int]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                expired = await conn.fetch(
                    """
                    SELECT id, inventory_id, quantity
                    FROM reservations
                    WHERE status = 'pending'
                      AND expires_at <= NOW()
                    FOR UPDATE SKIP LOCKED
                    """
                )
                if not expired:
                    return {"expired_count": 0, "tickets_released": 0}

                tickets_released = 0
                for row in expired:
                    await conn.execute(
                        """
                        UPDATE ticket_inventory
                        SET available = available + $2,
                            version = version + 1,
                            updated_at = NOW()
                        WHERE id = $1
                        """,
                        row["inventory_id"],
                        row["quantity"],
                    )
                    await conn.execute(
                        """
                        UPDATE reservations
                        SET status = 'expired', updated_at = NOW()
                        WHERE id = $1
                        """,
                        row["id"],
                    )
                    tickets_released += int(row["quantity"])

                return {
                    "expired_count": len(expired),
                    "tickets_released": tickets_released,
                }

    return await with_deadlock_retry(attempt)


async def get_reservation(pool: asyncpg.Pool, reservation_id: UUID) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM reservations WHERE id = $1",
            reservation_id,
        )
        if row is None:
            raise NotFoundError("reservation not found")
        return _row_to_dict(row)


async def list_events(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        events = await conn.fetch(
            "SELECT id, name, venue, starts_at FROM events ORDER BY starts_at"
        )
        result = []
        for event in events:
            inventory = await conn.fetch(
                """
                SELECT id, event_id, section, total, available, version
                FROM ticket_inventory
                WHERE event_id = $1
                ORDER BY section
                """,
                event["id"],
            )
            result.append(
                {
                    "id": event["id"],
                    "name": event["name"],
                    "venue": event["venue"],
                    "starts_at": event["starts_at"],
                    "inventory": [dict(i) for i in inventory],
                }
            )
        return result


async def get_inventory(pool: asyncpg.Pool, inventory_id: UUID) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, event_id, section, total, available, version
            FROM ticket_inventory
            WHERE id = $1
            """,
            inventory_id,
        )
        if row is None:
            raise NotFoundError("inventory not found")
        return dict(row)


async def reset_inventory(
    pool: asyncpg.Pool,
    inventory_id: UUID,
    *,
    available: int | None = None,
) -> dict[str, Any]:
    """Test helper: wipe reservations and restore stock.

    If ``available`` is provided, both ``total`` and ``available`` are set to
    that value so scarce-stock load tests keep the invariant
    available + held == total.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            inv = await conn.fetchrow(
                "SELECT * FROM ticket_inventory WHERE id = $1 FOR UPDATE",
                inventory_id,
            )
            if inv is None:
                raise NotFoundError("inventory not found")

            if available is None:
                new_total = int(inv["total"])
                new_available = new_total
            else:
                if available < 0:
                    raise ValueError("available out of range")
                new_total = available
                new_available = available

            await conn.execute(
                "DELETE FROM reservations WHERE inventory_id = $1",
                inventory_id,
            )
            row = await conn.fetchrow(
                """
                UPDATE ticket_inventory
                SET total = $2,
                    available = $3,
                    version = 0,
                    updated_at = NOW()
                WHERE id = $1
                RETURNING id, event_id, section, total, available, version
                """,
                inventory_id,
                new_total,
                new_available,
            )
            return dict(row)


async def consistency_check(pool: asyncpg.Pool, inventory_id: UUID) -> dict[str, Any]:
    """Verify: available + active reserved qty == total."""
    async with pool.acquire() as conn:
        inv = await conn.fetchrow(
            "SELECT total, available FROM ticket_inventory WHERE id = $1",
            inventory_id,
        )
        if inv is None:
            raise NotFoundError("inventory not found")
        held = await conn.fetchval(
            """
            SELECT COALESCE(SUM(quantity), 0)
            FROM reservations
            WHERE inventory_id = $1
              AND status IN ('pending', 'confirmed')
            """,
            inventory_id,
        )
        expected_available = inv["total"] - int(held)
        return {
            "inventory_id": str(inventory_id),
            "total": inv["total"],
            "available": inv["available"],
            "held_by_reservations": int(held),
            "expected_available": expected_available,
            "consistent": inv["available"] == expected_available,
        }
