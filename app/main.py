from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import get_settings
from app.db import close_pool, get_pool, init_pool
from app.errors import (
    ConflictError,
    IdempotencyConflictError,
    NotFoundError,
    SoldOutError,
)
from app.schemas import (
    ConfirmReservationRequest,
    CreateReservationRequest,
    EventResponse,
    ExpiryResult,
    HealthResponse,
    InventoryResponse,
    ReservationResponse,
)
from app.services import reservations as svc

app = FastAPI(
    title="High-Concurrency Ticket Reservation System",
    description=(
        "Demonstrates safe scarce-inventory reservation under concurrent load: "
        "atomic updates, optimistic/pessimistic locking, idempotency, TTL expiry, "
        "and deadlock retries."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_expiry_task: asyncio.Task | None = None


async def _expiry_loop() -> None:
    while True:
        try:
            pool = get_pool()
            await svc.expire_pending_reservations(pool)
        except Exception:
            # Keep the sweeper alive; errors are transient under load.
            pass
        await asyncio.sleep(2)


@app.on_event("startup")
async def startup() -> None:
    global _expiry_task
    await init_pool()
    _expiry_task = asyncio.create_task(_expiry_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    global _expiry_task
    if _expiry_task is not None:
        _expiry_task.cancel()
        with suppress(asyncio.CancelledError):
            await _expiry_task
        _expiry_task = None
    await close_pool()


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return HealthResponse(status="ok", database="up")
    except Exception:
        return HealthResponse(status="degraded", database="down")


@app.get("/events", response_model=list[EventResponse])
async def list_events() -> list[EventResponse]:
    rows = await svc.list_events(get_pool())
    return [EventResponse.model_validate(r) for r in rows]


@app.get("/inventory/{inventory_id}", response_model=InventoryResponse)
async def get_inventory(inventory_id: UUID) -> InventoryResponse:
    try:
        row = await svc.get_inventory(get_pool(), inventory_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return InventoryResponse.model_validate(row)


@app.get("/inventory/{inventory_id}/consistency")
async def inventory_consistency(inventory_id: UUID) -> dict:
    try:
        return await svc.consistency_check(get_pool(), inventory_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/inventory/{inventory_id}/reset", response_model=InventoryResponse)
async def reset_inventory(
    inventory_id: UUID,
    available: int | None = Query(default=None),
) -> InventoryResponse:
    """Test-only helper to restore stock between load-test runs."""
    try:
        row = await svc.reset_inventory(
            get_pool(), inventory_id, available=available
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return InventoryResponse.model_validate(row)


@app.post("/reservations", response_model=ReservationResponse, status_code=201)
async def create_reservation(
    body: CreateReservationRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
) -> ReservationResponse:
    settings = get_settings()
    strategy = body.strategy or settings.default_strategy  # type: ignore[assignment]
    try:
        row = await svc.create_reservation(
            get_pool(),
            inventory_id=body.inventory_id,
            user_id=body.user_id,
            quantity=body.quantity,
            idempotency_key=idempotency_key,
            strategy=strategy,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except SoldOutError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except IdempotencyConflictError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    status = 200 if row.get("reused") else 201
    return JSONResponse(
        content=ReservationResponse.model_validate(row).model_dump(mode="json"),
        status_code=status,
    )


@app.get("/reservations/{reservation_id}", response_model=ReservationResponse)
async def get_reservation(reservation_id: UUID) -> ReservationResponse:
    try:
        row = await svc.get_reservation(get_pool(), reservation_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ReservationResponse.model_validate(row)


@app.post("/reservations/{reservation_id}/confirm", response_model=ReservationResponse)
async def confirm_reservation(
    reservation_id: UUID,
    body: ConfirmReservationRequest,
) -> ReservationResponse:
    try:
        row = await svc.confirm_reservation(
            get_pool(), reservation_id, body.user_id
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except SoldOutError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ReservationResponse.model_validate(row)


@app.post("/reservations/{reservation_id}/cancel", response_model=ReservationResponse)
async def cancel_reservation(
    reservation_id: UUID,
    body: ConfirmReservationRequest,
) -> ReservationResponse:
    try:
        row = await svc.cancel_reservation(
            get_pool(), reservation_id, body.user_id
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except SoldOutError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ReservationResponse.model_validate(row)


@app.post("/admin/expire", response_model=ExpiryResult)
async def run_expiry() -> ExpiryResult:
    result = await svc.expire_pending_reservations(get_pool())
    return ExpiryResult.model_validate(result)


@app.get("/meta/strategies")
async def strategies(request: Request) -> dict:
    return {
        "default": get_settings().default_strategy,
        "strategies": {
            "naive": "Unsafe read-check-write (absolute SET). Oversells under load.",
            "atomic": "UPDATE ... WHERE available >= qty. Preferred default.",
            "optimistic": "Version CAS with application retries.",
            "pessimistic": "SELECT FOR UPDATE row lock.",
        },
        "reservation_ttl_seconds": get_settings().reservation_ttl_seconds,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "docs": str(request.base_url) + "docs",
    }
