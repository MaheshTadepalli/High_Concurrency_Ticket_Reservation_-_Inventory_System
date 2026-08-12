from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class ReservationStatus(str, Enum):
    pending = "pending"
    confirmed = "confirmed"
    expired = "expired"
    cancelled = "cancelled"


StrategyName = Literal["naive", "atomic", "optimistic", "pessimistic"]


class CreateReservationRequest(BaseModel):
    inventory_id: UUID
    user_id: str = Field(min_length=1, max_length=128)
    quantity: int = Field(ge=1, le=20)
    strategy: StrategyName | None = None


class ReservationResponse(BaseModel):
    id: UUID
    inventory_id: UUID
    user_id: str
    quantity: int
    status: ReservationStatus
    strategy: str
    idempotency_key: str
    expires_at: datetime
    created_at: datetime
    available_after: int | None = None
    reused: bool = False


class InventoryResponse(BaseModel):
    id: UUID
    event_id: UUID
    section: str
    total: int
    available: int
    version: int


class EventResponse(BaseModel):
    id: UUID
    name: str
    venue: str
    starts_at: datetime
    inventory: list[InventoryResponse] = []


class ConfirmReservationRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)


class ExpiryResult(BaseModel):
    expired_count: int
    tickets_released: int


class HealthResponse(BaseModel):
    status: str
    database: str
