-- High-concurrency ticket inventory schema
-- Demonstrates: constraints, optimistic versioning, idempotency, TTL

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    venue       TEXT NOT NULL,
    starts_at   TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE ticket_inventory (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id    UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    section     TEXT NOT NULL DEFAULT 'GA',
    total       INT  NOT NULL CHECK (total >= 0),
    available   INT  NOT NULL CHECK (available >= 0),
    -- Optimistic locking version counter
    version     INT  NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT available_lte_total CHECK (available <= total)
);

CREATE INDEX idx_inventory_event ON ticket_inventory(event_id);

CREATE TYPE reservation_status AS ENUM (
    'pending',
    'confirmed',
    'expired',
    'cancelled'
);

CREATE TABLE reservations (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    inventory_id     UUID NOT NULL REFERENCES ticket_inventory(id),
    user_id          TEXT NOT NULL,
    quantity         INT  NOT NULL CHECK (quantity > 0),
    status           reservation_status NOT NULL DEFAULT 'pending',
    -- Idempotency: same key returns the same reservation
    idempotency_key  TEXT NOT NULL,
    strategy         TEXT NOT NULL,
    expires_at       TIMESTAMPTZ NOT NULL,
    confirmed_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_idempotency UNIQUE (idempotency_key)
);

CREATE INDEX idx_reservations_inventory ON reservations(inventory_id);
CREATE INDEX idx_reservations_status_expires
    ON reservations(status, expires_at)
    WHERE status = 'pending';
CREATE INDEX idx_reservations_user ON reservations(user_id);

-- Seed: one hot event with scarce inventory (perfect for load tests)
INSERT INTO events (id, name, venue, starts_at)
VALUES (
    '11111111-1111-1111-1111-111111111111',
    'Sold-Out Stadium Tour',
    'Metro Arena',
    NOW() + INTERVAL '30 days'
);

INSERT INTO ticket_inventory (id, event_id, section, total, available, version)
VALUES (
    '22222222-2222-2222-2222-222222222222',
    '11111111-1111-1111-1111-111111111111',
    'GA',
    100,
    100,
    0
);
