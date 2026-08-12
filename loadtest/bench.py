#!/usr/bin/env python3
"""Concurrent load test / benchmark for reservation strategies.

Usage:
  python loadtest/bench.py --strategy atomic --requests 500 --concurrency 50
  python loadtest/bench.py --compare   # run all strategies and print table
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid
from dataclasses import dataclass

import httpx

DEFAULT_INVENTORY = "22222222-2222-2222-2222-222222222222"
STRATEGIES = ("naive", "atomic", "optimistic", "pessimistic")


@dataclass
class RunResult:
    strategy: str
    requests: int
    concurrency: int
    stock: int
    ok: int
    sold_out: int
    errors: int
    elapsed_s: float
    rps: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    available_after: int
    held: int
    consistent: bool
    oversold: bool


async def reset(client: httpx.AsyncClient, inventory_id: str, stock: int) -> None:
    r = await client.post(
        f"/inventory/{inventory_id}/reset",
        params={"available": stock},
    )
    r.raise_for_status()


async def one_reserve(
    client: httpx.AsyncClient,
    *,
    inventory_id: str,
    strategy: str,
    sem: asyncio.Semaphore,
    latencies: list[float],
    counters: dict[str, int],
) -> None:
    async with sem:
        key = str(uuid.uuid4())
        payload = {
            "inventory_id": inventory_id,
            "user_id": f"user-{uuid.uuid4().hex[:8]}",
            "quantity": 1,
            "strategy": strategy,
        }
        t0 = time.perf_counter()
        try:
            r = await client.post(
                "/reservations",
                json=payload,
                headers={"Idempotency-Key": key},
            )
            elapsed = (time.perf_counter() - t0) * 1000
            latencies.append(elapsed)
            if r.status_code in (200, 201):
                counters["ok"] += 1
            elif r.status_code == 409:
                counters["sold_out"] += 1
            else:
                counters["errors"] += 1
        except Exception:
            latencies.append((time.perf_counter() - t0) * 1000)
            counters["errors"] += 1


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100) * (len(ordered) - 1)))))
    return ordered[idx]


async def run_strategy(
    base_url: str,
    *,
    strategy: str,
    requests: int,
    concurrency: int,
    stock: int,
    inventory_id: str,
) -> RunResult:
    limits = httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0, limits=limits) as client:
        await reset(client, inventory_id, stock)

        sem = asyncio.Semaphore(concurrency)
        latencies: list[float] = []
        counters = {"ok": 0, "sold_out": 0, "errors": 0}

        t0 = time.perf_counter()
        await asyncio.gather(
            *[
                one_reserve(
                    client,
                    inventory_id=inventory_id,
                    strategy=strategy,
                    sem=sem,
                    latencies=latencies,
                    counters=counters,
                )
                for _ in range(requests)
            ]
        )
        elapsed = time.perf_counter() - t0

        inv = (await client.get(f"/inventory/{inventory_id}")).json()
        consistency = (await client.get(f"/inventory/{inventory_id}/consistency")).json()

        held = consistency["held_by_reservations"]
        oversold = held > stock
        rps = requests / elapsed if elapsed > 0 else 0.0

        return RunResult(
            strategy=strategy,
            requests=requests,
            concurrency=concurrency,
            stock=stock,
            ok=counters["ok"],
            sold_out=counters["sold_out"],
            errors=counters["errors"],
            elapsed_s=elapsed,
            rps=rps,
            latency_p50_ms=percentile(latencies, 50),
            latency_p95_ms=percentile(latencies, 95),
            latency_p99_ms=percentile(latencies, 99),
            available_after=inv["available"],
            held=held,
            consistent=consistency["consistent"],
            oversold=oversold,
        )


SAFE_STRATEGIES = ("atomic", "optimistic", "pessimistic")


def print_result(r: RunResult) -> None:
    flag = "OVERSOLD" if r.oversold else ("OK" if r.consistent else "INCONSISTENT")
    print(
        f"{r.strategy:12}  ok={r.ok:4}  sold_out={r.sold_out:4}  err={r.errors:3}  "
        f"rps={r.rps:7.1f}  p50={r.latency_p50_ms:6.1f}ms  "
        f"p95={r.latency_p95_ms:6.1f}ms  p99={r.latency_p99_ms:6.1f}ms  "
        f"avail={r.available_after:4}  held={r.held:4}  [{flag}]"
    )


def print_summary_table(results: list[RunResult]) -> None:
    print()
    print(f"{'strategy':12} {'rps':>8} {'p50_ms':>8} {'p95_ms':>8} {'p99_ms':>8} {'ok':>5} {'sold_out':>8}")
    print("-" * 68)
    for r in results:
        print(
            f"{r.strategy:12} {r.rps:8.1f} {r.latency_p50_ms:8.1f} "
            f"{r.latency_p95_ms:8.1f} {r.latency_p99_ms:8.1f} {r.ok:5d} {r.sold_out:8d}"
        )


async def main_async(args: argparse.Namespace) -> None:
    if args.safe:
        strategies = list(SAFE_STRATEGIES)
    elif args.compare:
        strategies = list(STRATEGIES)
    else:
        strategies = [args.strategy]
    print(
        f"base={args.base_url}  requests={args.requests}  "
        f"concurrency={args.concurrency}  stock={args.stock}"
    )
    print("-" * 120)
    results = []
    for strategy in strategies:
        result = await run_strategy(
            args.base_url,
            strategy=strategy,
            requests=args.requests,
            concurrency=args.concurrency,
            stock=args.stock,
            inventory_id=args.inventory_id,
        )
        print_result(result)
        results.append(result)
    print("-" * 120)
    print_summary_table(results)
    safe = [r for r in results if r.consistent and not r.oversold]
    if safe:
        best_rps = max(safe, key=lambda r: r.rps)
        best_p50 = min(safe, key=lambda r: r.latency_p50_ms)
        print()
        print(f"Highest throughput: {best_rps.strategy} @ {best_rps.rps:.1f} rps")
        print(f"Lowest p50 latency: {best_p50.strategy} @ {best_p50.latency_p50_ms:.1f} ms")
    unsafe = [r for r in results if r.oversold or not r.consistent]
    if unsafe:
        print(
            "Unsafe under this load: "
            + ", ".join(f"{r.strategy} (held={r.held}/{r.stock})" for r in unsafe)
        )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ticket reservation load test")
    p.add_argument("--base-url", default="http://127.0.0.1:8001")
    p.add_argument("--strategy", default="atomic", choices=STRATEGIES)
    p.add_argument("--compare", action="store_true", help="Benchmark all strategies")
    p.add_argument(
        "--safe",
        action="store_true",
        help="Benchmark only safe strategies (atomic/optimistic/pessimistic)",
    )
    p.add_argument("--requests", type=int, default=500)
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--stock", type=int, default=100)
    p.add_argument("--inventory-id", default=DEFAULT_INVENTORY)
    return p


if __name__ == "__main__":
    asyncio.run(main_async(build_parser().parse_args()))
