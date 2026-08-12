#!/usr/bin/env python3
"""Quick correctness demo: fire N concurrent reserves against scarce stock."""

from __future__ import annotations

import argparse
import asyncio
import uuid

import httpx

INVENTORY = "22222222-2222-2222-2222-222222222222"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument("--strategy", default="atomic")
    parser.add_argument("--stock", type=int, default=10)
    parser.add_argument("--buyers", type=int, default=50)
    args = parser.parse_args()

    async with httpx.AsyncClient(base_url=args.base_url, timeout=30.0) as client:
        await client.post(
            f"/inventory/{INVENTORY}/reset", params={"available": args.stock}
        )

        async def buy(i: int) -> tuple[int, str]:
            r = await client.post(
                "/reservations",
                headers={"Idempotency-Key": str(uuid.uuid4())},
                json={
                    "inventory_id": INVENTORY,
                    "user_id": f"buyer-{i}",
                    "quantity": 1,
                    "strategy": args.strategy,
                },
            )
            return r.status_code, r.text

        results = await asyncio.gather(*[buy(i) for i in range(args.buyers)])
        ok = sum(1 for code, _ in results if code in (200, 201))
        conflict = sum(1 for code, _ in results if code == 409)
        other = args.buyers - ok - conflict

        consistency = (await client.get(f"/inventory/{INVENTORY}/consistency")).json()
        print(f"strategy={args.strategy} stock={args.stock} buyers={args.buyers}")
        print(f"reserved_ok={ok} sold_out={conflict} other={other}")
        print(
            f"available={consistency['available']} held={consistency['held_by_reservations']} "
            f"consistent={consistency['consistent']}"
        )
        if consistency["held_by_reservations"] > args.stock:
            print("RESULT: OVERSOLD")
        elif consistency["consistent"] and ok == min(args.stock, args.buyers):
            print("RESULT: SAFE (exact sell-through)")
        elif consistency["consistent"]:
            print("RESULT: SAFE")
        else:
            print("RESULT: INCONSISTENT")


if __name__ == "__main__":
    asyncio.run(main())
