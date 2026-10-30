"""Exercise the running service over real HTTP (no database credentials needed)."""

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx


async def verify(base_url: str, count: int, concurrency: int):
    prefix = "verify_" + uuid4().hex[:12]
    latencies = []
    limiter = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(base_url=base_url, timeout=5) as client:
        health = await client.get("/")
        assert health.status_code == 200 and health.json()["status"] == "HEALTHY"
        assert (await client.get(f"/v1/transactions/{prefix}_missing")).status_code == 404

        async def send(i):
            async with limiter:
                start = time.perf_counter()
                response = await client.post(
                    "/v1/webhooks/transactions",
                    json={
                        "transaction_id": f"{prefix}_{i}",
                        "source_account": "acc_user_789",
                        "destination_account": "acc_merchant_456",
                        "amount": 1500,
                        "currency": "INR",
                    },
                )
                latencies.append((time.perf_counter() - start) * 1000)
                assert response.status_code == 202, response.text

        # 20 extra deliveries for the same ID exercise idempotency under contention.
        await asyncio.gather(*(send(i) for i in list(range(count)) + [0] * 20))
        assert max(latencies) < 500, f"Slowest acknowledgment: {max(latencies):.1f}ms"
        first = (await client.get(f"/v1/transactions/{prefix}_0")).json()[0]
        assert first["status"] == "PROCESSING" and first["processed_at"] is None

        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            response = await client.get(f"/v1/transactions/{prefix}_{count - 1}")
            assert response.status_code == 200, response.text
            row = response.json()[0]
            if row["status"] == "PROCESSED":
                break
            await asyncio.sleep(0.2)
        else:
            raise AssertionError("Worker did not complete pending transactions within 40 seconds")

        async def query(i):
            async with limiter:
                response = await client.get(f"/v1/transactions/{prefix}_{i}")
                assert response.status_code == 200
                row = response.json()[0]
                assert row["status"] == "PROCESSED"
                assert row["amount"] == 1500 and row["currency"] == "INR"
                delay = (
                    datetime.fromisoformat(row["processed_at"])
                    - datetime.fromisoformat(row["created_at"])
                ).total_seconds()
                assert 30 <= delay < 40
                return delay

        delays = await asyncio.gather(*(query(i) for i in range(count)))
        # A replay after completion must not rewrite either timestamp.
        before = (await client.get(f"/v1/transactions/{prefix}_0")).json()
        await send(0)
        assert (await client.get(f"/v1/transactions/{prefix}_0")).json() == before
    ordered = sorted(latencies)
    return {
        "checked_at": datetime.now(UTC).isoformat(),
        "base_url": base_url,
        "unique_transactions": count,
        "duplicate_deliveries": 21,
        "concurrency": concurrency,
        "acknowledgment_ms": {
            "p50": round(ordered[int(len(ordered) * 0.5)], 3),
            "p95": round(ordered[int(len(ordered) * 0.95)], 3),
            "max": round(max(ordered), 3),
        },
        "processing_delay_seconds": {"min": min(delays), "max": max(delays)},
        "result": "PASS",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.count < 1 or args.concurrency < 1:
        parser.error("count and concurrency must be positive")
    result = asyncio.run(verify(args.base_url, args.count, args.concurrency))
    text = json.dumps(result, indent=2) + "\n"
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)


if __name__ == "__main__":
    main()
