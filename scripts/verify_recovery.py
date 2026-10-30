"""Stop/restart this project's local Compose services and verify durable recovery.

Run only against the disposable local development stack, with no other tests running.
This interrupts its API, worker and database, preserves its data, and restarts them.
"""

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]


async def compose(*args):
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        *args,
        cwd=PROJECT_ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await asyncio.wait_for(process.communicate(), timeout=60)
    if process.returncode:
        raise RuntimeError(output.decode())


async def wait_healthy(client):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            if (await client.get("/")).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.2)
    raise AssertionError("API did not recover within 30 seconds")


async def verify():
    prefix = "recovery_" + uuid4().hex[:12]
    payload = {
        "transaction_id": prefix,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 150.50,
        "currency": "USD",
    }
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=5) as client:
        await wait_healthy(client)
        response = await client.post("/v1/webhooks/transactions", json=payload)
        assert response.status_code == 202
        original = (await client.get(f"/v1/transactions/{prefix}")).json()[0]
        assert original["status"] == "PROCESSING"
        # Hard-kill the worker while the accepted transaction is still waiting.
        await compose("kill", "-s", "SIGKILL", "worker")
        await compose("stop", "worker")
        try:
            await compose("stop", "db")
            during_outage = {**payload, "transaction_id": prefix + "_outage"}
            started = time.perf_counter()
            response = await client.post("/v1/webhooks/transactions", json=during_outage)
            outage_latency_ms = (time.perf_counter() - started) * 1000
            assert response.status_code == 503, response.text
            assert outage_latency_ms < 500
            assert (await client.get("/")).status_code == 503
        finally:
            await compose("start", "db")
            await compose("restart", "api")
            await compose("start", "worker")
        await wait_healthy(client)
        # Retry the rejected delivery with its original ID after storage recovers.
        assert (
            await client.post("/v1/webhooks/transactions", json=during_outage)
        ).status_code == 202

        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            rows = [
                (await client.get(f"/v1/transactions/{identifier}")).json()[0]
                for identifier in (prefix, prefix + "_outage")
            ]
            if all(row["status"] == "PROCESSED" for row in rows):
                break
            await asyncio.sleep(0.2)
        else:
            raise AssertionError("Pending transactions were not recovered")
        assert rows[0]["created_at"] == original["created_at"]
        for row in rows:
            delay = (
                datetime.fromisoformat(row["processed_at"])
                - datetime.fromisoformat(row["created_at"])
            ).total_seconds()
            assert delay >= 30
        assert (await client.post("/v1/webhooks/transactions", json=payload)).status_code == 202
        assert (await client.get(f"/v1/transactions/{prefix}")).json()[0] == rows[0]
    return {
        "checked_at": datetime.now(UTC).isoformat(),
        "worker_sigkill_recovery": "PASS",
        "api_restart_recovery": "PASS",
        "database_stop_start_durability": "PASS",
        "database_outage_response": 503,
        "database_outage_response_ms": round(outage_latency_ms, 3),
        "rejected_webhook_retry": "PASS",
        "completed_duplicate_keeps_timestamps": "PASS",
        "result": "PASS",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(verify())
    text = json.dumps(result, indent=2) + "\n"
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)


if __name__ == "__main__":
    main()
