import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest
import simplejson

from app.config import Settings
from app.database import Database
from app.migrate import migrate
from app.models import TransactionIn
from app.worker import run_worker
from tests.conftest import make_due


async def test_health(client):
    response = await client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "HEALTHY"
    assert (
        abs((datetime.now(UTC) - datetime.fromisoformat(body["current_time"])).total_seconds()) < 2
    )


async def test_accept_and_query_pending(client, payload, database):
    start = time.perf_counter()
    response = await client.post("/v1/webhooks/transactions", json=payload)
    assert response.status_code == 202
    assert time.perf_counter() - start < 0.5
    result = (await client.get(f"/v1/transactions/{payload['transaction_id']}")).json()
    assert len(result) == 1
    row = result[0]
    assert {key: row[key] for key in payload} == payload
    assert row["status"] == "PROCESSING"
    assert row["processed_at"] is None
    assert await database.process_ready_batch(100) == []


async def test_missing_transaction(client):
    response = await client.get("/v1/transactions/txn_missing")
    assert response.status_code == 404


async def test_concurrent_duplicates_first_payload_wins(client, payload, database):
    responses = await asyncio.gather(
        *(client.post("/v1/webhooks/transactions", json=payload) for _ in range(60))
    )
    assert all(response.status_code == 202 for response in responses)
    changed = {**payload, "amount": 999, "currency": "USD"}
    assert (await client.post("/v1/webhooks/transactions", json=changed)).status_code == 202
    await make_due(database)
    assert await database.process_ready_batch(100) == [payload["transaction_id"]]
    assert await database.process_ready_batch(100) == []
    before = await database.get(payload["transaction_id"])
    assert before["amount"] == Decimal("1500")
    assert before["currency"] == "INR"
    assert (await client.post("/v1/webhooks/transactions", json=payload)).status_code == 202
    assert await database.process_ready_batch(100) == []
    assert await database.get(payload["transaction_id"]) == before


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("amount", 0),
        ("amount", -1),
        ("amount", True),
        ("amount", "12.50"),
        ("amount", 1.12345),
        ("currency", "inr"),
        ("currency", "INRR"),
        ("source_account", ""),
        ("transaction_id", "x' OR 1=1 --"),
        ("transaction_id", "x" * 129),
        ("extra", "unexpected"),
    ],
)
async def test_invalid_payloads_are_not_persisted(client, payload, database, key, value):
    response = await client.post("/v1/webhooks/transactions", json={**payload, key: value})
    assert response.status_code == 422
    assert await database.get(payload["transaction_id"]) is None


@pytest.mark.parametrize("body", ['{"amount":', "{}", "[]", "null", '{"amount": NaN}'])
async def test_malformed_or_missing_json(client, body):
    response = await client.post(
        "/v1/webhooks/transactions", content=body, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422


async def test_exact_decimal_round_trip(client, payload):
    payload["amount"] = Decimal("9999999999999999.0001")
    response = await client.post(
        "/v1/webhooks/transactions",
        content=simplejson.dumps(payload, use_decimal=True),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 202
    response = await client.get(f"/v1/transactions/{payload['transaction_id']}")
    result = simplejson.loads(response.text, use_decimal=True)
    assert result[0]["amount"] == Decimal("9999999999999999.0001")
    assert '"amount":9999999999999999.0001' in response.text


async def test_oversized_and_chunked_bodies(client):
    assert (await client.post("/v1/webhooks/transactions", content="x" * 17000)).status_code == 413

    async def chunks():
        for _ in range(20):
            yield b"x" * 1000

    assert (await client.post("/v1/webhooks/transactions", content=chunks())).status_code == 413


async def test_two_workers_complete_each_transaction_once(database, database_url, payload):
    for i in range(40):
        await database.accept(TransactionIn(**{**payload, "transaction_id": f"txn_{i}"}))
    await make_due(database)
    other = Database(Settings(database_url=database_url), options=database.test_options)
    await other.open()
    try:
        results = await asyncio.gather(
            *(db.process_ready_batch(10) for db in [database, other] * 4)
        )
    finally:
        await other.close()
    completed = [item for batch in results for item in batch]
    assert len(completed) == len(set(completed)) == 40


async def test_pending_work_survives_connection_pool_restart(database, database_url, payload):
    await database.accept(TransactionIn(**payload))
    await make_due(database)
    await database.close()
    restarted = Database(Settings(database_url=database_url), options=database.test_options)
    await restarted.open()
    try:
        assert await restarted.process_ready_batch(100) == [payload["transaction_id"]]
        row = await restarted.get(payload["transaction_id"])
        assert row["status"] == "PROCESSED"
    finally:
        await restarted.close()


async def test_failed_processing_rolls_back_and_can_be_retried(database, payload):
    await database.accept(TransactionIn(**payload))
    await make_due(database)
    async with database.pool.connection() as connection:
        await connection.execute("""
            CREATE FUNCTION fail_completion() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'simulated storage failure'; END $$;
            CREATE TRIGGER fail_completion BEFORE UPDATE ON transactions
            FOR EACH ROW EXECUTE FUNCTION fail_completion();
        """)
    with pytest.raises(psycopg.Error):
        await database.process_ready_batch(100)
    assert (await database.get(payload["transaction_id"]))["status"] == "PROCESSING"
    async with database.pool.connection() as connection:
        await connection.execute("DROP TRIGGER fail_completion ON transactions")
    assert await database.process_ready_batch(100) == [payload["transaction_id"]]


async def test_worker_retries_transient_failure_and_stops(database, payload, monkeypatch):
    await database.accept(TransactionIn(**payload))
    await make_due(database)
    original = database.process_ready_batch
    stop = asyncio.Event()
    calls = 0

    async def fail_once(batch_size):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise psycopg.OperationalError("simulated outage")
        result = await original(batch_size)
        stop.set()
        return result

    monkeypatch.setattr(database, "process_ready_batch", fail_once)
    await asyncio.wait_for(run_worker(database, Settings(), stop), timeout=3)
    assert calls == 2
    assert (await database.get(payload["transaction_id"]))["status"] == "PROCESSED"


async def test_database_error_returns_retryable_503(client, payload, database, monkeypatch):
    async def unavailable(*args):
        raise psycopg.OperationalError("secret database URL must not be reflected")

    monkeypatch.setattr(database, "accept", unavailable)
    response = await client.post("/v1/webhooks/transactions", json=payload)
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"
    assert "secret" not in response.text


async def test_database_timeout_is_bounded(client, payload, database, monkeypatch):
    async def slow(*args):
        await asyncio.sleep(2)

    monkeypatch.setattr(database, "accept", slow)
    start = time.perf_counter()
    response = await client.post("/v1/webhooks/transactions", json=payload)
    assert response.status_code == 503
    assert time.perf_counter() - start < 0.5


async def test_health_fails_when_storage_unavailable(client, database, monkeypatch):
    async def unavailable():
        raise psycopg.OperationalError("unavailable")

    monkeypatch.setattr(database, "health", unavailable)
    assert (await client.get("/")).status_code == 503


async def test_migration_is_repeatable(database, database_url, payload):
    await database.accept(TransactionIn(**payload))
    await migrate(database_url, options=database.test_options)
    assert await database.get(payload["transaction_id"]) is not None


@pytest.mark.slow
async def test_real_30_second_delay_under_load(client, database, payload):
    stop = asyncio.Event()
    task = asyncio.create_task(run_worker(database, Settings(), stop))
    latencies = []
    limiter = asyncio.Semaphore(20)

    async def send(i):
        async with limiter:
            start = time.perf_counter()
            response = await client.post(
                "/v1/webhooks/transactions", json={**payload, "transaction_id": f"load_{i}"}
            )
            latencies.append((time.perf_counter() - start) * 1000)
            assert response.status_code == 202

    try:
        await asyncio.gather(*(send(i) for i in range(200)))
        assert max(latencies) < 500
        assert (await database.get("load_0"))["status"] == "PROCESSING"
        deadline = time.monotonic() + 36
        while time.monotonic() < deadline:
            async with database.pool.connection() as connection:
                cursor = await connection.execute(
                    "SELECT count(*) AS pending FROM transactions WHERE status = 'PROCESSING'"
                )
                if (await cursor.fetchone())["pending"] == 0:
                    break
            await asyncio.sleep(0.1)
        async with database.pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT count(*) AS total, "
                "min(extract(epoch FROM processed_at - created_at)) AS min_delay, "
                "max(extract(epoch FROM processed_at - created_at)) AS max_delay "
                "FROM transactions WHERE status = 'PROCESSED'"
            )
            result = await cursor.fetchone()
        assert result["total"] == 200
        assert Decimal("30") <= result["min_delay"] <= result["max_delay"] < Decimal("35")
        ordered = sorted(latencies)
        print(
            f"\n200 requests, concurrency 20: p50={ordered[99]:.1f}ms "
            f"p95={ordered[189]:.1f}ms max={max(latencies):.1f}ms; "
            f"processing={result['min_delay']:.3f}–{result['max_delay']:.3f}s"
        )
    finally:
        stop.set()
        await task
