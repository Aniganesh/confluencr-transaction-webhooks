import os
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql

from app.config import Settings
from app.database import Database
from app.main import create_app
from app.migrate import migrate


@pytest.fixture
def database_url():
    # Test schemas are isolated; never truncate or drop application tables.
    return os.getenv("TEST_DATABASE_URL", "postgresql://webhooks:webhooks@127.0.0.1:55432/webhooks")


@pytest.fixture
async def database(database_url):
    schema = "test_" + uuid4().hex
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as connection:
        await connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    options = f"-c search_path={schema}"
    settings = Settings(database_url=database_url)
    db = Database(settings, options=options)
    try:
        await migrate(database_url, options=options)
        await db.open()
        # Needed by restart/multi-worker tests to build an independent pool.
        db.test_options = options
        yield db
    finally:
        await db.close()
        async with await psycopg.AsyncConnection.connect(
            database_url, autocommit=True
        ) as connection:
            await connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


@pytest.fixture
async def client(database, database_url):
    api = create_app(Settings(database_url=database_url), database)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture
def payload():
    return {
        "transaction_id": "txn_test_123",
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }


async def make_due(database, transaction_id=None):
    # Move receipt AND deadline into the past; production delay is never configurable.
    async with database.pool.connection() as connection:
        await connection.execute(
            "UPDATE transactions SET created_at = now() - INTERVAL '31 seconds', "
            "ready_at = now() - INTERVAL '1 second' "
            "WHERE (%s::text IS NULL OR transaction_id = %s)",
            (transaction_id, transaction_id),
        )
