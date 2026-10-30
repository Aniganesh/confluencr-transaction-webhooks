from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from app.config import Settings
from app.models import TransactionIn

PUBLIC_COLUMNS = (
    "transaction_id, source_account, destination_account, amount, currency, "
    "status, created_at, processed_at"
)


class Database:
    def __init__(self, settings: Settings, *, options: str = ""):
        self.pool = AsyncConnectionPool(
            conninfo=settings.database_url,
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size,
            timeout=0.2,
            max_waiting=128,
            open=False,
            kwargs={
                "autocommit": True,
                "row_factory": dict_row,
                "options": f"-c statement_timeout=350 -c lock_timeout=100 {options}",
                "connect_timeout": 3,
                "application_name": "transaction-webhooks",
            },
        )

    async def open(self):
        await self.pool.open(wait=True, timeout=10)

    async def close(self):
        await self.pool.close()

    async def health(self):
        async with self.pool.connection() as connection:
            # Also check that migrations have run, not just that PostgreSQL is up.
            await connection.execute("SELECT 1 FROM transactions LIMIT 0")

    async def accept(self, transaction: TransactionIn) -> None:
        async with self.pool.connection() as connection:
            # A single autocommitted statement is both persistence and durable enqueue.
            # The unique primary key arbitrates concurrent duplicates across API replicas.
            await connection.execute(
                "INSERT INTO transactions "
                "(transaction_id, source_account, destination_account, amount, currency) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING",
                (
                    transaction.transaction_id,
                    transaction.source_account,
                    transaction.destination_account,
                    transaction.amount,
                    transaction.currency,
                ),
            )

    async def get(self, transaction_id: str):
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                f"SELECT {PUBLIC_COLUMNS} FROM transactions WHERE transaction_id = %s",
                (transaction_id,),
            )
            return await cursor.fetchone()

    async def process_ready_batch(self, batch_size: int) -> list[str]:
        async with self.pool.connection() as connection:
            # Short transaction: no locks or connections held throughout the 30s delay.
            # Failure/cancellation before commit leaves rows pending for the next worker.
            async with connection.transaction():
                cursor = await connection.execute(
                    "WITH ready AS ("
                    " SELECT transaction_id FROM transactions"
                    " WHERE status = 'PROCESSING' AND ready_at <= statement_timestamp()"
                    " ORDER BY ready_at, transaction_id LIMIT %s FOR UPDATE SKIP LOCKED"
                    ") UPDATE transactions AS t"
                    " SET status = 'PROCESSED', processed_at = clock_timestamp()"
                    " FROM ready WHERE t.transaction_id = ready.transaction_id"
                    " RETURNING t.transaction_id",
                    (batch_size,),
                )
                rows = await cursor.fetchall()
            return [row["transaction_id"] for row in rows]
