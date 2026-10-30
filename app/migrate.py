"""Small, checksummed, transactional SQL migration runner: python -m app.migrate."""

import asyncio
import hashlib
from pathlib import Path

import psycopg

from app.config import Settings


async def migrate(database_url: str, *, options: str = "") -> None:
    async with await psycopg.AsyncConnection.connect(database_url, options=options) as connection:
        # Serialize concurrent migration processes; released at transaction end.
        await connection.execute("SELECT pg_advisory_xact_lock(7402101)")
        await connection.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(name TEXT PRIMARY KEY, sha256 TEXT NOT NULL, applied_at TIMESTAMPTZ DEFAULT now())"
        )
        for path in sorted(Path(__file__).with_name("migrations").glob("*.sql")):
            sql = path.read_text()
            checksum = hashlib.sha256(sql.encode()).hexdigest()
            cursor = await connection.execute(
                "SELECT sha256 FROM schema_migrations WHERE name = %s", (path.name,)
            )
            existing = await cursor.fetchone()
            if existing:
                if existing[0] != checksum:
                    raise RuntimeError(f"Applied migration was modified: {path.name}")
                continue
            await connection.execute(sql, prepare=False)
            await connection.execute(
                "INSERT INTO schema_migrations (name, sha256) VALUES (%s, %s)",
                (path.name, checksum),
            )


if __name__ == "__main__":
    asyncio.run(migrate(Settings().database_url))
