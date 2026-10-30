"""Run independently of the API: python -m app.worker."""

import asyncio
import logging
import signal

from app.config import Settings
from app.database import Database

logger = logging.getLogger(__name__)


async def run_worker(database: Database, settings: Settings, stop: asyncio.Event) -> None:
    backoff = 0.5
    while not stop.is_set():
        try:
            completed = await database.process_ready_batch(settings.worker_batch_size)
            if completed:
                logger.info("transactions_processed count=%d", len(completed))
            backoff = 0.5
            delay = (
                settings.worker_poll_seconds if len(completed) < settings.worker_batch_size else 0
            )
        except Exception as exc:
            # Pending rows remain durable; retry automatically after a transient failure.
            logger.warning(
                "worker_batch_failed error_type=%s retry_in=%.1f", type(exc).__name__, backoff
            )
            delay = backoff
            backoff = min(backoff * 2, 10)
        if delay:
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass


async def main():
    settings = Settings()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(message)s")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    database = Database(settings)
    try:
        await database.open()
        await database.health()
        logger.info("worker_started")
        await run_worker(database, settings, stop)
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
