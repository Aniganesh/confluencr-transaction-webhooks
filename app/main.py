import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import simplejson
from fastapi import FastAPI, HTTPException, Path, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from psycopg_pool import PoolTimeout, TooManyRequests

from app.config import Settings
from app.database import Database
from app.models import Acknowledgment, Health, TransactionIn, TransactionOut

logger = logging.getLogger(__name__)
MAX_BODY_BYTES = 16_384


class DecimalRequest(Request):
    async def json(self):
        return json.loads(await self.body(), parse_float=Decimal, parse_constant=Decimal)


class DecimalRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def exact_json_handler(request: Request):
            return await handler(DecimalRequest(request.scope, request.receive))

        return exact_json_handler


class DecimalJSONResponse(JSONResponse):
    """Emit exact NUMERIC values as JSON numbers without converting money to float."""

    def render(self, content) -> bytes:
        return simplejson.dumps(
            content,
            use_decimal=True,
            allow_nan=False,
            separators=(",", ":"),
            default=lambda value: value.isoformat().replace("+00:00", "Z"),
        ).encode()


class BodyLimitMiddleware:
    """Bound JSON parsing work, including bodies delivered with chunked encoding."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST":
            await self.app(scope, receive, send)
            return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > MAX_BODY_BYTES:
                await JSONResponse({"detail": "Request body too large"}, status_code=413)(
                    scope, receive, send
                )
                return
            if not message.get("more_body", False):
                break

        async def replay():
            return {"type": "http.request", "body": bytes(body), "more_body": False}

        await self.app(scope, replay, send)


def create_app(settings: Settings | None = None, database: Database | None = None) -> FastAPI:
    settings = settings or Settings()
    database = database or Database(settings)

    @asynccontextmanager
    async def lifespan(app):
        try:
            await database.open()
            await database.health()
            yield
        finally:
            await database.close()

    api = FastAPI(
        title="Transaction Webhook Service",
        version="1.0.0",
        description=(
            "Durable acknowledgments, idempotent ingestion and delayed background processing."
        ),
        lifespan=lifespan,
    )
    api.state.database = database
    api.router.route_class = DecimalRoute
    api.add_middleware(BodyLimitMiddleware)

    @api.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError):
        # Avoid reflecting non-finite numbers or account data in validation errors.
        return JSONResponse(
            {"detail": [{k: e[k] for k in ("type", "loc", "msg")} for e in exc.errors()]},
            status_code=422,
        )

    async def storage_unavailable(request: Request, exc: Exception):
        # Never log request payloads, account IDs, database URLs, or raw driver errors.
        logger.warning("storage_unavailable error_type=%s", type(exc).__name__)
        return JSONResponse(
            {"detail": "Storage temporarily unavailable; retry with the same transaction_id"},
            status_code=503,
            headers={"Retry-After": "1"},
        )

    for error in (psycopg.Error, PoolTimeout, TooManyRequests, TimeoutError):
        api.add_exception_handler(error, storage_unavailable)

    @api.get("/", response_model=Health, tags=["health"])
    async def health():
        async with asyncio.timeout(settings.request_timeout_seconds):
            await database.health()
        return Health(current_time=datetime.now(UTC))

    @api.post(
        "/v1/webhooks/transactions",
        status_code=202,
        response_model=Acknowledgment,
        responses={
            422: {"description": "Invalid transaction"},
            503: {"description": "Retry later"},
        },
        tags=["transactions"],
    )
    async def accept_webhook(transaction: TransactionIn):
        async with asyncio.timeout(settings.request_timeout_seconds):
            await database.accept(transaction)
        return Acknowledgment()

    @api.get(
        "/v1/transactions/{transaction_id}",
        response_model=list[TransactionOut],
        response_class=DecimalJSONResponse,
        responses={404: {"description": "Transaction not found"}},
        tags=["transactions"],
    )
    async def get_transaction(
        transaction_id: str = Path(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$"),
    ) -> Response:
        async with asyncio.timeout(settings.request_timeout_seconds):
            row = await database.get(transaction_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Transaction not found")
        # Explicit model validation, then retain Decimal through to the JSON encoder.
        result = TransactionOut.model_validate(row)
        return DecimalJSONResponse([result.model_dump(mode="python")])

    return api


app = create_app()
