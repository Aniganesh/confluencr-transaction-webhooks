from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, WithJsonSchema, field_validator

Identifier = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")]
Amount = Annotated[
    Decimal,
    Field(gt=0, max_digits=20, decimal_places=4, allow_inf_nan=False),
    WithJsonSchema({"type": "number", "exclusiveMinimum": 0}),
]


class TransactionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transaction_id: Identifier
    source_account: Identifier
    destination_account: Identifier
    amount: Amount
    currency: str = Field(pattern=r"^[A-Z]{3}$")

    @field_validator("amount", mode="before")
    @classmethod
    def numeric_amount(cls, value):
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise ValueError("amount must be a JSON number")
        return value


class TransactionOut(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: Amount
    currency: str
    status: Literal["PROCESSING", "PROCESSED"]
    created_at: datetime
    processed_at: datetime | None


class Acknowledgment(BaseModel):
    status: Literal["ACCEPTED"] = "ACCEPTED"


class Health(BaseModel):
    status: Literal["HEALTHY"] = "HEALTHY"
    current_time: datetime
