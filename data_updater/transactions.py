import datetime

from typing import Optional
from decimal import Decimal
from pydantic import BaseModel, Field, validator

# We use a pydantic BaseModel to validate the format of each transaction retrieved through the API
# It also allows use to immediately create a transaction_date that will help us setting up the start_date when ingesting the new data
# And it could be the target column to partition the data (to be ingested in Snowflake for example)


class Transaction(BaseModel):
    transaction_datetime_epoch: int = Field(..., alias="Transaction datetime")
    transaction_date: Optional[str]
    amount_from: Decimal = Field(..., alias="Amount_from")
    currency_from: str = Field(..., alias="Currency_from")
    currency_to: str = Field(..., alias="CurrencyTo")
    status: str = Field(..., alias="Status")

    @validator("transaction_date", always=True)
    def set_transaction_date(cls, _, values):
        epoch_dt = (
            # If we have a 13 digits epoch, we remove the millisec
            values["transaction_datetime_epoch"] // 1000
            if values["transaction_datetime_epoch"] > 9999999999
            else values["transaction_datetime_epoch"]
        )
        return datetime.datetime.utcfromtimestamp(epoch_dt).strftime("%Y-%m-%d")
