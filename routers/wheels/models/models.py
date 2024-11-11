from enum import Enum
from typing import Optional
from fastapi import HTTPException, status
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator
from constants import (PS_GRID, PS_SHIPPED, PS_REJECTED,
                       PS_LABORATORY, PS_BASE_PLATFORM,
                       WS_MIN_WHEELS, WS_MAX_WHEELS, PS_STORAGE, WH_UNPLACED)


class WheelStatus(str, Enum):
    laboratory = PS_LABORATORY
    shipped = PS_SHIPPED
    basePlatform = PS_BASE_PLATFORM
    grid = PS_GRID
    rejected = PS_REJECTED
    storage = PS_STORAGE
    unplaced = WH_UNPLACED


class WheelStackData(BaseModel):
    wheelStackId: str
    wheelStackPosition: int = Field(...,
                                    description='Maximum 6 wheels, 0 -> 6 indexes, inclusive',
                                    ge=WS_MIN_WHEELS,
                                    lt=WS_MAX_WHEELS,
                                    )


class CreateWheelRequest(BaseModel):
    wheelId: str = Field(..., description="Unique identifier for the wheel.")
    batchNumber: str = Field(..., description="Batch number associated with the wheel.")
    receiptDate: datetime = Field(..., description="The date the wheel was received in ISO 8601 format.")
    status: WheelStatus = Field(...,
                                description="Current placement status of the Wheel")
    wheelStack: Optional[WheelStackData] = Field(None,
                                                 description='Data of `wheelStack`, to which wheel is assigned')
    sqlData: Optional[dict] = Field(None,
                                    description='Transferred data from `sql` service')

    @field_validator('receiptDate')
    def validate_receipt_date(cls, date: datetime):
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        if date > datetime.now(timezone.utc):
            raise HTTPException(
                detail='`receiptDate` cannot be in the future',
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return date
