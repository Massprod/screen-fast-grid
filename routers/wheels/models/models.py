from enum import Enum
from typing import Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator


class WheelStatus(str, Enum):
    laboratory = 'laboratory'
    shipped = 'shipped'
    orderQue = 'orderQue'
    basePlatform = 'basePlatform'
    grid = 'grid'


class WheelStackData(BaseModel):
    wheelStackId: str
    wheelStackPosition: int


class CreateWheelRequest(BaseModel):
    wheelId: str = Field(..., description="Unique identifier for the wheel.")
    batchNumber: str = Field(..., description="Batch number associated with the wheel.")
    wheelDiameter: int = Field(...,
                               gt=0,
                               lt=100000,
                               description="Diameter of the wheel in mm. Must be a positive integer.")
    receiptDate: datetime = Field(..., description="The date the wheel was received in ISO 8601 format.")
    status: WheelStatus = Field(...,
                                description="Current status of the wheel. Possible values are 'laboratory', "
                                            "'shipped', 'orderQue', 'basePlatform', 'grid'.")
    wheelStack: Optional[WheelStackData] = Field(None,
                                                 description='data of the `wheelStack` in which this wheel is placed')

    @field_validator('receiptDate')
    def validate_receipt_date(cls, date: datetime):
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        if date > datetime.now(timezone.utc):
            raise ValueError('`receiptDate` cannot be in the future')
        return date
