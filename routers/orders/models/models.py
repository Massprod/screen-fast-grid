from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum
from constants import *


class OrderType(str, Enum):
    moveWholeStack = ORDER_MOVE_WHOLE_STACK
    moveTopWheel = ORDER_MOVE_TOP_WHEEL
    moveToLaboratory = ORDER_MOVE_TO_LABORATORY
    mergeWheelStacks = ORDER_MERGE_WHEEL_STACKS


class CreateOrderRequest(BaseModel):
    orderType: OrderType = Field(..., description="Type of operation (moveWholeStack, moveTopWheel, moveToLaboratory, mergeWheelStack)")
    source: dict = Field(..., description="Source type and identifier (row and column placement of the wheelstack in `type`)")
    destination: dict = Field(..., description="Destination type and identifier (row and column placement of the wheelstack in `type`)")
    orderName: Optional[str] = Field("", description="Optional order name")
    orderDescription: Optional[str] = Field("", description="Optional order description")

    class Config:
        json_schema_extra = {
            'example': {
                'orderType': 'moveWholeStack',
                'source': {
                    'type': 'basePlacement',
                    'identifier': '1,1',
                },
                'destination': {
                    'type': 'grid',
                    'identifier': 'D,1',
                },
                'orderName': 'testRequestOrder',
                'orderDescription': 'testRequestOrderDescription',
            }
        }
