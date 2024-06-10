from enum import Enum
from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator, model_validator


class WheelStackStatus(str, Enum):
    orderQue = 'orderQue'  # Waiting for order to be executed
    shipped = 'shipped'  # Moved from the grid (removed)
    inActive = 'inActive'  # Exist 0)


class WheelStackPlacement(str, Enum):
    grid = 'grid'  # Positioned in the grid
    base = 'base'  # Positioned in the basePlatform


class CreateWheelStackRequest(BaseModel):
    originalPisId: str = Field(...,
                               description='Original ID created by PIS, before its given to our service.')
    batchNumber: str = Field(...,
                             description="batch number of the WheelStack, we can't have wheels with different "
                                         "batchNumbers inside the one Wheelstack")
    placement: WheelStackPlacement = Field(...,
                                           description='Current placement of this `wheelStack`')
    rowPlacement: str = Field(...,
                              description='Current identifier of the `row` this `wheelStack` is placed')
    colPlacement: str = Field(...,
                              description='Current identifier of the `column` this `wheelStack` is placed')
    # createdAt: datetime = Field(...,
    #                             description='`datetime` timestamp of creation, in this DB')
    # lastChange: datetime = Field(...,
    #                              description='`datetime` timestamp of the last change of this `wheelStack`')
    lastOrder: Optional[str] = Field(None,
                                     description='`orderId` id of the last order executed on this `wheelStack`')
    maxSize: int = Field(...,
                         description='Maximum amount of wheels available for placement in this `wheelStack`',
                         ge=1,
                         lt=7,
                         )

    blocked: bool = Field(...,
                          description="Anytime order placed on this `wheelStack` to move or merge or anything else,"
                                      " for now we're just blocking both `wheelStack`'s until order is done."
                                      " So it's a mark of availability of this `wheelStack`"
                                      "  if it's blocked we shouldn't be able to do anything with it.")
    wheels: List[str] = Field(default_factory=list,
                              description="list with all the `wheel`'s place in this `wheelStack`."
                                          " We're using array because we should be able to easily maintain order."
                                          " And our wheels placed like 0 -> 5 - indexes, from bottom -> top.")
    status: WheelStackStatus = Field(...,
                                     description="Status of the `wheelStack`."
                                                 "`orderQue` - waiting for order to be executed,"
                                                 "`shipped` - moved from the grid (removed),"
                                                 "`grid` - currently positioned in our grid,"
                                                 "`basePlatform` - currently positioned on platform (before grid)")

    # @field_validator('createdAt', 'lastChange')
    # def validate_date(cls, date: datetime):
    #     if date.tzinfo is None:
    #         date = date.replace(tzinfo=timezone.utc)
    #     if date > datetime.now(timezone.utc):
    #         raise ValueError("`datetime` fields shouldn't be from a future")
    #     return date

    # @model_validator(mode='after')
    # def check_changes(self):
    #     creation_time = self.createdAt
    #     change_time = self.lastChange
    #     if creation_time and change_time and change_time < creation_time:
    #         raise ValueError("`lastChange` can't be made earlier than time of creation `createdAt`")
    #     return self

    class Config:
        json_schema_extra = {
            "example": {
                "originalPisId": "PIS12345",
                "batchNumber": "batch12345",
                "placement": "base",
                "rowPlacement": "A",
                "colPlacement": "1",
                # "createdAt": "2023-05-01T00:00:00Z",
                # "lastChange": "2023-05-01T00:00:00Z",
                "lastOrder": None,
                "maxSize": 6,
                "blocked": False,
                "wheels": [],
                "status": "inActive"
            }
        }


class UpdateWheelStackRequest(BaseModel):
    # We need A LOT of extra checks, we can have out-of-limits ROWs, COLs.
    # We can have status set to base, when we get placement for GRID etc...
    placement: Optional[WheelStackPlacement] = Field(None,
                                                     description='Current identifier of the `row` this `wheelStack` '
                                                                 'is placed')
    rowPlacement: Optional[str] = Field(None,
                                        description='Current identifier of the `row` this `wheelStack` is placed')
    colPlacement: Optional[str] = Field(None,
                                        description='Current identifier of the `column` this `wheelStack` is placed')
    # lastOrder: Optional[str] = Field(None,
    #                                  description='`orderId` id of the last order executed on this `wheelStack`')
    blocked: Optional[bool] = Field(None,
                                    description="Anytime order placed on this `wheelStack` to move or merge or "
                                                "anything else,"
                                                " for now we're just blocking both `wheelStack`'s until order is done."
                                                " So it's a mark of availability of this `wheelStack`"
                                                "  if it's blocked we shouldn't be able to do anything with it.")
    wheels: Optional[List[str]] = Field(default_factory=list,
                                        description="list with all the `wheel`'s place in this `wheelStack`."
                                                    "We're using array because we should be able to easily maintain "
                                                    "order."
                                                    " And our wheels placed like 0 -> 5 - indexes, from bottom -> top.")
    status: Optional[WheelStackStatus] = Field(None,
                                               description="Status of the `wheelStack`."
                                                           "`orderQue` - waiting for order to be executed,"
                                                           "`shipped` - moved from the grid (removed),"
                                                           "`grid` - currently positioned in our grid,"
                                                           "`basePlatform` - currently positioned on platform (before "
                                                           "grid)")

    class Config:
        json_schema_extra = {
            "example": {
                "placement": "grid",
                "rowPlacement": "B",
                "colPlacement": "3",
                "blocked": False,
                "wheels": [],
                "status": "inActive"
            }
        }
