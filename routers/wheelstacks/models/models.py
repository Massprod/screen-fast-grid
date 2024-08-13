from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, conlist, field_validator
from constants import (PS_BASE_PLATFORM, PS_GRID,
                       PS_SHIPPED, PS_LABORATORY,
                       PS_REJECTED, PRES_TYPE_GRID,
                       PRES_TYPE_PLATFORM, WS_MAX_WHEELS, WS_MIN_WHEELS)


class WheelStackStatus(str, Enum):
    basePlatform = PS_BASE_PLATFORM
    grid = PS_GRID
    shipped = PS_SHIPPED
    laboratory = PS_LABORATORY
    rejected = PS_REJECTED


# We should only allow creation on the `basePlatform`
class CreatePlacement(str, Enum):
    basePlatform = PRES_TYPE_PLATFORM


# We should only allow moving them from `basePlatform` -> `grid`.
class AllowedPlacement(str, Enum):
    grid = PRES_TYPE_GRID
    basePlatform = PRES_TYPE_PLATFORM


# def create_enum_with_empty_placeholder(name, values):
#     return Enum(name, {value: value for value in values})


class CreateWheelStackRequest(BaseModel):
    originalPisId: str = Field(...,
                               description='Original ID created by PIS, before its given to our service.')
    batchNumber: str = Field(...,
                             description="batch number of the WheelStack, we can't have wheels with different "
                                         "batch_numbers inside the one Wheelstack")
    placementType: CreatePlacement = Field(...,
                                           description='Type of the placement we want it to place into:'
                                                       'only `basePlatform` allowed')
    placementId: str = Field(...,
                             description='`objectId` of the `basePlatform` on which we want to place it')
    rowPlacement: str = Field(...,
                              description='Current identifier of the `row` this `wheelStack` '
                                          'is placed')
    colPlacement: str = Field(...,
                              description='Current identifier of the `column` this '
                                          '`wheelStack` is placed')
    # createdAt: datetime = Field(...,
    #                             description='`datetime` timestamp of creation, in this DB')
    # lastChange: datetime = Field(...,
    #                              description='`datetime` timestamp of the last change of this `wheelStack`')
    lastOrder: Optional[str] = Field(None,
                                     description='`orderId` id of the last order executed on this `wheelStack`')
    maxSize: int = Field(...,
                         description='Maximum amount of wheels available for placement in this `wheelStack`',
                         ge=WS_MIN_WHEELS + 1,
                         lt=WS_MAX_WHEELS + 1,
                         )

    blocked: bool = Field(...,
                          description="Anytime order placed on this `wheelStack` to move or merge or anything else,"
                                      " for now we're just blocking both `wheelStack`'s until order is done."
                                      " So it's a mark of availability of this `wheelStack`"
                                      "  if it's blocked we shouldn't be able to do anything with it.")
    wheels: conlist(str, min_length=0, max_length=7) = Field(
        default_factory=list,
        description="list with all `objectId`s of the `wheel`'s to store in this `wheelStack`."
                    " We're using array because we should be able to easily maintain order."
                    " And our wheels placed like 0 -> 5 - indexes, from bottom -> top.",
    )
    status: WheelStackStatus = Field(...,
                                     description=f"Current placement.\n"
                                                 f"`{PS_LABORATORY}` - in the laboratory\n"
                                                 f"`{PS_SHIPPED}` - completed product\n"
                                                 f"`{PS_GRID}` - currently positioned in our grid\n"
                                                 f"`{PS_BASE_PLATFORM}` - currently positioned on platform (before "
                                                 "grid)\n"
                                                 f"`{PS_REJECTED}` - marked as rejected and removed")

    @field_validator('wheels')
    def validate_uniqueness(cls, wheels: list[str]):
        if len(wheels) != len(set(wheels)):
            raise ValueError('Each `objectId` of the `wheels` should be unique.')
        return wheels


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
                "placementType": PRES_TYPE_PLATFORM,
                "placementId": '',
                "rowPlacement": "A",
                "colPlacement": "1",
                # "createdAt": "2023-05-01T00:00:00Z",
                # "lastChange": "2023-05-01T00:00:00Z",
                "lastOrder": None,
                "maxSize": 6,
                "blocked": False,
                "wheels": [],
                "status": PS_BASE_PLATFORM,
            }
        }
        use_enum_values = True  # using values instead of Names


class ForceUpdateWheelStackRequest(BaseModel):
    originalPisId: str = Field(...,
                               description='original id from `pis` system')
    batchNumber: str = Field(...,
                             description='`batchNumber` of the wheels inside')
    placementType: AllowedPlacement = Field(None,
                                            description='Type of the placement we want it to place into:'
                                                        'only `grid` and `basePlatform` allowed')
    placementId: str = Field(...,
                             description='`objectId` of the placement on which we want to place it')
    rowPlacement: str = Field(...,
                              description='Desired `row` of the cell in which this should be placed')
    colPlacement: str = Field(None,
                              description='Desired `col` of the cell in which this should be placed')
    lastOrder: Optional[str | None] = Field(None,
                                            description='`orderId` id of the last order executed on this `wheelStack`')
    maxSize: int = Field(...,
                         description='Maximum amount of wheels available for placement in this `wheelStack`',
                         ge=WS_MIN_WHEELS + 1,
                         lt=WS_MAX_WHEELS + 1,
                         )
    blocked: bool = Field(False,
                          description="Anytime order placed on this `wheelStack` to move or merge or "
                                      "anything else,"
                                      " for now we're just blocking both `wheelStack`'s until order is done."
                                      " So it's a mark of availability of this `wheelStack`"
                                      "  if it's blocked we shouldn't be able to do anything with it.")
    wheels: conlist(
        str,
        min_length=WS_MIN_WHEELS + 1,
        max_length=WS_MAX_WHEELS
    ) = Field(default_factory=list,
              description="list with all the `wheel`'s placed in this `wheelStack`."
                          " We're using array because we should be able to easily maintain order."
                          " And our wheels placed like 0 -> 5 - indexes, from bottom -> top.")
    status: WheelStackStatus = Field(...,
                                     description=f"Current placement.\n"
                                                 f"`{PS_LABORATORY}` - in the laboratory\n"
                                                 f"`{PS_SHIPPED}` - completed product\n"
                                                 f"`{PS_GRID}` - currently positioned in our grid\n"
                                                 f"`{PS_BASE_PLATFORM}` - currently positioned on platform (before "
                                                 "grid)\n"
                                                 f"`{PS_REJECTED}` - marked as rejected and removed")

    @field_validator('wheels')
    def validate_uniqueness(cls, wheels: list[str]):
        if len(wheels) != len(set(wheels)):
            raise ValueError('Each `objectId` of the `wheels` should be unique.')
        return wheels

    class Config:
        json_schema_extra = {
            "example": {
                "originalPisId": "no_limit",
                "batchNumber": "no_limit",
                "placementType": "grid",
                "placementId": '6699f225c41ba1feabc771c2',
                "rowPlacement": "no_limit",
                "colPlacement": "no_limit",
                "lastOrder": None,
                "maxSize": 6,
                "blocked": False,
                "wheels": ["no_limit", "no_limit", "no_limit"],
                "status": PS_BASE_PLATFORM,
            }
        }
        use_enum_values = True  # using values instead of Name
