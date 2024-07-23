from enum import Enum
from pydantic import BaseModel, Field
from constants import (ORDER_MOVE_TOP_WHEEL,
                       ORDER_MOVE_WHOLE_STACK,
                       ORDER_MOVE_TO_LABORATORY,
                       ORDER_MERGE_WHEEL_STACKS,
                       ORDER_MOVE_TO_REJECTED,
                       ORDER_MOVE_TO_PROCESSING,
                       PRES_TYPE_PLATFORM,
                       PRES_TYPE_GRID)


# MOVE ORDER
class OrderType(str, Enum):
    moveWholeStack = ORDER_MOVE_WHOLE_STACK
    moveTopWheel = ORDER_MOVE_TOP_WHEEL
    mergeWheelStacks = ORDER_MERGE_WHEEL_STACKS


class SourcePlacementType(str, Enum):
    grid = PRES_TYPE_GRID
    basePlatform = PRES_TYPE_PLATFORM


class Source(BaseModel):
    placementType: SourcePlacementType = Field(...,
                                               description='`placementType` from which we take an object')
    placementId: str = Field(...,
                             description='`_id` of the `grid` or `basePlatform` used as `source`')
    rowPlacement: str = Field(...,
                              description='`row` identifier of a cell in a placement')
    columnPlacement: str = Field(...,
                                 description='`column` identifier of a cell in a placement')


class DestinationPlacementType(str, Enum):
    grid = PRES_TYPE_GRID


class Destination(BaseModel):
    placementType: DestinationPlacementType = Field(...,
                                                    description='`placementType` from which we take an object')
    placementId: str = Field(...,
                             description='`_id` of the `grid` used as `destination`')
    rowPlacement: str = Field(...,
                              description='`row` identifier of a cell in a placement')
    columnPlacement: str = Field(...,
                                 description='`column` identifier of a cell in a placement')


class CreateMoveOrderRequest(BaseModel):
    orderName: str = Field('',
                           description='Optional name of the `order`')
    orderDescription: str = Field('',
                                  description='Optional description of the `order`')
    source: Source = Field(...,
                           description='data to identify and validate `source` as correct one')
    destination: Destination = Field(...,
                                     description='data to identify and validate `destination` as correct one')
    orderType: OrderType = Field(...,
                                 description='type of a new `order` to create')


# LAB ORDER
class LabSourcePlacementType(str, Enum):
    grid = PRES_TYPE_GRID


class LabSource(BaseModel):
    placementType: LabSourcePlacementType = Field(...,
                                                  description='`placementType` from which we take an object')
    placementId: str = Field(...,
                             description='`_id` of the `grid` or `basePlatform` used as `source`')
    rowPlacement: str = Field(...,
                              description='`row` identifier of a cell in a placement')
    columnPlacement: str = Field(...,
                                 description='`column` identifier of a cell in a placement')


class LabDestination(BaseModel):
    placementType: LabSourcePlacementType = Field(...,
                                                  description='`placementType` from which we take an object')
    placementId: str = Field(...,
                             description='`_id` of the `grid` used as `destination`')
    elementName: str = Field(...,
                             description='Name of the `extra` element in the `grid`')


class CreateLabOrderRequest(BaseModel):
    orderName: str = Field('',
                           description='Optional name of the `order`')
    orderDescription: str = Field('',
                                  description='Optional description of the `order`')
    source: LabSource = Field(...,
                              description='data to identify and validate `source` as correct one')
    destination: LabDestination = Field(...,
                                        description='data to identify and validate `destination` as correct one')
    chosenWheel: str = Field(...,
                             description='`objectId` of a `wheel` which is going to the lab')
