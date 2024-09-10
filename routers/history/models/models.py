from enum import Enum
from pydantic import BaseModel, Field
from constants import PT_BASE_PLATFORM, PT_GRID, PT_STORAGE


class BasicPlacementTypes(str, Enum):
    basePlatform = PT_BASE_PLATFORM
    grid = PT_GRID
    storage = PT_STORAGE


class ForceHistoryRecord(BaseModel):
    placementId: str = Field(...,
                             description='`objectId` of the placement to use')
    placementType: BasicPlacementTypes = Field(...,
                                               description='`placementType` of the placement to use |'
                                                           ' Used to identify collection')
