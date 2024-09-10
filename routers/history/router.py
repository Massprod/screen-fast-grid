from bson import ObjectId
from loguru import logger
from fastapi.responses import JSONResponse
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import APIRouter, Depends, status, Body
from constants import DB_PMK_NAME, CLN_PLACEMENT_HISTORY
from routers.history.crud import db_history_create_record
from routers.history.models.models import ForceHistoryRecord
from routers.history.history_actions import gather_placement_history_data
from utility.utilities import get_object_id, convert_object_id_and_datetime_to_str


# We need to record at times:
#  - creating `basePlatform`
#  - creating `grid`
#  - creating and placing `wheelstack` <- we always (create and place it) at the same time
#  - creating `storage`
# All 3 types of placements should be recorded by themselves, but the overall schema is the same.
# All of them have `wheelstacks`, `wheels`, `placementData` and `placementOrders`.


router = APIRouter()


@router.post(
    path='/create',
    description='Force the creation of a history record for the chosen placement',
    name='Create History Record',
)
async def route_post_force_history_record(
        placement_info: ForceHistoryRecord = Body(...,
                                                  description='Basic required placement data'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        # ADD ADMIN TOKEN
):
    placement_info = placement_info.model_dump()
    placement_object_id: ObjectId = await get_object_id(placement_info['placementId'])
    placement_type: str = placement_info['placementType']
    logger.info(f'ID: {placement_object_id} | Type: {placement_type}')
    placement_data = await gather_placement_history_data(placement_object_id, placement_type, db)
    history_record = await db_history_create_record(
        placement_data, db, DB_PMK_NAME, CLN_PLACEMENT_HISTORY
    )
    created_id: str = convert_object_id_and_datetime_to_str(history_record.inserted_id)
    return JSONResponse(
        content={
            '_id': created_id
        },
        status_code=status.HTTP_200_OK,
    )
