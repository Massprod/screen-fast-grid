from bson import ObjectId
from loguru import logger
from datetime import datetime, timezone
from fastapi.responses import JSONResponse
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from constants import DB_PMK_NAME, CLN_PLACEMENT_HISTORY
from fastapi import APIRouter, Depends, status, Body, Query
from routers.history.history_actions import gather_placement_history_data
from routers.history.models.models import ForceHistoryRecord, BasicPlacementTypes
from routers.history.crud import db_history_create_record, db_history_get_records
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
        # TODO:
        #  ADD ADMIN TOKEN
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


@router.get(
    path='/all',
    description='Gathers and returns all db records in provided period.'
                ' If no period provided, returns all records',
    name='Get History Records',
)
async def route_get_history_records(
        include_data: bool = Query(
            default=True,
            description='Include data of the records,'
                        ' or just provide their basic info: `_id`, `createdAt, `placementType`',
        ),
        period_start: datetime = Query(
            default=datetime(1970, 1, 1, tzinfo=timezone.utc),
            description='Start date of the period, default is Unix Epoch (start of time)',
        ),
        period_end: datetime = Query(
            default=datetime.now(timezone.utc),
            description='End date of the period, default is time of the request',
        ),
        placement_id: str = Query(
            None,
            description='`ObjectId` of a placement to filter records on',
        ),
        placement_type: BasicPlacementTypes = Query(
            None,
            description='`placementType` of a placement to filter records on',
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    if placement_id:
        placement_id: ObjectId = await get_object_id(placement_id)
    history_records: list[dict] = await db_history_get_records(
        include_data, period_start, period_end, db, DB_PMK_NAME, CLN_PLACEMENT_HISTORY, placement_id, placement_type
    )
    cor_history_records: list[dict] = convert_object_id_and_datetime_to_str(history_records)
    return JSONResponse(
        content=cor_history_records,
        status_code=status.HTTP_200_OK,
    )
