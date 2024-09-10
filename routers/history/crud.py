from bson import ObjectId
from loguru import logger
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient
from utility.utilities import log_db_record, get_db_collection, log_db_error_record


async def db_history_get_placement_data(
        placement_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to get `placementData` for `placementId` => {placement_id}' + db_info
    )
    query = {
        '_id': placement_id
    }
    try:
        result = await collection.find_one(query)
        logger.info(
            f'Successfully gathered `placementData` for `placementId` => {placement_id}' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while gathering `placementData` for `placementId` => {placement_id}' + db_info + error_extra
        )
        raise HTTPException(
            detail='Error while gathering data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_history_create_record(
        placement_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_info = await log_db_record(db_name, db_collection)
    placement_id: ObjectId = placement_data['placementData']['_id']
    placement_type: str = placement_data['placementType']
    logger.info(
        f'Attempt to create `historyRecord` for `placementId` => {placement_id} of type => {placement_type}' + db_info
    )
    try:
        result = await collection.insert_one(placement_data)
        logger.info(
            f'Successfully created `historyRecord` for `placementId` => {placement_id}'
            f' of type {placement_type}' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while creating `historyRecord` for `placementId` => {placement_id}'
            f' of type => {placement_type}' + db_info + error_extra
        )
        raise HTTPException(
            detail='Error while creating `historyRecord`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
