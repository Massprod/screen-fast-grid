from bson import ObjectId
from loguru import logger
from datetime import datetime
from pymongo.errors import PyMongoError
from fastapi import HTTPException, status
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


async def db_history_get_records(
        include_data: bool,
        period_start: datetime,
        period_end: datetime,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        placement_id: ObjectId | None = None,
        placement_type: str | None = None,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_info = await log_db_record(db_name, db_collection)
    query = {
        'createdAt': {
            '$gte': period_start,
            '$lte': period_end,
        }
    }
    projection = {}
    log_str: str = f'Attempt to gather all `historyRecord`s in period: {period_start} => {period_end}'
    if placement_id:
        log_str += f'| For the `placementId` => {placement_id}'
        query['placementData._id'] = placement_id
    if placement_type:
        log_str += f' of type => {placement_type}'
        query['placementType'] = placement_type
    if not include_data:
        projection = {
            '_id': 1,
            'createdAt': 1,
            'placementType': 1,
        }
    log_str += f'| Record data included: {include_data}'
    logger.info(log_str + db_info)
    try:
        result = await collection.find(query, projection).to_list(length=None)
        log_str = f'Successfully gathered `historyRecord`s data if period: {period_start} => {period_end}'
        if placement_id:
            log_str += f'| For the `placementId` => {placement_id}'
        if placement_type:
            log_str += f' of type => {placement_type}'
        log_str += f'| Record data included: {include_data}'
        logger.info(log_str + db_info)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        error_str: str = f'Error while gathering all `historyRecord`s in period: {period_start} => {period_end}'
        if placement_id:
            error_str += f'| For the `placementId` => {placement_id}'
        if placement_type:
            error_str += f' of type => {placement_type}'
        error_str += f'| Record data included: {include_data}'
        logger.error(error_str + db_info + error_extra)
        raise HTTPException(
            detail='Error while gathering `historyRecord`s data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_history_get_record(
        include_data: bool,
        record_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_info = await log_db_record(db_name, db_collection)
    query = {
        '_id': record_id,
    }
    projection = {}
    if not include_data:
        projection = {
            '_id': 1,
            'createdAt': 1,
            'placementType': 1,
        }
    log_str: str = f'Attempt to gather `historyRecord` data => {record_id} | With data included = {include_data}'
    logger.info(log_str + db_info)
    try:
        result = await collection.find_one(query, projection)
        logger.info(f'Successfully gathered `historyRecord` data => {record_id}')
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        error_str: str = f'Error while gathering `historyRecord` data => {record_id}'
        logger.error(error_str + db_info + error_extra)
        raise HTTPException(
            detail='Error while gathering `historyRecord` data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
