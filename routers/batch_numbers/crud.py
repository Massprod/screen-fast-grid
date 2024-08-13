from loguru import logger
from pymongo.errors import PyMongoError
from fastapi import status, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from utility.utilities import get_db_collection, log_db_record, time_w_timezone
from datetime import timedelta


async def batch_number_record_make_json_friendly(record_data: dict) -> dict:
    record_data['_id'] = str(record_data['_id'])
    record_data['createdAt'] = record_data['createdAt'].isoformat()
    if record_data['laboratoryTestDate'] is not None:
        record_data['laboratoryTestDate'] = record_data['laboratoryTestDate'].isoformat()
    return record_data


async def db_create_batch_number(
        batch_number_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None
):
    collection = await get_db_collection(db, db_name, db_collection)
    batch_number_data['createdAt'] = await time_w_timezone()
    db_log_data = await log_db_record(db_name, db_collection)
    logger.info(
        f'Creating a new `batchNumber` record in `{db_collection} collection' + db_log_data
    )
    try:
        res = await collection.insert_one(batch_number_data, session=session)
        logger.info(
            f'Successfully created a new `batchNumber` with `objectId` = {res.inserted_id}' + db_log_data
        )
        return res
    except PyMongoError as error:
        logger.error(f'Error while creating `batchNumber` = {error}' + db_log_data)
        raise HTTPException(
            detail=f'Error while creating `batchNumber`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_find_batch_number(
        batch_number: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_log_data = await log_db_record(db_name, db_collection)
    logger.info(
        f'Searching `batchNumber` record in `{db_collection} collection' + db_log_data
    )
    query = {
        'batchNumber': batch_number
    }
    try:
        res = await collection.find_one(query, session=session)
        logger.info(
            f'Successfully found `batchNumber` = {batch_number}' + db_log_data
        )
        return res
    except PyMongoError as error:
        logger.error(f'Error while searching `batchNumber` = {error}' + db_log_data)
        raise HTTPException(
            detail=f'Error while searching `batchNumber',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_find_all_batch_numbers(
        laboratory_passed: bool,
        days_delta: int,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_log_data = await log_db_record(db_name, db_collection)
    search_log: str = f'Searching all `batchNumber`s with laboratoryPassed = {laboratory_passed} | '
    query = {}
    if laboratory_passed is not None:
        query = {
            'laboratoryPassed': laboratory_passed,
        }
    if days_delta is not None:
        time_shift = timedelta(days=days_delta)
        cur_time = await time_w_timezone()
        target_time = cur_time - time_shift
        target_time = target_time.replace(hour=0, minute=0, second=0, microsecond=0)
        query = {
            'createdAt': {
                '$gte': target_time,
                '$lte': cur_time
            }
        }
        search_log += f'Limited by period start: {target_time.isoformat()} -> end: {cur_time.isoformat()}'
    logger.info(search_log + db_log_data)
    try:
        res = await collection.find(query).to_list(length=None)
        return res
    except PyMongoError as error:
        logger.error(
            f'Error while searching `batchNumber` with laboratoryPassed = {laboratory_passed} = {error}' + db_log_data
        )
        raise HTTPException(
            detail=f'Error while searching `batchNumber`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_change_lab_status(
        batch_number: str,
        laboratory_passed: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_log_data = await log_db_record(db_name, db_collection)
    query = {
        'batchNumber': batch_number,
    }
    projection = {
        '$set': {
            'laboratoryPassed': laboratory_passed,
            'laboratoryTestDate': await time_w_timezone()
        },
    }
    logger.info(
        f'Updating `batchNumber` = {batch_number} => labStatus = {laboratory_passed}' + db_log_data
    )
    try:
        res = await collection.update_one(query, projection)
        if 0 == res.matched_count:
            logger.info(
                f'`batchNumber` = {batch_number}. Not Found' + db_log_data
            )
        return res
    except PyMongoError as error:
        logger.error(
            f'Error while updating labStatus for `batchNumber` = {batch_number} => {error}' + db_log_data
        )
        raise HTTPException(
            detail='Error while updating labStatus',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )



