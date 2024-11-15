from loguru import logger
from bson import ObjectId
from constants import OUT_STATUSES
from pymongo.errors import PyMongoError
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from utility.utilities import get_db_collection, log_db_record, log_db_error_record, time_w_timezone


async def wheel_make_json_friendly(wheel_data):
    wheel_data['_id'] = str(wheel_data['_id'])
    wheel_data['receiptDate'] = wheel_data['receiptDate'].isoformat()
    if 'wheelStack' in wheel_data and wheel_data['wheelStack']:
        if 'wheelStackId' in wheel_data['wheelStack']:
            wheel_data['wheelStack']['wheelStackId'] = str(wheel_data['wheelStack']['wheelStackId'])
    return wheel_data


async def db_find_wheel(
        wheel_id: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        wheel_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheel_collection.find_one({'wheelId': wheel_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error finding wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database query error")


async def db_find_wheel_by_object_id(
        wheel_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
):
    try:
        wheel_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheel_collection.find_one({'_id': wheel_object_id}, session=session)
        return res
    except PyMongoError as e:
        logger.error(f"Error finding wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database query error")


async def db_update_wheel_status(
        wheel_object_id: ObjectId,
        new_status: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None
):
    wheel_collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': wheel_object_id,
    }
    update = {
        '$set': {
            'status': new_status,
        }
    }
    try:
        res = await wheel_collection.update_one(query, update, session=session)
        return res
    except PyMongoError as e:
        logger.error(f"Error updating wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database update error")


async def db_update_wheel_position(
        wheel_object_id: ObjectId,
        new_position: int,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession,
):
    wheel_collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': wheel_object_id,
    }
    update = {
        '$set': {
            'wheelStack.wheelStackPosition': new_position,
        }
    }
    try:
        res = await wheel_collection.update_one(query, update, session=session)
        return res
    except PyMongoError as e:
        logger.error(f"Error updating wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database update error")


async def db_update_wheel(
        wheel_object_id: ObjectId,
        wheel_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None
):
    try:
        wheel_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheel_collection.update_one(
            {'_id': wheel_object_id},
            {'$set': wheel_data},
            session=session,
        )
        return res
    except PyMongoError as e:
        logger.error(f"Error updating wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database update error")


async def db_insert_wheel(
        wheel_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
):
    try:
        wheel_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheel_collection.insert_one(wheel_data, session=session)
        return res
    except PyMongoError as e:
        logger.error(f"Error inserting wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database insertion error")


async def db_delete_wheel(
        wheel_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        wheel_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheel_collection.delete_one({'_id': wheel_object_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error deleting wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database deletion error")


async def db_get_all_wheels(
        filters: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_log_data = await log_db_record(db_name, db_collection)
    filter_record: list[str] = []
    query = {}
    for key, value in filters.items():
        if value:
            filter_record.append(
                f'`{key}` = {value}'
            )
            query[key] = value
    
    if filter_record:
        logger.info(
            f'Searching `wheels` with filters: {'|'.join(filter_record)} | {db_log_data}'
        )
    else:
        logger.info(
            f'Searching all `wheels` in the collection' + db_log_data
        )
    try:
        res = await collection.find(query).to_list(length=None)
        logger.info(
            f'Successfully found all `wheels`' + db_log_data
        )
        return res
    except PyMongoError as error:
        logger.error(f"Error getting `wheels` = {error}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database insertion error"
        )


async def db_get_wheels_by_transfer_data(
        include_data: bool,
        transfer_status: bool,
        correct_status: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to gather all `wheel`s with include_data => {include_data}'
        f' & transfer_status => {transfer_status}' + db_info
    )
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        'transferData.transferStatus': transfer_status,
    }
    if correct_status:
        query['status'] = {
            '$in': OUT_STATUSES
        }
    projection = {}
    if not include_data:
        projection['_id'] = True
    try:
        result = await collection.find(query, projection).to_list(length=None)
        logger.info(
            f'Successfully gathered all `wheel` documents'
            f' with include_data => {include_data} & transfer_status => {transfer_status}' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            'Error while gathering wheels' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while gathering `wheel` documents',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_update_wheel_transfer_status(
        wheel_object_id: ObjectId,
        transfer_status: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to update `transferStatus` of the `wheel` document with `_id` => {wheel_object_id}' + db_info
    )
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': wheel_object_id,
        'status': {
            '$in': OUT_STATUSES
        },
    }
    update = {
        '$set': {
            'transferData.transferStatus': transfer_status,
            'transferData.transferDate': await time_w_timezone()
        }
    }
    try:
        result = await collection.update_one(query, update)
        logger.info(
            f'Successfully updated all `wheel` document with `_id` => {wheel_object_id} ' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while updating `transferStatus` of the `wheel` document with `_id` => {wheel_object_id}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while updating `wheel` document',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_find_many_wheels_by_id(
        wheel_object_ids: list[ObjectId],
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to gather many `wheel`s' + db_info
    )
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': {
            '$in': wheel_object_ids
        }
    }
    try:
        result = await collection.find(query).to_list(length=None)
        logger.info(
            f'Successfully found all `wheel`s' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while gathering `wheel`s documents' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while gathering `wheel`s documents',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_find_wheels_free_fields(
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        filter_fields: dict,
        session: AsyncIOMotorClientSession = None,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Gathering `wheel`s with free filtering'
    )
    collection = await get_db_collection(db, db_name, db_collection)
    query = {}
    for field, value in filter_fields.items():
        query[field] = value
    try:
        result = await collection.find(query).to_list(length=None)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while executing free filter `wheel`s DB request' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while gathering `wheel`s documents',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )