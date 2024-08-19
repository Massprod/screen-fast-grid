from bson import ObjectId
from loguru import logger
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from utility.utilities import get_db_collection, time_w_timezone, log_db_record, log_db_error_record


async def db_storage_make_json_friendly(storage_data: dict) -> dict:
    storage_data['_id'] = str(storage_data['_id'])
    storage_data['createdAt'] = storage_data['createdAt'].isoformat()
    storage_data['lastChange'] = storage_data['lastChange'].isoformat()
    if 'elements' in storage_data:
        for element_id in storage_data['elements']:
            storage_data['elements'][element_id] = str(storage_data['elements'][element_id])
    return storage_data


async def db_create_storage(
        storage_name: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(f'Attempt to create a `storage` document with name = {storage_name}' + db_info)
    collection = await get_db_collection(db, db_name, db_collection)
    creation_time = await time_w_timezone()
    storage_data = {
        'name': storage_name,
        'createdAt': creation_time,
        'lastChange': creation_time,
        'elements': {},
    }
    try:
        result = await collection.insert_one(storage_data)
        logger.info(f'Successfully created `storage` document with name = {storage_name}' + db_info)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(f'Error while creating `storage` document with name = {storage_name}' + db_info + error_extra)
        raise HTTPException(
            detail=f'Error while creating `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_storage_by_name(
        storage_name: str,
        include_data: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(f'Attempt to search a `storage` document with name = {storage_name}' + db_info)
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        'name': storage_name,
    }
    projection = {
        'name': True,
        'createdAt': True,
        'lastChange': True,
    }
    if include_data:
        projection['elements'] = True
    try:
        result = await collection.find_one(query, projection)
        if result is None:
            logger.info(f"Unsuccessful search for `storage` document with name = {storage_name}" + db_info)
        else:
            logger.info(f'Successful search for `storage` document with name = {storage_name}' + db_info)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while searching for `storage` document with name = {storage_name}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while searching for `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_storage_by_object_id(
        storage_object_id: ObjectId,
        include_data: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(f'Attempt to search a `storage` document with `objectId` = {storage_object_id}' + db_info)
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': storage_object_id,
    }
    projection = {
        'name': True,
        'createdAt': True,
        'lastChange': True,
    }
    if include_data:
        projection['elements'] = True
    try:
        result = await collection.find_one(query, projection)
        if result is None:
            logger.info(f"Unsuccessful search for `storage` document with name = {storage_object_id}" + db_info)
        else:
            logger.info(f'Successful search for `storage` document with name = {storage_object_id}' + db_info)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while searching for `storage` document with `objectId` = {storage_object_id}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while searching for `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_storage_place_wheelstack(
        storage_object_id: ObjectId,
        wheelstack_id: ObjectId,
        batch_number: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to add a new `wheelstack` = {wheelstack_id} into'
        f' `storage` with `objectId` = {storage_object_id} ' + db_info
    )
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': storage_object_id,
    }
    update = {
        '$set': {
            f'elements.{batch_number}.{wheelstack_id}': wheelstack_id,
        }
    }
    if record_change:
        update['$set']['lastChange'] = await time_w_timezone()
    try:
        result = await collection.update_one(query, update, session=session)
        log_mes = f'add operation for `storage` document with name = {storage_object_id}' + db_info
        if 0 == result.modified_count:
            logger.info(f'Unsuccessful {log_mes}')
        else:
            logger.info(f'Successful {log_mes}')
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while adding new element into `storage`'
            f' document with `objectId` = {storage_object_id}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while adding new element into `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_storage_check_placed_wheelstack(
        storage_object_id: ObjectId,
        wheelstack_object_id: ObjectId,
        batch_number_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': storage_object_id,
        f'elements.{batch_number_object_id}.{wheelstack_object_id}': wheelstack_object_id,
    }
    try:
        result = await collection.find_one(query)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while searching element in `storage`'
            f' document with `objectId` = {storage_object_id}' + error_extra
        )
        raise HTTPException(
            detail=f'Error while adding new element into `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_storage_delete_placed_wheelstack(
        storage_object_id: ObjectId,
        wheelstack_object_id: ObjectId,
        batch_number_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession,
        record_change: bool = True,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': storage_object_id,
        f'elements.{batch_number_object_id}.{str(wheelstack_object_id)}': {
            '$exists': True,
        },
    }
    update = {
        '$unset': {
            f'elements.{batch_number_object_id}.{str(wheelstack_object_id)}': 1,
        },
    }
    if record_change:
        update['$set'] = {
            'lastChange': await time_w_timezone()
        }
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        raise HTTPException(
            detail=f'Error while deleting placed `wheelstack`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
