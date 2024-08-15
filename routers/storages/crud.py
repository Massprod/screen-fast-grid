from loguru import logger
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient
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
        storage_object_id: str,
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
            f'Error while searching for `storage` document with name = {storage_object_id}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while searching for `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
