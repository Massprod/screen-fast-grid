from bson import ObjectId
from loguru import logger
from pymongo.errors import PyMongoError
from fastapi import status, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from utility.utilities import get_db_collection, time_w_timezone, log_db_record


async def platform_make_json_friendly(platform_data):
    platform_data['_id'] = str(platform_data['_id'])
    platform_data['preset'] = str(platform_data['preset'])
    platform_data['createdAt'] = platform_data['createdAt'].isoformat()
    platform_data['lastChange'] = platform_data['lastChange'].isoformat()
    for row in platform_data['rows']:
        for col in platform_data['rows'][row]['columns']:
            field = platform_data['rows'][row]['columns'][col]
            if field['wheelStack'] is not None:
                field['wheelStack'] = str(field['wheelStack'])
            if field['blockedBy'] is not None:
                field['blockedBy'] = str(field['blockedBy'])
    if 'extra' in platform_data:
        for extra_element in platform_data['extra']:
            if 'orders' in platform_data['extra'][extra_element]:
                orders = platform_data['extra'][extra_element]['orders']
                for order in orders:
                    orders[order] = str(orders[order])
    return platform_data


async def get_all_platforms_data(
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        res = await collection.find({}).to_list(length=None)
        return res
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_all_platforms(
        include_data: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {}
    projection = {}
    if not include_data:
        projection = {
            'rowsOrder': 0,
            'rows': 0,
            'extra': 0,
        }
    try:
        res = await collection.find(query, projection).to_list(length=None)
        return res
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_platform_by_object_id(
        platform_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        platform = await collection.find_one({'_id': platform_object_id})
        return platform
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_platform_preset_by_object_id(
        platform_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        preset_id = await collection.find_one(
            {'_id': platform_object_id},
            {'preset': 1},
        )
        return preset_id
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_platform_by_name(
        platform_name: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        platform = await collection.find_one(
            {'name': platform_name}
        )
        return platform
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}`')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def create_platform(
        preset_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    platform_data = {
        'preset': preset_data['_id'],
        'name': preset_data['name'],
        'createdAt': await time_w_timezone(),
        'lastChange': await time_w_timezone(),
        'rowsOrder': preset_data['rowsOrder'],
        'rows': preset_data['rows'],
        'extra': preset_data['extra'],
    }
    collection = await get_db_collection(db, db_name, db_collection)
    db_log_data = await log_db_record(db_name, db_collection)
    logger.info(
        f'Creating a new `platform` record in `{db_collection}` collection'
        f' with `preset` = {preset_data['_id']}' + db_log_data
    )
    try:
        res = await collection.insert_one(platform_data)
        logger.info(
            f'Successfully created a new `platform` with `objectId` = {res.inserted_id}' + db_log_data
        )
        return res
    except PyMongoError as error:
        logger.error(f'Error while creating `platform` = {error}' + db_log_data)
        raise HTTPException(
            detail=f'Error while creating `platform`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def cell_exist(
        platform_id,
        row: str,
        col: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': platform_id,
        f'rows.{row}.columns.{col}.wheelStack': {
            '$exists': True
        }
    }
    try:
        cell = await collection.find_one(query)
        return cell
    except PyMongoError as error:
        raise HTTPException(
            detail=f'Error while searching `cell` in {db_collection}: {error}',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_cell_status(
        placement_id: ObjectId,
        row: str,
        col: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        f'_id': placement_id,
        f'rows.{row}.columns.{col}.blocked': {
            '$exists': True,
        }
    }
    projection = {
        f'rows.{row}.columns.{col}.blocked': 1,
        '_id': 0,
    }
    try:
        cell_status = await collection.find_one(query, projection)
        return cell_status
    except PyMongoError as error:
        logger.error(f'Error while searching `cell_status` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while searching `cell_status`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_platform_cell_data(
        platform_id: ObjectId,
        row: str,
        col: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': platform_id,
        f'rows.{row}.columns.{col}': {
            '$exists': True,
        }
    }
    projection = {
        '_id': 1,
        f'rows.{row}.columns.{col}': 1,
    }
    try:
        cell_data = await collection.find_one(query, projection)
        return cell_data
    except PyMongoError as error:
        logger.error(f'Error while searching `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while searching `cell_data`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def place_wheelstack_in_platform(
        placement_id: ObjectId,
        wheelstack_object_id: ObjectId,
        row: str,
        column: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': placement_id,
        f'rows.{row}.columns.{column}.wheelStack': {
            '$exists': True
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{column}.wheelStack': wheelstack_object_id,
            'lastChange': await time_w_timezone(),
        }
    }
    try:
        result = await collection.update_one(query, update)
        return result
    except PyMongoError as error:
        logger.error(f'Error while placing `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while updating `wheelStack',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


# BLock|Unblock|Clear cells
async def block_platform_cell(
        placement_id: ObjectId,
        row: str,
        column: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': placement_id,
        f'rows.{row}.columns.{column}.blocked': {
            '$exists': True
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{column}.blocked': True,
            'lastChange': await time_w_timezone(),
        }
    }
    try:
        result = await collection.update_one(query, update)
        return result
    except PyMongoError as error:
        logger.error(f'Error while blocking `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while blocking `cell_data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def unblock_platform_cell(
        placement_id: ObjectId,
        row: str,
        column: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': placement_id,
        f'rows.{row}.columns.{column}.blocked': {
            '$exists': True
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{column}.blocked': False,
            'lastChange': await time_w_timezone(),
        }
    }
    try:
        result = await collection.update_one(query, update)
        return result
    except PyMongoError as error:
        logger.error(f'Error while unblocking `cell` {row}|{column} in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while unblocking `cell_data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def clear_platform_cell(
        placement_id: ObjectId,
        row: str,
        column: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': placement_id,
        f'rows.{row}.columns.{column}': {
            '$exists': True
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{column}.wheelStack': None,
            f'rows.{row}.columns.{column}.blocked': False,
            f'rows.{row}.columns.{column}.blockedBy': None,
            'lastChange': await time_w_timezone(),
        }
    }
    try:
        result = await collection.update_one(query, update)
        return result
    except PyMongoError as error:
        logger.error(f'Error while clearing `cell` {row}|{column} in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while clearing `cell_data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_update_platform_cell_data(
        platform_id: ObjectId,
        row: str,
        col: str,
        new_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': platform_id,
        f'rows.{row}.columns.{col}': {
            '$exists': True,
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{col}.wheelStack': new_data['wheelStack'],
            f'rows.{row}.columns.{col}.blocked': new_data['blocked'],
            f'rows.{row}.columns.{col}.blockedBy': new_data['blockedBy'],
        }
    }
    if record_change:
        update['$set']['lastChange'] = await time_w_timezone()
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        logger.error(f'Error while updating `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while updating `cell_data`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_platform_last_change_time(
        platform_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': platform_id,
    }
    projection = {
        '_id': 1,
        'lastChange': 1,
    }
    try:
        result = await collection.find_one(query, projection)
        return result
    except PyMongoError as error:
        logger.error(f'Error while searching in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while getting `lastChange` time',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_update_platform_last_change(
        platform_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session=None,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': platform_id,
    }
    update = {
        '$set': {
            'lastChange': await time_w_timezone(),
        }
    }
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        logger.error(f'Error while updating in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while updating `lastChange` time',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
