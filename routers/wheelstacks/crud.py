from loguru import logger
from bson import ObjectId
from pymongo.errors import PyMongoError
from fastapi import HTTPException, status
from constants import PS_SHIPPED, PS_REJECTED
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from utility.utilities import get_db_collection, time_w_timezone, log_db_record, log_db_error_record


async def wheelstack_make_json_friendly(wheelstack_data):
    wheelstack_data['_id'] = str(wheelstack_data['_id'])
    wheelstack_data['lastOrder'] = str(wheelstack_data['lastOrder'])
    wheelstack_data['placement']['placementId'] = str(wheelstack_data['placement']['placementId'])
    wheelstack_data['createdAt'] = wheelstack_data['createdAt'].isoformat()
    wheelstack_data['lastChange'] = wheelstack_data['lastChange'].isoformat()
    for index, wheel_id in enumerate(wheelstack_data['wheels']):
        wheelstack_data['wheels'][index] = str(wheel_id)
    return wheelstack_data


async def all_make_json_friendly(wheelstacks_data):
    all_data = {}
    for wheelstack in wheelstacks_data:
        wheelstack_id = str(wheelstack['_id'])
        wheelstack['_id'] = wheelstack_id
        wheelstack['lastOrder'] = str(wheelstack['lastOrder'])
        wheelstack['placement']['placementId'] = str(wheelstack['placement']['placementId'])
        wheelstack['createdAt'] = wheelstack['createdAt'].isoformat()
        wheelstack['lastChange'] = wheelstack['lastChange'].isoformat()
        for index, wheel_id in enumerate(wheelstack['wheels']):
            wheelstack['wheels'][index] = str(wheel_id)
        all_data[wheelstack_id] = wheelstack
    return all_data


async def db_find_all_wheelstacks(
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str
):
    try:
        collection = await get_db_collection(db, db_name, db_collection)
        result = await collection.find({}).to_list(length=None)
        return result
    except PyMongoError as e:
        logger.error(f"Error getting all `wheelStack`s: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def db_find_all_pro_rej_available_in_placement(
        batch_number: str,
        placement_id: ObjectId,
        placement_type: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        'batchNumber': batch_number,
        'placement': {
            'type': placement_type,
            'placementId': placement_id,
        },
        'blocked': False,
        'status': placement_type,
    }
    try:
        result = await collection.find(query).to_list(length=None)
        return result
    except PyMongoError as e:
        logger.error(f"Error getting all available in placement `wheelStack`s: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def db_find_all_pro_rej_available(
        batch_number: str,
        available_statuses: list[str],
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        'batchNumber': batch_number,
        'blocked': False,
        'status': {
            '$in': available_statuses,
        }
    }
    try:
        result = await collection.find(query).to_list(length=None)
        return result
    except PyMongoError as e:
        logger.error(f'Error getting all available `wheelstack`s: {e}')
        raise HTTPException(
            detail='Error while searching for all available `wheelstack`s',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_find_wheelstack_by_object_id(
        wheelstack_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        wheelstack_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheelstack_collection.find_one({'_id': wheelstack_object_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error searching a `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def db_insert_wheelstack(
        wheelstack_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        wheelstacks_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheelstacks_collection.insert_one(wheelstack_data)
        return res
    except PyMongoError as e:
        logger.error(f"Error inserting `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database insertion error")


async def db_delete_wheelstack(
        wheelstack_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        wheelstacks_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheelstacks_collection.delete_one({'_id': wheelstack_object_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error deleting `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database deletion error")


async def db_update_wheelstack(
        new_data: dict,
        wheelstack_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True,
):
    collection = await get_db_collection(db, db_name, db_collection)
    if record_change:
        new_data['lastChange'] = await time_w_timezone()
    query = {
        '_id': wheelstack_object_id,
    }
    update = {
        '$set': new_data
    }
    try:
        result = await collection.update_one(query, update, session=session, )
        return result
    except PyMongoError as e:
        logger.error(f"Error updating `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database update error")


async def db_get_wheelstack_last_change(
        wheelstack_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': wheelstack_object_id,
    }
    projection = {
        '_id': 1,
        'lastChange': 1,
    }
    try:
        result = await collection.find_one(query, projection)
        return result
    except PyMongoError as e:
        logger.error(f"Error searching `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def db_history_get_placement_wheelstacks(
        placement_id: ObjectId,
        placement_type: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to gather `wheelstacksData` for `placementId` => {placement_id}'
        f' of type {placement_type}' + db_info
    )
    query = {
        '$and': [
            {'placement.placementId': placement_id},
            {'placement.type': placement_type},
            {'status': {
                '$nin': [PS_SHIPPED, PS_REJECTED]
            },
            },
        ]
    }
    try:
        result = await collection.find(query).to_list(length=None)
        logger.info(
            f'Successfully gathered `wheelstacksData` for `placementId` => {placement_id}'
            f' of type {placement_type}' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while gathering `wheelstacksData` for `placementId` => {placement_id}'
            f' of type {placement_type}' + db_info + error_extra
        )
        raise HTTPException(
            detail='Error while gathering data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
