from loguru import logger
from pymongo.errors import PyMongoError
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from bson import ObjectId
from utility.utilities import get_db_collection, time_w_timezone


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


async def db_find_wheelstack_by_pis(
        original_pis_id: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        wheelstack_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheelstack_collection.find_one({'originalPisId': original_pis_id})
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
):
    try:
        new_data['lastChange'] = await time_w_timezone()
        collection = await get_db_collection(db, db_name, db_collection)
        res = await collection.update_one(
            {'_id': wheelstack_object_id},
            {'$set': new_data},
        )
        return res
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
