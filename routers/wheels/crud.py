from loguru import logger
from bson import ObjectId
from pymongo.errors import PyMongoError
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient
from utility.utilities import get_db_collection


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
):
    try:
        wheel_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheel_collection.find_one({'_id': wheel_object_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error finding wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database query error")


async def db_update_wheel(
        wheel_object_id: ObjectId,
        wheel_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        wheel_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheel_collection.update_one(
            {'_id': wheel_object_id},
            {'$set': wheel_data}
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
):
    try:
        wheel_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheel_collection.insert_one(wheel_data)
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
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        wheel_collection = await get_db_collection(db, db_name, db_collection)
        res = await wheel_collection.find({}).to_list(length=None)
        return res
    except PyMongoError as error:
        logger.error(f"Error getting all wheels: {error}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database insertion error")
