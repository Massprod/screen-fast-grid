from loguru import logger
from pymongo.errors import PyMongoError
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient


async def make_json_friendly(unfriendly_dict: dict) -> dict:
    if '_id' in unfriendly_dict:
        unfriendly_dict['_id'] = str(unfriendly_dict['_id'])
    if 'receiptDate' in unfriendly_dict:
        unfriendly_dict['receiptDate'] = unfriendly_dict['receiptDate'].isoformat()
    return unfriendly_dict


async def db_insert_wheel(
        db: AsyncIOMotorClient,
        wheel_data,
        db_name: str = 'pmkScreen',
        db_collection: str = 'wheels',
):
    try:
        wheel_collection = db[db_name][db_collection]
        res = await wheel_collection.insert_one(wheel_data)
        return res
    except PyMongoError as e:
        logger.error(f"Error inserting wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database insertion error")


async def db_find_wheel(
        db: AsyncIOMotorClient,
        wheel_id: str,
        db_name: str = 'pmkScreen',
        db_collection: str = 'wheels'
):
    try:
        wheel_collection = db[db_name][db_collection]
        res = await wheel_collection.find_one({'wheelId': wheel_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error finding wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database query error")


async def db_update_wheel(
        db: AsyncIOMotorClient,
        wheel_data,
        db_name: str = 'pmkScreen',
        db_collection: str = 'wheels',
):
    try:
        wheel_collection = db[db_name][db_collection]
        wheel_id = wheel_data['wheelId']
        res = await wheel_collection.update_one({'wheelId': wheel_id}, {'$set': wheel_data})
        return res
    except PyMongoError as e:
        logger.error(f"Error updating wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database update error")


async def db_delete_wheel(
        db: AsyncIOMotorClient,
        wheel_id,
        db_name: str = 'pmkScreen',
        db_collection: str = 'wheels',
):
    try:
        wheel_collection = db[db_name][db_collection]
        res = await wheel_collection.delete_one({'wheelId': wheel_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error deleting wheel: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database deletion error")
