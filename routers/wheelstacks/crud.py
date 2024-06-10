from loguru import logger
from pymongo.errors import PyMongoError
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient
from .models.models import UpdateWheelStackRequest
from bson import ObjectId
from datetime import datetime
from ..wheels.crud import db_find_wheel


async def make_json_friendly(unfriendly_dict: dict) -> dict:
    for key, value in unfriendly_dict.items():
        if isinstance(value, ObjectId):
            unfriendly_dict[key] = str(unfriendly_dict[key])
        elif isinstance(value, datetime):
            unfriendly_dict[key] = unfriendly_dict[key].isoformat()
    return unfriendly_dict


async def db_find_wheelstack(
        db: AsyncIOMotorClient,
        wheelstack_object_id: ObjectId,
        db_name: str = 'pmkScreen',
        db_collection: str = 'wheelStacks',
):
    try:
        wheelstack_collection = db[db_name][db_collection]
        res = await wheelstack_collection.find_one({'_id': wheelstack_object_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error searching a `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def db_find_wheelstack_by_pis(
        db: AsyncIOMotorClient,
        original_pis_id: str,
        db_name: str = 'pmkScreen',
        db_collection: str = 'wheelStacks',
):
    try:
        wheelstack_collection = db[db_name][db_collection]
        res = await wheelstack_collection.find_one({'originalPisId': original_pis_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error searching a `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def db_insert_wheelstack(
        db: AsyncIOMotorClient,
        wheel_stack_data,
        db_name: str = 'pmkScreen',
        db_collection: str = 'wheelStacks',
):
    try:
        wheelstacks_collection = db[db_name][db_collection]
        wheel_stack_data['createdAt'] = datetime.now()
        wheel_stack_data['lastChange'] = datetime.now()
        res = await wheelstacks_collection.insert_one(wheel_stack_data)
        return res
    except PyMongoError as e:
        logger.error(f"Error inserting `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database insertion error")


async def db_delete_wheelstack(
        db: AsyncIOMotorClient,
        wheelstack_object_id: ObjectId,
        db_name: str = 'pmkScreen',
        db_collection: str = 'wheelStacks',
):
    try:
        wheelstacks_collection = db[db_name][db_collection]
        res = await wheelstacks_collection.delete_one({'_id': wheelstack_object_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error deleting `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database deletion error")


async def db_update_wheelstack(
        db: AsyncIOMotorClient,
        wheelstack_object_id: ObjectId,
        wheel_stack_data: dict,
        db_name: str = 'pmkScreen',
        db_collection: str = 'wheelStacks',
):
    # Add checks for col_row placements, we can have them our of bound.
    # So, if it's GRID we need to check GRID row/col for existence, same for BASE.
    try:
        # Replace all the checks somewhere else. CRUD should just do simple actions, checks in diff.
        wheelstacks_collection = db[db_name][db_collection]
        new_data = {key: value for key, value in wheel_stack_data.items() if value is not None}
        new_data['_id'] = wheelstack_object_id
        for wheel_id in new_data['wheels']:
            if await db_find_wheel(db, wheel_id=wheel_id) is None:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                    detail=f'{wheel_id} ID doesnt exist in DB')
        if not new_data['wheels']:
            new_data.pop('wheels')
        new_data['lastChange'] = datetime.now()
        res = await wheelstacks_collection.update_one({'_id': wheelstack_object_id}, {'$set': new_data})
        return res
    except PyMongoError as e:
        logger.error(f"Error updating `wheelStack`: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database update error")