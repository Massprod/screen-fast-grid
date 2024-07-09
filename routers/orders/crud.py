from loguru import logger
from pymongo.errors import PyMongoError
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone
from routers.wheelstacks.crud import db_find_wheelstack, db_find_wheelstack_id_by_placement


async def get_all_orders_make_json_friendly(order_data: dict):
    order_data['_id'] = str(order_data['_id'])
    for wheel_stack in order_data['affectedWheelStacks']:
        order_data['affectedWheelStacks'][wheel_stack] = str(order_data['affectedWheelStacks'][wheel_stack])
    order_data['createdAt'] = order_data['createdAt'].isoformat()
    order_data['lastUpdated'] = order_data['lastUpdated'].isoformat()
    if 'completedAt' in order_data:
        order_data['completedAt'] = order_data['completedAt'].isoformat()
    if 'canceledAt' in order_data:
        order_data['canceledAt'] = order_data['canceledAt'].isoformat()
    return order_data


async def db_get_all_orders(
        db: AsyncIOMotorClient,
        db_name: str = 'pmkScreen',
        db_collection: str = 'activeOrders',
):
    try:
        orders_collection = db[db_name][db_collection]
        res = orders_collection.find({})
        return res
    except PyMongoError as e:
        logger.error(f"Error getting Orders: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")


async def db_create_order(
        db: AsyncIOMotorClient,
        order_data,
        db_name: str = 'pmkScreen',
        db_collection: str = 'activeOrders',
):
    try:
        orders_collection = db[db_name][db_collection]
        res = await orders_collection.insert_one(order_data)
        return res
    except PyMongoError as e:
        logger.error(f"Error creating Order: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database insertion error")


async def db_update_order(
        db: AsyncIOMotorClient,
        order_object_id,
        order_data,
        db_name: str = 'pmkScreen',
        db_collection: str = 'activeOrders',
):
    try:
        collection = db[db_name][db_collection]
        result = await collection.update_one(
            {'_id': order_object_id},
            {'$set': order_data}
        )
        return result
    except PyMongoError as e:
        logger.error(f"Error updating Order: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")


async def db_find_order(
        db: AsyncIOMotorClient,
        order_object_id,
        db_name: str = 'pmkScreen',
        db_collection: str = 'activeOrders',
):
    try:
        orders_collection = db[db_name][db_collection]
        res = await orders_collection.find_one({'_id': order_object_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error searching for Order: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def db_delete_order(
        db: AsyncIOMotorClient,
        order_object_id,
        db_collection: str,
        db_name: str = 'pmkScreen',
):
    try:
        collection = db[db_name][db_collection]
        res = await collection.delete_one({'_id': order_object_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error deleting Order: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
