import asyncio
from bson import ObjectId
from loguru import logger
from pymongo.errors import PyMongoError
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from utility.utilities import get_db_collection, log_db_record, log_db_error_record


async def order_make_json_friendly(order_data: dict):
    order_data['_id'] = str(order_data['_id'])
    order_data['source']['placementId'] = str(order_data['source']['placementId'])
    order_data['destination']['placementId'] = str(order_data['destination']['placementId'])
    order_data['createdAt'] = order_data['createdAt'].isoformat()
    order_data['lastUpdated'] = order_data['lastUpdated'].isoformat()
    source_wheelstack = order_data['affectedWheelStacks']['source']
    if source_wheelstack:
        order_data['affectedWheelStacks']['source'] = str(source_wheelstack)
    dest_wheelstack = order_data['affectedWheelStacks']['destination']
    if dest_wheelstack:
        order_data['affectedWheelStacks']['destination'] = str(dest_wheelstack)
    source_wheels = order_data['affectedWheels']['source']
    if source_wheels:
        for index, wheel in enumerate(source_wheels):
            source_wheels[index] = str(wheel)
        order_data['affectedWheels']['source'] = source_wheels
    dest_wheels = order_data['affectedWheels']['destination']
    if dest_wheels:
        for index, wheel in enumerate(dest_wheels):
            dest_wheels[index] = str(wheel)
        order_data['affectedWheels']['destination'] = dest_wheels
    if 'completedAt' in order_data:
        order_data['completedAt'] = order_data['completedAt'].isoformat()
    if 'canceledAt' in order_data:
        order_data['canceledAt'] = order_data['canceledAt'].isoformat()
    return order_data


async def db_get_all_orders(
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    orders_collection = await get_db_collection(db, db_name, db_collection)
    query = {}
    try:
        res = await orders_collection.find(query).to_list(length=None)
        return res
    except PyMongoError as e:
        logger.error(f"Error getting Orders: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")


async def db_get_order_by_object_id(
        order_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    order_collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': order_object_id,
    }
    try:
        res = await order_collection.find_one(query)
        return res
    except PyMongoError as e:
        logger.error(f"Error getting Orders: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")


async def db_create_order(
        order_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
):
    collection = await get_db_collection(db, db_name, db_collection)
    max_retries = 3
    for attempt in range(max_retries):
        try:
            res = await collection.insert_one(order_data, session=session)
            return res
        except PyMongoError as error:
            # `TransientTransactionError` <- not critical can be solved with retry.
            if error.has_error_label('TransientTransactionError'):
                logger.warning(f'`TransientTransactionError`: {error}. Attempt {attempt} of {max_retries}')
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
            else:
                logger.error(f"Error creating Order: {error}")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database insertion error")


async def db_update_order(
        order_object_id: ObjectId,
        order_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        collection = await get_db_collection(db, db_name,db_collection)
        result = await collection.update_one(
            {'_id': order_object_id},
            {'$set': order_data}
        )
        return result
    except PyMongoError as e:
        logger.error(f"Error updating Order: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")


async def db_find_order_by_object_id(
        order_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    try:
        orders_collection = await get_db_collection(db, db_name, db_collection)
        res = await orders_collection.find_one({'_id': order_object_id})
        return res
    except PyMongoError as e:
        logger.error(f"Error searching for Order: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def db_delete_order(
        order_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None
):
    try:
        collection = await get_db_collection(db, db_name, db_collection)
        res = await collection.delete_one({'_id': order_object_id}, session=session)
        return res
    except PyMongoError as e:
        logger.error(f"Error deleting Order: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")


async def db_history_get_orders_by_placement(
        placement_id: ObjectId,
        placement_type: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to gather `ordersData` for `placementId` => {placement_id}'
        f' of type {placement_type}' + db_info
    )
    query = {
        '$or': [
            {
                '$and': [
                    {'source.placementType': placement_type},
                    {'source.placementId': placement_id}
                ]
            },
            {
                '$and': [
                    {'destination.placementType': placement_type},
                    {'destination.placementId': placement_id}
                ]
            }
        ]
    }
    try:
        result = await collection.find(query).to_list(length=None)
        logger.info(
            f'Successfully gathered `ordersData` for `placementId` => {placement_id}'
            f' of type {placement_type}' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while gathering `ordersData` for `placementId` => {placement_id}'
            f' of type {placement_type}' + db_info + error_extra
        )
        raise HTTPException(
            detail='Error while gathering data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_orders_by_id_many(
        orders: list[ObjectId],
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClient = None,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to gather `ordersData`' + db_info
    )
    query = {
        '_id': {
            '$in': orders,
        }
    }
    try:
        result = await collection.find(query, session=session).to_list(length=None)
        logger.info(
            f'Successfully gathered `ordersData`' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while gatherng `ordersData`' + db_info + error_extra
        )
        raise HTTPException(
            detail='Error while gathering data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
