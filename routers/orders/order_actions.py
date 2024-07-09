from loguru import logger
from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import HTTPException, status
from bson import ObjectId
from routers.orders.crud import db_create_order, db_delete_order
from routers.wheelstacks.crud import db_update_wheelstack
from routers.grid.crud import put_wheelstack_in_grid
from routers.base_platform.crud import put_wheelstack_in_platform
from constants import *
from utility.db_utilities import get_preset, time_w_timezone


# Completions
async def orders_complete_whole_wheelstack_move(db: AsyncIOMotorClient, order_data: dict):
    # Chaos which need's to be completely changed after a first test version.
    # print(order_data)
    affected_wheelstack_object_id = order_data['affectedWheelStacks']['source']
    destination = order_data['destination']['type']
    destination_row, destination_col = order_data['destination']['identifier'].split(',')
    # Place wheelstack
    if destination == CLN_GRID:
        result = await put_wheelstack_in_grid(
            db, destination_row, destination_col, affected_wheelstack_object_id,
            db_name=DB_PMK_NAME, db_collection=CLN_GRID,
        )
        # print('gridPlacementResult', result)
    source = order_data['source']['type']
    source_row, source_col = order_data['source']['identifier'].split(',')
    # Delete wheelstack
    if source == CLN_BASE_PLACEMENT:
        result = await put_wheelstack_in_platform(
            db, source_row, source_col,
            None, db_name=DB_PMK_NAME, db_collection=CLN_BASE_PLACEMENT,
        )
        # print('basePlatformResult', result)
    elif source == CLN_GRID:
        result = await put_wheelstack_in_grid(
            db, source_row, source_col,
            None, db_name=DB_PMK_NAME, db_collection=CLN_GRID,
        )
    # Unblock everything
    await orders_cancel_unblock_wheelstack(
        db, affected_wheelstack_object_id,
        db_name=DB_PMK_NAME, db_collection=CLN_WHEEL_STACKS,
    )
    destination = order_data['destination']['type']
    destination_preset = await get_preset(destination)
    destination_row, destination_col = order_data['destination']['identifier'].split(',')
    await orders_unblock_placement(
        db, destination_row, destination_col,
        destination, destination_preset, DB_PMK_NAME,
    )
    # Delete completed
    await db_delete_order(
        db, order_data['_id'],
        db_collection=CLN_ACTIVE_ORDERS, db_name=DB_PMK_NAME,
    )
    # Relocate it to completed
    order_data['status'] = ORDER_STATUS_COMPLETED
    completion_time = await time_w_timezone()
    order_data['completedAt'] = completion_time
    order_data['lastUpdated'] = completion_time
    return await db_create_order(
        db, order_data,
        db_collection=CLN_COMPLETED_ORDERS, db_name=DB_PMK_NAME,
    )


async def orders_get_placement_data(
        db: AsyncIOMotorClient,
        row: str,
        column: str,
        db_collection: str,
        preset: str,
        db_name: str = 'pmkScreen',
):
    try:
        collection = db[db_name][db_collection]
        projection = {f'rows.{row}.columns.{column}': 1}
        resp = await collection.find_one({'preset': preset}, projection)
        placement_data = resp['rows'][row]['columns'][column]
        return placement_data
    except PyMongoError as e:
        logger.error(f"Error searching source placement: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def orders_block_placement(
        db: AsyncIOMotorClient,
        row: str,
        column: str,
        db_collection: str,
        preset: str,
        db_name: str = 'pmkScreen'
):
    try:
        collection = db[db_name][db_collection]
        projection = {f'rows.{row}.columns.{column}.blocked': True}
        resp = await collection.update_one({'preset': preset}, {'$set': projection})
        return resp
    except PyMongoError as e:
        logger.error(f"Error blocking placement: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def orders_unblock_placement(
        db: AsyncIOMotorClient,
        row: str,
        column: str,
        db_collection: str,
        preset: str,
        db_name: str = 'pmkScreen'
):
    try:
        collection = db[db_name][db_collection]
        projection = {f'rows.{row}.columns.{column}.blocked': False}
        resp = await collection.update_one({'preset': preset}, {'$set': projection})
        return resp
    except PyMongoError as e:
        logger.error(f"Error unblocking wheelstack: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def orders_block_wheelstack(
        db: AsyncIOMotorClient,
        wheel_stack_object_id: ObjectId,
        order_object_id: ObjectId,
        db_collection: str = 'wheelStacks',
        db_name: str = 'pmkScreen'
):
    try:
        collection = db[db_name][db_collection]
        resp = await collection.update_one(
            {'_id': wheel_stack_object_id},
            {'$set': {
                'blocked': True,
                'lastOrder': order_object_id,
                'status': 'orderQue',
            }
            }
        )
        return resp
    except PyMongoError as e:
        logger.error(f"Error blocking wheelstack: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database search error")


async def orders_cancel_unblock_wheelstack(
        db: AsyncIOMotorClient,
        wheel_stack_object_id: ObjectId,
        db_collection: str = 'wheelStacks',
        db_name: str = 'pmkScreen',
):
    try:
        collection = db[db_name][db_collection]
        resp = await collection.update_one(
            {'_id': wheel_stack_object_id},
            {'$set': {
                'blocked': False,
                'status': 'inActive',
            }
            }
        )
        return resp
    except PyMongoError as e:
        logger.error(f"Error searching source placement: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
