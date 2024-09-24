from bson import ObjectId
from loguru import logger
from datetime import datetime
from fastapi import HTTPException, status
from utility.utilities import time_w_timezone
from motor.motor_asyncio import AsyncIOMotorClient
from routers.wheels.crud import db_find_wheel_by_object_id
from routers.batch_numbers.crud import db_find_batch_number
from routers.orders.crud import db_history_get_orders_by_placement
from routers.history.crud import db_history_get_placement_data, db_history_create_record
from routers.wheelstacks.crud import db_history_get_placement_wheelstacks, db_find_wheelstack_by_object_id
from constants import (
    PLACEMENT_COLLECTIONS,
    DB_PMK_NAME,
    CLN_WHEELSTACKS,
    CLN_ACTIVE_ORDERS,
    CLN_PLACEMENT_HISTORY,
    CLN_WHEELS,
    CLN_BATCH_NUMBERS,
)


async def gather_wheels_data(wheelstacks_data: dict, db: AsyncIOMotorClient) -> dict:
    # { wheel_object_id: { wheel_data } }
    wheels_data: dict[str, dict] = {}
    for wheelstack_id, record in wheelstacks_data.items():
        for wheel_id in record['wheels']:
            if wheel_id in wheels_data:
                continue
            wheel_data: dict = await db_find_wheel_by_object_id(
                wheel_id, db, DB_PMK_NAME, CLN_WHEELS,
            )
            if wheel_data is None:
                logger.warning(
                    f'Not existing wheel used in the wheelstack record with `ObjectId` => {record['_id']}'
                )
                continue
            wheels_data[str(wheel_id)] = wheel_data
    return wheels_data


async def gather_batches_data(wheelstacks_data: dict, db: AsyncIOMotorClient) -> dict:
    # { batch_number: { batch_data } }
    batches_data: dict[str, dict] = {}
    for wheelstack_id, record in wheelstacks_data.items():
        batch_number: str = record['batchNumber']
        if batch_number in batches_data:
            continue
        batch_data: dict = await db_find_batch_number(
            batch_number, db, DB_PMK_NAME, CLN_BATCH_NUMBERS
        )
        if batch_data is None:
            logger.warning(
                f'Not existing `batchNumber` in the wheelstack record with `ObjectId`=> {record['_id']}'
            )
            continue
        batches_data[batch_number] = batch_data
    return batches_data


async def add_order_wheelstacks(wheelstacks_data, orders_data, db: AsyncIOMotorClient) -> None:
    for order in orders_data:
        source_wheelstack_object_id: ObjectId = order['affectedWheelStacks']['source']
        source_string_object_id: str = str(source_wheelstack_object_id)
        if source_wheelstack_object_id and source_string_object_id not in wheelstacks_data:
            source_wheelstack = await db_find_wheelstack_by_object_id(
                source_wheelstack_object_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
            )
            if source_wheelstack is None:
                logger.warning(
                    f'Corrupted `order` uses non existing `wheelstack` with `ObjectId` => {source_string_object_id}'
                )
            else:
                wheelstacks_data[source_string_object_id] = source_wheelstack
        dest_wheelstack_object_id: ObjectId = order['affectedWheelStacks']['destination']
        dest_string_object_id: str = str(dest_wheelstack_object_id)
        if dest_wheelstack_object_id and dest_string_object_id not in wheelstacks_data:
            dest_wheelstack = await db_find_wheelstack_by_object_id(
                dest_wheelstack_object_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
            )
            if dest_wheelstack is None:
                logger.warning(
                    f'Corrupted `order` uses non existing `wheelstack` with `ObjectId` => {dest_string_object_id}'
                )
            else:
                wheelstacks_data[dest_string_object_id] = dest_wheelstack


async def gather_placement_history_data(
        placement_id: ObjectId,
        placement_type: str,
        db: AsyncIOMotorClient,
) -> dict:
    placement_collection: str = PLACEMENT_COLLECTIONS[placement_type]
    history_record_date: datetime = await time_w_timezone()
    placement_data = await db_history_get_placement_data(placement_id, db, DB_PMK_NAME, placement_collection)
    if placement_data is None:
        logger.warning(
            f'Attempt to use NonExisting placement with `placementId` => {placement_id} and type => {placement_type}'
        )
        raise HTTPException(
            detail='Placement NotFound',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    wheelstacks_data = await db_history_get_placement_wheelstacks(
        placement_id, placement_type, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    wheelstacks_data = {
        str(wheelstack_data['_id']): wheelstack_data for wheelstack_data in wheelstacks_data
    }
    # Only care about `activeOrders` <- we actually need to combine all orders in one, but later.
    orders_data = await db_history_get_orders_by_placement(
        placement_id, placement_type, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS
    )
    # We can have order `basePlatform` -> `grid`.
    # `wheelstack` is not present in `grid` but it will, and we need this data.
    await add_order_wheelstacks(wheelstacks_data, orders_data, db)
    wheels_data = await gather_wheels_data(wheelstacks_data, db)
    batches_data = await gather_batches_data(wheelstacks_data, db)
    # Converting to dictionary for better usage.
    orders_data = {
        str(order['_id']): order for order in orders_data
    }
    history_record_data: dict = {
        'createdAt': history_record_date,
        'placementType': placement_type,
        'placementData': placement_data,
        'wheelstacksData': wheelstacks_data,
        'placementOrders': orders_data,
        'wheelsData': wheels_data,
        'batchesData': batches_data,
    }
    return history_record_data


async def background_history_record(
        placement_id: ObjectId,
        placement_type: str,
        db: AsyncIOMotorClient,
) -> None:
    logger.info(
        f'Started backgroundTask of creating a history record for `placement` => {placement_id}'
        f' of type {placement_type}'
    )
    placement_data = await gather_placement_history_data(
        placement_id, placement_type, db
    )
    history_record = await db_history_create_record(
        placement_data, db, DB_PMK_NAME, CLN_PLACEMENT_HISTORY
    )
    logger.info(
        f'End of creating a history record for `placement`  => {placement_id}'
        f' of type {placement_type} | History record `ObjectId` => {history_record.inserted_id}'
    )
