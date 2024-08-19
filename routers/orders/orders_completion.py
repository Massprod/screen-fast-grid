from loguru import logger
from bson import ObjectId
from fastapi import HTTPException, status

from routers.storages.crud import db_get_storage_by_object_id, db_storage_place_wheelstack, \
    db_storage_delete_placed_wheelstack
from utility.utilities import time_w_timezone, get_object_id
from motor.motor_asyncio import AsyncIOMotorClient
from routers.wheels.crud import (db_update_wheel_status,
                                 db_update_wheel_position,
                                 db_find_wheel_by_object_id,
                                 db_update_wheel)
from routers.orders.crud import db_delete_order, db_create_order
from routers.grid.crud import (db_get_grid_cell_data,
                               db_update_grid_cell_data,
                               db_get_grid_extra_cell_data,
                               db_delete_extra_cell_order)
from routers.base_platform.crud import db_get_platform_cell_data, db_update_platform_cell_data
from routers.wheelstacks.crud import (
    db_find_wheelstack_by_object_id,
    db_update_wheelstack
)
from constants import (PRES_TYPE_GRID, PRES_TYPE_PLATFORM,
                       DB_PMK_NAME, CLN_GRID, CLN_BASE_PLATFORM,
                       CLN_WHEELSTACKS, CLN_WHEELS, CLN_ACTIVE_ORDERS,
                       CLN_COMPLETED_ORDERS, ORDER_STATUS_COMPLETED,
                       PS_REJECTED, PS_GRID, PS_SHIPPED, PS_LABORATORY, CLN_STORAGES, PS_STORAGE)


async def orders_complete_move_wholestack(order_data: dict, db: AsyncIOMotorClient) -> ObjectId:
    # Source can be a `grid` and `basePlatform`.
    # -1- Check if source cell exists and correct `wheelStack` on it
    source_type: str = order_data['source']['placementType']
    source_id: ObjectId = order_data['source']['placementId']
    source_row: str = order_data['source']['rowPlacement']
    source_col: str = order_data['source']['columnPlacement']
    source_cell_data = None
    if PRES_TYPE_GRID == source_type:
        source_cell_data = await db_get_grid_cell_data(
            source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID
        )
    elif PRES_TYPE_PLATFORM == source_type:
        source_cell_data = await db_get_platform_cell_data(
            source_id, source_row, source_col, db, DB_PMK_NAME, CLN_BASE_PLATFORM
        )
    if source_cell_data is None:
        logger.error(f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                     f'But given order = {order_data['_id']} marks it as source cell.')
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                   f'But given order = {order_data['_id']} marks it as source cell.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    if source_cell_data['blockedBy'] != order_data['_id']:
        logger.error(f'Corrupted `order` = {order_data['_id']},'
                     f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                     f'But different order is blocking it {source_cell_data['blockedBy']}')
        raise HTTPException(
            detail=f'Corrupted `order` = {order_data['_id']},'
                   f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                   f'But different order is blocking it {source_cell_data['blockedBy']}',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    # -2- <- Check source `wheelStack` it should exist
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_cell_data['wheelStack'], db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        logger.error(
            f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
            f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.'
        )
        raise HTTPException(
            detail=f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
                   f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    # -3- <- Check destination it should exist.
    dest_type = order_data['destination']['placementType']
    dest_id = order_data['destination']['placementId']
    dest_row = order_data['destination']['rowPlacement']
    dest_col = order_data['destination']['columnPlacement']
    destination_cell_data = await db_get_grid_cell_data(
        dest_id, dest_row, dest_col, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        logger.error(
            f'Corrupted `grid` = {dest_id}  cell {dest_row}|{dest_col}.'
            f'Used in order = {order_data['_id']}, but it doesnt exist.'
        )
        raise HTTPException(
            detail=f'Corrupted extra element cell {dest_row}|{dest_col}.'
                   f'Used in order = {order_data['_id']}, but it doesnt exist.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    destination_cell_data = destination_cell_data['rows'][dest_row]['columns'][dest_col]
    if destination_cell_data['wheelStack'] is not None:
        logger.error(
            f'Corrupted `grid` = {dest_id} cell {dest_row}|{dest_col}.'
            f'Used in order = {order_data['_id']} as destination, but its already taken'
        )
        raise HTTPException(
            detail=f'Corrupted `grid` = {dest_id} cell {dest_row}|{dest_col}.'
                   f'Used in order = {order_data['_id']} as destination, but its already taken',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    # -4- <- Clear source cell
    source_cell_data['blocked'] = False
    source_cell_data['blockedBy'] = None
    source_cell_data['wheelStack'] = None
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            if PRES_TYPE_GRID == source_type:
                await db_update_grid_cell_data(
                    source_id, source_row, source_col, source_cell_data,
                    db, DB_PMK_NAME, CLN_GRID, session, False
                )
            elif PRES_TYPE_PLATFORM == source_type:
                await db_update_platform_cell_data(
                    source_id, source_row, source_col, source_cell_data,
                    db, DB_PMK_NAME, CLN_BASE_PLATFORM, session
                )
            # -5- <- Transfer `wheelStack` on destination cell
            destination_cell_data['blocked'] = False
            destination_cell_data['blockedBy'] = None
            destination_cell_data['wheelStack'] = source_wheelstack_data['_id']
            await db_update_grid_cell_data(
                dest_id, dest_row, dest_col, destination_cell_data,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            # -6- <- Unblock `wheelStack`
            source_wheelstack_data['placement']['type'] = dest_type
            source_wheelstack_data['placement']['placementId'] = dest_id
            source_wheelstack_data['rowPlacement'] = dest_row
            source_wheelstack_data['colPlacement'] = dest_col
            source_wheelstack_data['lastOrder'] = order_data['_id']
            source_wheelstack_data['blocked'] = False
            # `moveWholeStack` only for `grid` -> `grid` or `basePlatform` -> `grid`.
            source_wheelstack_data['status'] = PS_GRID
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'],
                db, DB_PMK_NAME, CLN_WHEELSTACKS, session, True
            )
            # -7- Update status of every affected wheel
            for wheel in order_data['affectedWheels']['source']:
                await db_update_wheel_status(
                    wheel, PS_GRID, db, DB_PMK_NAME, CLN_WHEELS, session
                )
            # -8- Delete order from `activeOrders`
            await db_delete_order(
                order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            # -9- Add order into `completedOrders`
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            completed_order = await db_create_order(
                order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
            )
            return completed_order.inserted_id


async def orders_complete_move_to_processing(order_data: dict, db: AsyncIOMotorClient) -> ObjectId:
    # Source can only be a `grid`
    # -1- Check if source cell exists and correct `wheelStack` on it
    source_type: str = order_data['source']['placementType']
    source_id: ObjectId = order_data['source']['placementId']
    source_row: str = order_data['source']['rowPlacement']
    source_col: str = order_data['source']['columnPlacement']
    source_cell_data = await db_get_grid_cell_data(
        source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID
    )
    if source_cell_data is None:
        logger.error(f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                     f'But given order = {order_data['_id']} marks it as source cell.')
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                   f'But given order = {order_data['_id']} marks it as source cell.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    if source_cell_data['blockedBy'] != order_data['_id']:
        logger.error(f'Corrupted `order` = {order_data['_id']},'
                     f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                     f'But different order is blocking it {source_cell_data['blockedBy']}')
        raise HTTPException(
            detail=f'Corrupted `order` = {order_data['_id']},'
                   f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                   f'But different order is blocking it {source_cell_data['blockedBy']}',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    # -2- <- Check source `wheelStack` it should exist
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_cell_data['wheelStack'], db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        logger.error(
            f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
            f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.'
        )
        raise HTTPException(
            detail=f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
                   f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    # -3- <- Check destination it should exist and have this order
    dest_type = order_data['destination']['placementType']
    dest_id = order_data['destination']['placementId']
    dest_element_row = order_data['destination']['rowPlacement']
    dest_element_name = order_data['destination']['columnPlacement']
    destination_cell_data = await db_get_grid_extra_cell_data(
        dest_id, dest_element_name, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        logger.error(
            f'Corrupted `grid` = {dest_id}  cell {dest_element_row}|{dest_element_name}.'
            f'Used in order = {order_data['_id']}, but it doesnt exist.'
        )
        raise HTTPException(
            detail=f'Corrupted extra element cell {dest_element_row}|{dest_element_name}.'
                   f'Used in order = {order_data['_id']}, but it doesnt exist.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    destination_cell_data = destination_cell_data['extra'][dest_element_name]
    if str(order_data['_id']) not in destination_cell_data['orders']:
        logger.error(
            f'Corrupted order = {order_data['_id']} marked as placed in'
            f'grid = {dest_id} cell {dest_element_row}|{dest_element_name}.'
            f'But it doesnt exist in this cell orders.'
        )
        raise HTTPException(
            detail=f'Corrupted order = {order_data['_id']} marked as placed in'
                   f'grid = {dest_id} cell {dest_element_row}|{dest_element_name}.'
                   f'But it doesnt exist in this cell orders.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    # -4- <- Clear source cell
    source_cell_data['wheelStack'] = None
    source_cell_data['blockedBy'] = None
    source_cell_data['blocked'] = False
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            await db_update_grid_cell_data(
                source_id, source_row, source_col, source_cell_data,
                db, DB_PMK_NAME, CLN_GRID, session, False
            )
            # -5- <- Delete order from destination element
            await db_delete_extra_cell_order(
                dest_id, dest_element_name, order_data['_id'],
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            # -6- <- Update `wheelStack` record
            source_wheelstack_data['placement']['type'] = dest_type
            source_wheelstack_data['placement']['placementId'] = dest_id
            source_wheelstack_data['rowPlacement'] = dest_element_row
            source_wheelstack_data['colPlacement'] = dest_element_name
            source_wheelstack_data['lastOrder'] = order_data['_id']
            source_wheelstack_data['status'] = PS_SHIPPED
            source_wheelstack_data['blocked'] = True
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            # -7- <- Update status for each wheel
            for wheel in order_data['affectedWheels']['source']:
                await db_update_wheel_status(
                    wheel, PS_SHIPPED, db, DB_PMK_NAME, CLN_WHEELS, session
                )
            # -8- Delete order from `activeOrders`
            await db_delete_order(
                order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            # -9- Add order into `completedOrders`
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            completed_order = await db_create_order(
                order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
            )
            return completed_order.inserted_id


async def orders_complete_move_to_rejected(order_data: dict, db: AsyncIOMotorClient) -> ObjectId:
    # Source can only be a `grid`
    # -1- Check if source cell exists and correct `wheelStack` on it
    source_type: str = order_data['source']['placementType']
    source_id: ObjectId = order_data['source']['placementId']
    source_row: str = order_data['source']['rowPlacement']
    source_col: str = order_data['source']['columnPlacement']
    source_cell_data = await db_get_grid_cell_data(
        source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID
    )
    if source_cell_data is None:
        logger.error(f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                     f'But given order = {order_data['_id']} marks it as source cell.')
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                   f'But given order = {order_data['_id']} marks it as source cell.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    if source_cell_data['blockedBy'] != order_data['_id']:
        logger.error(f'Corrupted `order` = {order_data['_id']},'
                     f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                     f'But different order is blocking it {source_cell_data['blockedBy']}')
        raise HTTPException(
            detail=f'Corrupted `order` = {order_data['_id']},'
                   f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                   f'But different order is blocking it {source_cell_data['blockedBy']}',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    # -2- <- Check source `wheelStack` it should exist
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_cell_data['wheelStack'], db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        logger.error(
            f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
            f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.'
        )
        raise HTTPException(
            detail=f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
                   f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    # -3- <- Check destination it should exist and have this order
    dest_type = order_data['destination']['placementType']
    dest_id = order_data['destination']['placementId']
    dest_element_row = order_data['destination']['rowPlacement']
    dest_element_name = order_data['destination']['columnPlacement']
    destination_cell_data = await db_get_grid_extra_cell_data(
        dest_id, dest_element_name, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        logger.error(
            f'Corrupted `grid` = {dest_id}  cell {dest_element_row}|{dest_element_name}.'
            f'Used in order = {order_data['_id']}, but it doesnt exist.'
        )
        raise HTTPException(
            detail=f'Corrupted extra element cell {dest_element_row}|{dest_element_name}.'
                   f'Used in order = {order_data['_id']}, but it doesnt exist.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    destination_cell_data = destination_cell_data['extra'][dest_element_name]
    if str(order_data['_id']) not in destination_cell_data['orders']:
        logger.error(
            f'Corrupted order = {order_data['_id']} marked as placed in'
            f'grid = {dest_id} cell {dest_element_row}|{dest_element_name}.'
            f'But it doesnt exist in this cell orders.'
        )
        raise HTTPException(
            detail=f'Corrupted order = {order_data['_id']} marked as placed in'
                   f'grid = {dest_id} cell {dest_element_row}|{dest_element_name}.'
                   f'But it doesnt exist in this cell orders.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    # -4- <- Clear source cell
    source_cell_data['wheelStack'] = None
    source_cell_data['blockedBy'] = None
    source_cell_data['blocked'] = False
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            await db_update_grid_cell_data(
                source_id, source_row, source_col, source_cell_data,
                db, DB_PMK_NAME, CLN_GRID, session, False
            )
            # -5- <- Delete order from destination element
            await db_delete_extra_cell_order(
                dest_id, dest_element_name, order_data['_id'],
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            # -6- <- Update `wheelStack` record
            source_wheelstack_data['placement']['type'] = dest_type
            source_wheelstack_data['placement']['placementId'] = dest_id
            source_wheelstack_data['rowPlacement'] = dest_element_row
            source_wheelstack_data['colPlacement'] = dest_element_name
            source_wheelstack_data['lastOrder'] = order_data['_id']
            source_wheelstack_data['status'] = PS_REJECTED
            source_wheelstack_data['blocked'] = True
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            # -7- <- Update status for each wheel
            for wheel in order_data['affectedWheels']['source']:
                await db_update_wheel_status(
                    wheel, PS_REJECTED, db, DB_PMK_NAME, CLN_WHEELS, session
                )
            # -8- Delete order from `activeOrders`
            await db_delete_order(
                order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            # -9- Add order into `completedOrders`
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            completed_order = await db_create_order(
                order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
            )
            return completed_order.inserted_id


async def orders_complete_move_to_laboratory(order_data: dict, db: AsyncIOMotorClient) -> ObjectId:
    # Source can only be a `grid`
    # -1- Check if source cell exists and correct `wheelStack` on it
    source_type: str = order_data['source']['placementType']
    source_id: ObjectId = order_data['source']['placementId']
    source_row: str = order_data['source']['rowPlacement']
    source_col: str = order_data['source']['columnPlacement']
    source_cell_data = await db_get_grid_cell_data(
        source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID
    )
    if source_cell_data is None:
        logger.error(f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                     f'But given order = {order_data['_id']} marks it as source cell.')
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                   f'But given order = {order_data['_id']} marks it as source cell.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    if source_cell_data['blockedBy'] != order_data['_id']:
        logger.error(f'Corrupted `order` = {order_data['_id']},'
                     f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                     f'But different order is blocking it {source_cell_data['blockedBy']}')
        raise HTTPException(
            detail=f'Corrupted `order` = {order_data['_id']},'
                   f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                   f'But different order is blocking it {source_cell_data['blockedBy']}',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    # -2- <- Check source `wheelStack` it should exist
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_cell_data['wheelStack'], db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        logger.error(
            f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
            f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.'
        )
        raise HTTPException(
            detail=f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
                   f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    # -3- <- Check destination it should exist and have this order
    dest_id = order_data['destination']['placementId']
    dest_element_row = order_data['destination']['rowPlacement']
    dest_element_name = order_data['destination']['columnPlacement']
    destination_cell_data = await db_get_grid_extra_cell_data(
        dest_id, dest_element_name, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        logger.error(
            f'Corrupted `grid` = {dest_id}  cell {dest_element_row}|{dest_element_name}.'
            f'Used in order = {order_data['_id']}, but it doesnt exist.'
        )
        raise HTTPException(
            detail=f'Corrupted extra element cell {dest_element_row}|{dest_element_name}.'
                   f'Used in order = {order_data['_id']}, but it doesnt exist.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    destination_cell_data = destination_cell_data['extra'][dest_element_name]
    if str(order_data['_id']) not in destination_cell_data['orders']:
        logger.error(
            f'Corrupted order = {order_data['_id']} marked as placed in'
            f'grid = {dest_id} cell {dest_element_row}|{dest_element_name}.'
            f'But it doesnt exist in this cell orders.'
        )
        raise HTTPException(
            detail=f'Corrupted order = {order_data['_id']} marked as placed in'
                   f'grid = {dest_id} cell {dest_element_row}|{dest_element_name}.'
                   f'But it doesnt exist in this cell orders.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    lab_wheel = order_data['affectedWheels']['source'][0]
    try:
        source_wheelstack_data['wheels'].remove(lab_wheel)
    except ValueError as e:
        logger.error(
            f'Corrupted order = {order_data['_id']}, marking `wheel` for deletion = {lab_wheel}.'
            f' But it doesnt present in the affected wheelStack = {source_wheelstack_data['_id']}. Error: {e}'
        )
        raise HTTPException(
            detail=f'Corrupted order = {order_data['_id']}, marking `wheel` for deletion = {lab_wheel}.'
                   f'But id doesnt presented in the affected wheelStack = {source_wheelstack_data['_id']}.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    # -4- <- Unblock source cell
    source_cell_data['blockedBy'] = None
    source_cell_data['blocked'] = False
    # We shouldn't leave empty `wheelStack` placed in the grid.
    # But we're still leaving it in DB, but the cell should be cleared.
    if 0 == len(source_wheelstack_data['wheels']):
        source_cell_data['wheelStack'] = None
        source_wheelstack_data['status'] = PS_SHIPPED
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            await db_update_grid_cell_data(
                source_id, source_row, source_col, source_cell_data, db,
                DB_PMK_NAME, CLN_GRID, session, False
            )
            # -5- <- Delete order from destination element
            await db_delete_extra_cell_order(
                dest_id, dest_element_name, order_data['_id'],
                db, DB_PMK_NAME, CLN_GRID, session, True,
            )
            logger.error('delete cell')
            # -6- <- Unblock source `wheelStack` + reshuffle wheels.
            source_wheelstack_data['lastOrder'] = order_data['_id']
            source_wheelstack_data['blocked'] = False
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            logger.error('update wheelstack')
            # We store `objectId` in `affectedWheels` and `wheels`.
            # So it's guaranteed to be unique, we don't need to check a position, we can just remove it.
            for new_pos, wheel in enumerate(source_wheelstack_data['wheels']):
                logger.error('wheels', wheel)
                await db_update_wheel_position(
                    wheel, new_pos, db, DB_PMK_NAME, CLN_WHEELS, session
                )
            # -7- <- Update `chosenWheel`
            lab_wheel_data = await db_find_wheel_by_object_id(
                lab_wheel, db, DB_PMK_NAME, CLN_WHEELS, session
            )
            lab_wheel_data['wheelStack'] = None
            lab_wheel_data['status'] = PS_LABORATORY
            await db_update_wheel(
                lab_wheel, lab_wheel_data, db, DB_PMK_NAME, CLN_WHEELS, session
            )
            # -8- <- Delete order from `activeOrders`
            await db_delete_order(
                order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            # -9- <- Add order into `completedOrders`
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            completed_order = await db_create_order(
                order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
            )
            return completed_order.inserted_id


async def orders_complete_move_to_storage(
        order_data: dict,
        db: AsyncIOMotorClient,
) -> ObjectId:
    source_type: str = order_data['source']['placementType']
    source_id: ObjectId = order_data['source']['placementId']
    source_row: str = order_data['source']['rowPlacement']
    source_col: str = order_data['source']['columnPlacement']
    source_cell_data = await db_get_grid_cell_data(
        source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID
    )
    if source_cell_data is None:
        logger.error(f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                     f'But given order = {order_data['_id']} marks it as source cell.')
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell doesnt exist in the `{source_type}` = {source_id}'
                   f'But given order = {order_data['_id']} marks it as source cell.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    if source_cell_data['blockedBy'] != order_data['_id']:
        logger.error(f'Corrupted `order` = {order_data['_id']},'
                     f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                     f'But different order is blocking it {source_cell_data['blockedBy']}')
        raise HTTPException(
            detail=f'Corrupted `order` = {order_data['_id']},'
                   f' marking cell {source_row}|{source_col} in `grid` {source_id}.'
                   f'But different order is blocking it {source_cell_data['blockedBy']}',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    # -2- <- Check source `wheelStack` it should exist
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_cell_data['wheelStack'], db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        logger.error(
            f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
            f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.'
        )
        raise HTTPException(
            detail=f'Corrupted cell {source_row}|{source_col} in grid = {source_id}.'
                   f'Marks `wheelStacks` {source_cell_data['wheelStack']} as placed on it, but it doesnt exist.',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    storage_id = order_data['destination']['placementId']
    storage_data = await db_get_storage_by_object_id(
        storage_id, False, db, DB_PMK_NAME, CLN_STORAGES
    )
    if storage_data is None:
        logger.error(
            f'Corrupted order = {order_data['_id']} using non-existing `storage`'
        )
        raise HTTPException(
            detail=f'Corrupted order = {order_data['_id']} using non-existing `storage`',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_cell_data['wheelStack'] = None
    source_cell_data['blockedBy'] = None
    source_cell_data['blocked'] = False
    completion_time = await time_w_timezone()
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            await db_update_grid_cell_data(
                source_id, source_row, source_col, source_cell_data,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            source_wheelstack_data['placement']['type'] = PS_STORAGE
            source_wheelstack_data['placement']['placementId'] = storage_id
            source_wheelstack_data['rowPlacement'] = '0'
            source_wheelstack_data['colPlacement'] = '0'
            source_wheelstack_data['lastOrder'] = order_data['_id']
            source_wheelstack_data['status'] = PS_STORAGE
            source_wheelstack_data['blocked'] = False
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            for wheel in order_data['affectedWheels']['source']:
                await db_update_wheel_status(
                    wheel, PS_STORAGE, db, DB_PMK_NAME, CLN_WHEELS, session
                )
            await db_delete_order(
                order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            completed_order = await db_create_order(
                order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
            )
            await db_storage_place_wheelstack(
                storage_id, source_wheelstack_data['_id'], source_wheelstack_data['batchNumber'],
                db, DB_PMK_NAME, CLN_STORAGES, session
            )
            return completed_order.inserted_id


async def orders_complete_move_wholestack_from_storage(
        order_data: dict,
        db: AsyncIOMotorClient,
) -> ObjectId:
    storage_id = order_data['source']['placementId']
    source_wheelstack_id: ObjectId = await get_object_id(order_data['affectedWheelStacks']['source'])
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        raise HTTPException(
            detail=f'Corrupted order, point to non-existing `wheelstack`',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_id = order_data['destination']['placementId']
    destination_row = order_data['destination']['rowPlacement']
    destination_col = order_data['destination']['columnPlacement']
    destination_cell_data = await db_get_grid_cell_data(
        destination_id, destination_row, destination_col,
        db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        raise HTTPException(
            detail=f'Corrupted order, points     to non-existing cell '
                   f'= {destination_row}|{destination_col} in grid = {destination_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_cell_data = destination_cell_data['rows'][destination_row]['columns'][destination_col]
    if destination_cell_data['blocked'] and destination_cell_data['blockedBy'] != order_data['_id']:
        raise HTTPException(
            detail=f'Corrupted cell and order, cell blocked by different order = {destination_cell_data['blockedBy']}',
            status_code=status.HTTP_409_CONFLICT,
        )
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            # Delete wheelstack from storage
            del_res = await db_storage_delete_placed_wheelstack(
                storage_id, source_wheelstack_data['_id'], source_wheelstack_data['batchNumber'],
                db, DB_PMK_NAME, CLN_STORAGES, session, True
            )
            if 0 == del_res.modified_count:
                raise HTTPException(
                    detail=f'Corrupted Order. `wheelstack` not present in `storage` = {storage_id}',
                    status_code=status.HTTP_404_NOT_FOUND,
                )
            # Update wheelstack
            source_wheelstack_data['status'] = PS_GRID
            source_wheelstack_data['placement'] = {
                'type': PS_GRID,
                'placementId': destination_id,
            }
            source_wheelstack_data['rowPlacement'] = destination_row
            source_wheelstack_data['colPlacement'] = destination_col
            source_wheelstack_data['blocked'] = False
            source_wheelstack_data['status'] = PS_GRID
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'],
                db, DB_PMK_NAME, CLN_WHEELSTACKS, session, True
            )
            # Update wheels
            for wheel in source_wheelstack_data['wheels']:
                await db_update_wheel_status(
                    wheel, PS_GRID, db, DB_PMK_NAME, CLN_WHEELS, session
                )
            # Update placement cell in grid
            destination_cell_data['blocked'] = False
            destination_cell_data['blockedBy'] = None
            destination_cell_data['wheelStack'] = source_wheelstack_data['_id']
            await db_update_grid_cell_data(
                destination_id, destination_row, destination_col, destination_cell_data,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            completed_order = await db_create_order(
                order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
            )
            await db_delete_order(
                order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            return completed_order.inserted_id


async def orders_complete_move_to_pro_rej_from_storage(
        db: AsyncIOMotorClient,
        order_data: dict,
        processing: bool,
) -> ObjectId:
    storage_id = order_data['source']['placementId']
    source_wheelstack_id: ObjectId = await get_object_id(order_data['affectedWheelStacks']['source'])
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        raise HTTPException(
            detail=f'Corrupted order, point to non-existing `wheelstack`',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_id = order_data['destination']['placementId']
    destination_row = order_data['destination']['rowPlacement']
    # Using it as extraElement name
    destination_col = order_data['destination']['columnPlacement']
    destination_extra_element_data = await db_get_grid_extra_cell_data(
        destination_id, destination_col,
        db, DB_PMK_NAME, CLN_GRID
    )
    if destination_extra_element_data is None:
        raise HTTPException(
            detail=f'Corrupted order, points to non-existing extraElement'
                   f'= {destination_row}|{destination_col} in grid = {destination_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_extra_element_data = destination_extra_element_data['extra'][destination_col]
    if str(order_data['_id']) not in destination_extra_element_data['orders']:
        raise HTTPException(
            detail=f'Corrupted order, points to existing extraElement, but not present in it',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    elements_status = PS_SHIPPED if processing else PS_REJECTED
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            # Delete wheelstack from storage
            del_res = await db_storage_delete_placed_wheelstack(
                storage_id, source_wheelstack_data['_id'], source_wheelstack_data['batchNumber'],
                db, DB_PMK_NAME, CLN_STORAGES, session, True,
            )
            if 0 == del_res.modified_count:
                raise HTTPException(
                    detail=f'Corrupted Order. `wheelstack` not present in `storage` = {storage_id}',
                    status_code=status.HTTP_404_NOT_FOUND
                )
            # Update wheelstack status
            source_wheelstack_data['status'] = elements_status
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['rowPlacement'] = destination_row
            source_wheelstack_data['colPlacement'] = destination_col
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'],
                db, DB_PMK_NAME, CLN_WHEELSTACKS, session, True
            )
            # Update wheels statuses
            for wheel in source_wheelstack_data['wheels']:
                await db_update_wheel_status(
                    wheel, elements_status, db, DB_PMK_NAME, CLN_WHEELS, session,
                )
            # Delete order from extraElement
            await db_delete_extra_cell_order(
                destination_id, destination_col, order_data['_id'],
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            # Delete active order
            await db_delete_order(
                order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            # Create completed order
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            completed_order = await db_create_order(
                order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
            )
            return completed_order.inserted_id


async def orders_complete_move_from_storage_to_storage(
        order_data: dict,
        db: AsyncIOMotorClient,
) -> ObjectId:
    source_wheelstack_id: ObjectId = await get_object_id(order_data['affectedWheelStacks']['source'])
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    destination_storage_id = await get_object_id(order_data['destination']['placementId'])
    dest_exists = await db_get_storage_by_object_id(
        destination_storage_id, False, db, DB_PMK_NAME, CLN_STORAGES
    )
    if dest_exists is None:
        raise HTTPException(
            detail='Destination Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_wheelstack_data['placement']['type'] = PS_STORAGE
    source_wheelstack_data['placement']['placementId'] = dest_exists['_id']
    source_wheelstack_data['blocked'] = False
    completion_time = await time_w_timezone()
    order_data['completedAt'] = completion_time
    order_data['status'] = ORDER_STATUS_COMPLETED
    order_data['lastUpdated'] = completion_time
    source_storage_id = await get_object_id(order_data['source']['placementId'])
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            await db_delete_order(
                order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            await db_storage_delete_placed_wheelstack(
                source_storage_id, source_wheelstack_data['_id'], source_wheelstack_data['batchNumber'],
                db, DB_PMK_NAME, CLN_STORAGES, session, True
            )
            await db_storage_place_wheelstack(
                destination_storage_id, source_wheelstack_id, source_wheelstack_data['batchNumber'],
                db, DB_PMK_NAME, CLN_STORAGES, session, True
            )
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session,
            )
            created_order = await db_create_order(
                order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
            )
            return created_order.inserted_id


async def orders_complete_move_from_storage_to_lab(
        order_data: dict,
        db: AsyncIOMotorClient,
) -> ObjectId:
    completion_time = await time_w_timezone()
    order_data['status'] = ORDER_STATUS_COMPLETED
    order_data['completedAt'] = completion_time
    order_data['lastUpdated'] = completion_time
    source_storage_id = await get_object_id(order_data['source']['placementId'])
    source_wheelstack_id = await get_object_id(order_data['affectedWheelStacks']['source'])
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        raise HTTPException(
            detail='source wheelstack Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    chosen_wheel_id = order_data['affectedWheels']['source'][0]
    try:
        source_wheelstack_data['wheels'].remove(chosen_wheel_id)
    except ValueError:
        raise HTTPException(
            detail='chosenWheel not present in the wheelstack',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_wheelstack_data['blocked'] = False
    source_wheelstack_data['lastOrder'] = order_data['_id']
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            await db_delete_order(
                order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'],
                db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            completed_order = await db_create_order(
                order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session,
            )
            lab_wheel_data = await db_find_wheel_by_object_id(
                chosen_wheel_id, db, DB_PMK_NAME, CLN_WHEELS, session
            )
            lab_wheel_data['wheelStack'] = None
            lab_wheel_data['status'] = PS_LABORATORY
            await db_update_wheel(
                chosen_wheel_id, lab_wheel_data, db, DB_PMK_NAME, CLN_WHEELS, session
            )
            return completed_order.inserted_id
