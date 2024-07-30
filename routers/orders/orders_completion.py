from loguru import logger
from bson import ObjectId
from fastapi import HTTPException, status
from utility.utilities import time_w_timezone
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
from routers.wheelstacks.crud import db_find_wheelstack_by_object_id, db_update_wheelstack
from constants import (PRES_TYPE_GRID, PRES_TYPE_PLATFORM,
                       DB_PMK_NAME, CLN_GRID, CLN_BASE_PLATFORM,
                       CLN_WHEELSTACKS, CLN_WHEELS, CLN_ACTIVE_ORDERS,
                       CLN_COMPLETED_ORDERS, ORDER_STATUS_COMPLETED,
                       PS_REJECTED, PS_GRID, PS_SHIPPED, PS_LABORATORY)


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
    if PRES_TYPE_GRID == source_type:
        await db_update_grid_cell_data(
            source_id, source_row, source_col, source_cell_data, db, DB_PMK_NAME, CLN_GRID
        )
    elif PRES_TYPE_PLATFORM == source_type:
        await db_update_platform_cell_data(
            source_id, source_row, source_col, source_cell_data, db, DB_PMK_NAME, CLN_BASE_PLATFORM
        )
    # -5- <- Transfer `wheelStack` on destination cell
    destination_cell_data['blocked'] = False
    destination_cell_data['blockedBy'] = None
    destination_cell_data['wheelStack'] = source_wheelstack_data['_id']
    await db_update_grid_cell_data(
        dest_id, dest_row, dest_col, destination_cell_data, db, DB_PMK_NAME, CLN_GRID
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
        source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    # -7- Update status of every affected wheel
    for wheel in order_data['affectedWheels']['source']:
        await db_update_wheel_status(
            wheel, PS_GRID, db, DB_PMK_NAME, CLN_WHEELS
        )
    # -8- Delete order from `activeOrders`
    await db_delete_order(
        order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS
    )
    # -9- Add order into `completedOrders`
    completion_time = await time_w_timezone()
    order_data['status'] = ORDER_STATUS_COMPLETED
    order_data['lastUpdated'] = completion_time
    order_data['completedAt'] = completion_time
    completed_order = await db_create_order(
        order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS
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
    await db_update_grid_cell_data(
        source_id, source_row, source_col, source_cell_data, db, DB_PMK_NAME, CLN_GRID
    )
    # -5- <- Delete order from destination element
    await db_delete_extra_cell_order(
        dest_id, dest_element_name, order_data['_id'], db, DB_PMK_NAME, CLN_GRID
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
        source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    # -7- <- Update status for each wheel
    for wheel in order_data['affectedWheels']['source']:
        await db_update_wheel_status(
            wheel, PS_SHIPPED, db, DB_PMK_NAME, CLN_WHEELS
        )
    # -8- Delete order from `activeOrders`
    await db_delete_order(
        order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS
    )
    # -9- Add order into `completedOrders`
    completion_time = await time_w_timezone()
    order_data['status'] = ORDER_STATUS_COMPLETED
    order_data['lastUpdated'] = completion_time
    order_data['completedAt'] = completion_time
    completed_order = await db_create_order(
        order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS
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
    await db_update_grid_cell_data(
        source_id, source_row, source_col, source_cell_data, db, DB_PMK_NAME, CLN_GRID
    )
    # -5- <- Delete order from destination element
    await db_delete_extra_cell_order(
        dest_id, dest_element_name, order_data['_id'], db, DB_PMK_NAME, CLN_GRID
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
        source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    # -7- <- Update status for each wheel
    for wheel in order_data['affectedWheels']['source']:
        await db_update_wheel_status(
            wheel, PS_REJECTED, db, DB_PMK_NAME, CLN_WHEELS
        )
    # -8- Delete order from `activeOrders`
    await db_delete_order(
        order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS
    )
    # -9- Add order into `completedOrders`
    completion_time = await time_w_timezone()
    order_data['status'] = ORDER_STATUS_COMPLETED
    order_data['lastUpdated'] = completion_time
    order_data['completedAt'] = completion_time
    completed_order = await db_create_order(
        order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS
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
    await db_update_grid_cell_data(
        source_id, source_row, source_col, source_cell_data, db, DB_PMK_NAME, CLN_GRID
    )
    # -5- <- Delete order from destination element
    await db_delete_extra_cell_order(
        dest_id, dest_element_name, order_data['_id'], db, DB_PMK_NAME, CLN_GRID
    )
    # -6- <- Unblock source `wheelStack` + reshuffle wheels.
    source_wheelstack_data['lastOrder'] = order_data['_id']
    source_wheelstack_data['blocked'] = False
    await db_update_wheelstack(
        source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    # We store `objectId` in `affectedWheels` and `wheels`.
    # So it's guaranteed to be unique, we don't need to check a position, we can just remove it.
    for new_pos, wheel in enumerate(source_wheelstack_data['wheels']):
        await db_update_wheel_position(
            wheel, new_pos, db, DB_PMK_NAME, CLN_WHEELS
        )
    # -7- <- Update `chosenWheel`
    lab_wheel_data = await db_find_wheel_by_object_id(
        lab_wheel, db, DB_PMK_NAME, CLN_WHEELS
    )
    lab_wheel_data['wheelStack'] = None
    lab_wheel_data['status'] = PS_LABORATORY
    await db_update_wheel(
        lab_wheel, lab_wheel_data, db, DB_PMK_NAME, CLN_WHEELS
    )
    # -8- <- Delete order from `activeOrders`
    await db_delete_order(
        order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS
    )
    # -9- <- Add order into `completedOrders`
    completion_time = await time_w_timezone()
    order_data['status'] = ORDER_STATUS_COMPLETED
    order_data['lastUpdated'] = completion_time
    order_data['completedAt'] = completion_time
    completed_order = await db_create_order(
        order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS
    )
    return completed_order.inserted_id
