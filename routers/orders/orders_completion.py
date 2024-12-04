import asyncio
from loguru import logger
from bson import ObjectId
from fastapi import HTTPException, status
from routers.batch_numbers.crud import db_insert_test_wheel
from utility.utilities import time_w_timezone, get_object_id
from routers.orders.crud import db_delete_order, db_create_order
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from routers.base_platform.crud import db_get_platform_cell_data, db_update_platform_cell_data
from routers.storages.crud import (
    db_get_storage_by_object_id,
    db_storage_get_placed_wheelstack,
    db_storage_place_wheelstack,
    db_storage_delete_placed_wheelstack
)
from routers.wheels.crud import (
    db_update_wheel_status,
    db_update_wheel_position,
    db_find_wheel_by_object_id,
    db_update_wheel
)
from routers.grid.crud import (
    db_get_grid_cell_data,
    db_grid_update_last_change_time,
    db_update_grid_cell_data,
    db_get_grid_extra_cell_data,
)
from routers.wheelstacks.crud import (
    db_find_wheelstack_by_object_id,
    db_update_wheelstack
)
from constants import (
    CLN_BATCH_NUMBERS,
    PRES_TYPE_GRID,
    PRES_TYPE_PLATFORM,
    DB_PMK_NAME,
    CLN_GRID,
    CLN_BASE_PLATFORM,
    CLN_WHEELSTACKS,
    CLN_WHEELS,
    CLN_ACTIVE_ORDERS,
    CLN_COMPLETED_ORDERS,
    ORDER_STATUS_COMPLETED,
    PS_DECONSTRUCTED,
    PS_REJECTED,
    PS_GRID,
    PS_SHIPPED,
    PS_LABORATORY,
    CLN_STORAGES,
    PS_STORAGE,
    PS_BASE_PLATFORM
)


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
                record_change = False if dest_id == source_id else True
                await db_update_grid_cell_data(
                    source_id, source_row, source_col, source_cell_data,
                    db, DB_PMK_NAME, CLN_GRID, session, record_change
                )
            elif PRES_TYPE_PLATFORM == source_type:
                await db_update_platform_cell_data(
                    source_id, source_row, source_col, source_cell_data,
                    db, DB_PMK_NAME, CLN_BASE_PLATFORM, session, True
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
    # -4- <- Clear source cell
    source_cell_data['wheelStack'] = None
    source_cell_data['blockedBy'] = None
    source_cell_data['blocked'] = False
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            transaction_tasks = []
            update_both: bool = False if source_id == dest_id else True
            transaction_tasks.append(
                db_update_grid_cell_data(
                    source_id, source_row, source_col, source_cell_data,
                    db, DB_PMK_NAME, CLN_GRID, session, True
                )
            )
            if update_both:
                transaction_tasks.append(
                    db_grid_update_last_change_time(
                        dest_id, db, DB_PMK_NAME, CLN_GRID, session
                    )
                )
            # -5- <- Update `wheelStack` record
            source_wheelstack_data['placement']['type'] = dest_type
            source_wheelstack_data['placement']['placementId'] = dest_id
            source_wheelstack_data['rowPlacement'] = dest_element_row
            source_wheelstack_data['colPlacement'] = dest_element_name
            source_wheelstack_data['lastOrder'] = order_data['_id']
            source_wheelstack_data['status'] = PS_SHIPPED
            source_wheelstack_data['blocked'] = True
            transaction_tasks.append(
                 db_update_wheelstack(
                    source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
                )
            )
            # -6- <- Update status for each wheel
            for wheel in order_data['affectedWheels']['source']:
                transaction_tasks.append(
                    db_update_wheel_status(
                        wheel, PS_SHIPPED, db, DB_PMK_NAME, CLN_WHEELS, session
                    )
                )
            # -7- Delete order from `activeOrders`
            transaction_tasks.append(
                db_delete_order(
                    order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
                )
            )
            # -8- Add order into `completedOrders`
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            transaction_tasks.append(
                db_create_order(
                    order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
                )
            )
            transaction_results = await asyncio.gather(*transaction_tasks)
            completed_order_result = transaction_results[-1]
            return completed_order_result.inserted_id


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
    # -4- <- Clear source cell
    source_cell_data['wheelStack'] = None
    source_cell_data['blockedBy'] = None
    source_cell_data['blocked'] = False
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            transaction_tasks = []
            update_both: bool = False if source_id == dest_id else True
            transaction_tasks.append(
                db_update_grid_cell_data(
                    source_id, source_row, source_col, source_cell_data,
                    db, DB_PMK_NAME, CLN_GRID, session, True
                )
            )
            if update_both:
                db_grid_update_last_change_time(
                    dest_id, db, DB_PMK_NAME, CLN_GRID, session
                )
            # -5- <- Update `wheelStack` record
            source_wheelstack_data['placement']['type'] = dest_type
            source_wheelstack_data['placement']['placementId'] = dest_id
            source_wheelstack_data['rowPlacement'] = dest_element_row
            source_wheelstack_data['colPlacement'] = dest_element_name
            source_wheelstack_data['lastOrder'] = order_data['_id']
            source_wheelstack_data['status'] = PS_REJECTED
            source_wheelstack_data['blocked'] = True
            transaction_tasks.append(
                db_update_wheelstack(
                    source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
                )
            )
            # -6- <- Update status for each wheel
            for wheel in order_data['affectedWheels']['source']:
                transaction_tasks.append(
                    db_update_wheel_status(
                        wheel, PS_REJECTED, db, DB_PMK_NAME, CLN_WHEELS, session
                    )
                )
            # -7- Delete order from `activeOrders`
            transaction_tasks.append(
                db_delete_order(
                    order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
                )
            )
            # -8- Add order into `completedOrders`
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            transaction_tasks.append(
                db_create_order(
                    order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
                )
            )
            transaction_tasks_results = await asyncio.gather(*transaction_tasks)
            completed_order = transaction_tasks_results[-1]
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
    source_wheelstack_data['blocked'] = False
    source_wheelstack_data['lastOrder'] = order_data['_id']
    if 0 == len(source_wheelstack_data['wheels']):
        source_cell_data['wheelStack'] = None
        source_wheelstack_data['blocked'] = True
        source_wheelstack_data['status'] = PS_SHIPPED
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            transaction_tasks = []
            update_both: bool = False if source_id == dest_id else True
            transaction_tasks.append(
                db_update_grid_cell_data(
                    source_id, source_row, source_col, source_cell_data, db,
                    DB_PMK_NAME, CLN_GRID, session, True
                )
            )
            if update_both:
                db_grid_update_last_change_time(
                    dest_id, db, DB_PMK_NAME, CLN_GRID, session
                )
            # -5- <- Unblock source `wheelStack` + reshuffle wheels.
            transaction_tasks.append(
                db_update_wheelstack(
                    source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
                )
            )
            # We store `objectId` in `affectedWheels` and `wheels`.
            # So it's guaranteed to be unique, we don't need to check a position, we can just remove it.
            for new_pos, wheel in enumerate(source_wheelstack_data['wheels']):
                transaction_tasks.append(
                    db_update_wheel_position(
                        wheel, new_pos, db, DB_PMK_NAME, CLN_WHEELS, session
                    )
                )
            # -6- <- Update `chosenWheel`
            lab_wheel_data = await db_find_wheel_by_object_id(
                lab_wheel, db, DB_PMK_NAME, CLN_WHEELS, session
            )
            lab_wheel_data['wheelStack'] = None
            lab_wheel_data['status'] = PS_LABORATORY
            transaction_tasks.append(
                db_update_wheel(
                    lab_wheel, lab_wheel_data, db, DB_PMK_NAME, CLN_WHEELS, session
                )
            )
            # -7- <- Delete order from `activeOrders`
            transaction_tasks.append(
                db_delete_order(
                    order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
                )
            )
            # -8- <- Add order into `completedOrders`
            completion_time = await time_w_timezone()
            # region labRebuild
            target_batch_number = lab_wheel_data['batchNumber']
            test_wheel_record: dict[str, ObjectId | str | None] = {
                '_id': lab_wheel_data['_id'],
                'arrivalDate': completion_time,
                'result': None,
                'testDate': None,
                'confirmedBy': '',
            }
            transaction_tasks.append(
                db_insert_test_wheel(
                    target_batch_number, test_wheel_record,
                    db, DB_PMK_NAME, CLN_BATCH_NUMBERS, session
                )
            )
            # endregion labRebuild
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            transaction_tasks.append(
                db_create_order(
                    order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
                )
            )
            transaction_tasks_results = await asyncio.gather(*transaction_tasks)
            completed_order = transaction_tasks_results[-1]
            return completed_order.inserted_id


async def orders_complete_move_to_storage(
        order_data: dict,
        db: AsyncIOMotorClient,
) -> ObjectId:
    source_type: str = order_data['source']['placementType']
    source_id: ObjectId = order_data['source']['placementId']
    source_row: str = order_data['source']['rowPlacement']
    source_col: str = order_data['source']['columnPlacement']
    source_cell_data = None
    if PS_GRID == source_type:
        source_cell_data = await db_get_grid_cell_data(
            source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID
        )
    elif PS_BASE_PLATFORM == source_type:
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
            transaction_tasks = []
            if PS_GRID == source_type:
                transaction_tasks.append(
                    db_update_grid_cell_data(
                        source_id, source_row, source_col, source_cell_data,
                        db, DB_PMK_NAME, CLN_GRID, session, True
                    )
                )
            elif PS_BASE_PLATFORM == source_type:
                transaction_tasks.append(
                    db_update_platform_cell_data(
                        source_id, source_row, source_col, source_cell_data,
                        db, DB_PMK_NAME, CLN_BASE_PLATFORM, session, True
                    )
                )
            source_wheelstack_data['placement']['type'] = PS_STORAGE
            source_wheelstack_data['placement']['placementId'] = storage_id
            source_wheelstack_data['rowPlacement'] = '0'
            source_wheelstack_data['colPlacement'] = '0'
            source_wheelstack_data['lastOrder'] = order_data['_id']
            source_wheelstack_data['status'] = PS_STORAGE
            source_wheelstack_data['blocked'] = False
            transaction_tasks.append(
                db_update_wheelstack(
                    source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
                )
            )
            for wheel in order_data['affectedWheels']['source']:
                transaction_tasks.append(
                    db_update_wheel_status(
                        wheel, PS_STORAGE, db, DB_PMK_NAME, CLN_WHEELS, session
                    )
                )
            transaction_tasks.append(
                db_delete_order(
                    order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
                )
            )
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            transaction_tasks.append(
                db_storage_place_wheelstack(
                    storage_id, '', source_wheelstack_data['_id'],
                    db, DB_PMK_NAME, CLN_STORAGES, session
                )
            )
            transaction_tasks.append(
                db_create_order(
                    order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
                )
            )
            transaction_tasks_results = await asyncio.gather(*transaction_tasks)
            completed_order = transaction_tasks_results[-1]
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
                storage_id, '', source_wheelstack_data['_id'],
                db, DB_PMK_NAME, CLN_STORAGES, session, True
            )
            if 0 == del_res.modified_count:
                raise HTTPException(
                    detail=f'Corrupted Order. `wheelstack` not present in `storage` = {storage_id}',
                    status_code=status.HTTP_404_NOT_FOUND,
                )
            transaction_tasks = []
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
            transaction_tasks.append(
                db_update_wheelstack(
                    source_wheelstack_data, source_wheelstack_data['_id'],
                    db, DB_PMK_NAME, CLN_WHEELSTACKS, session, True
                )
            )
            # Update wheels
            for wheel in source_wheelstack_data['wheels']:
                transaction_tasks.append(
                    db_update_wheel_status(
                        wheel, PS_GRID, db, DB_PMK_NAME, CLN_WHEELS, session
                    )
                )
            # Update placement cell in grid
            destination_cell_data['blocked'] = False
            destination_cell_data['blockedBy'] = None
            destination_cell_data['wheelStack'] = source_wheelstack_data['_id']
            transaction_tasks.append(
                db_update_grid_cell_data(
                    destination_id, destination_row, destination_col, destination_cell_data,
                    db, DB_PMK_NAME, CLN_GRID, session, True
                )
            )
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            transaction_tasks.append(
                db_delete_order(
                    order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
                )
            )
            transaction_tasks.append(
                db_create_order(
                    order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
                )
            )
            transaction_tasks_results = await asyncio.gather(*transaction_tasks)
            completed_order = transaction_tasks_results[-1]
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
    elements_status = PS_SHIPPED if processing else PS_REJECTED
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            # Delete wheelstack from storage
            del_res = await db_storage_delete_placed_wheelstack(
                storage_id, '', source_wheelstack_data['_id'],
                db, DB_PMK_NAME, CLN_STORAGES, session, True,
            )
            if 0 == del_res.modified_count:
                raise HTTPException(
                    detail=f'Corrupted Order. `wheelstack` not present in `storage` = {storage_id}',
                    status_code=status.HTTP_404_NOT_FOUND
                )
            transaction_tasks = []
            # Update wheelstack status
            source_wheelstack_data['status'] = elements_status
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['rowPlacement'] = destination_row
            source_wheelstack_data['colPlacement'] = destination_col
            transaction_tasks.append(
                db_update_wheelstack(
                    source_wheelstack_data, source_wheelstack_data['_id'],
                    db, DB_PMK_NAME, CLN_WHEELSTACKS, session, True
                )
            )
            # Update wheels statuses
            for wheel in source_wheelstack_data['wheels']:
                transaction_tasks.append(
                    db_update_wheel_status(
                        wheel, elements_status, db, DB_PMK_NAME, CLN_WHEELS, session,
                    )
                )
            # Delete active order
            transaction_tasks.append(
                db_delete_order(
                    order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
                )
            )
            # Create completed order
            completion_time = await time_w_timezone()
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            transaction_tasks.append(
                db_create_order(
                    order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
                )
            )
            transaction_tasks_results = await asyncio.gather(*transaction_tasks)
            completed_order = transaction_tasks_results[-1]
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
            transaction_tasks = []
            transaction_tasks.append(
                db_delete_order(
                    order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
                )
            )
            record_both: bool = False if source_storage_id == destination_storage_id else True
            transaction_tasks.append(
                db_storage_delete_placed_wheelstack(
                    source_storage_id, '', source_wheelstack_data['_id'],
                    db, DB_PMK_NAME, CLN_STORAGES, session, record_both
                )
            )
            transaction_tasks.append(
                db_storage_place_wheelstack(
                    destination_storage_id, '', source_wheelstack_id,
                    db, DB_PMK_NAME, CLN_STORAGES, session, True
                )
            )
            transaction_tasks.append(
                db_update_wheelstack(
                    source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session,
                )
            )
            transaction_tasks.append(
                db_create_order(
                    order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
                )
            )
            transaction_tasks_results = await asyncio.gather(*transaction_tasks)
            created_order = transaction_tasks_results[-1]
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
    dest_id = order_data['destination']['placementId']
    dest_element_name = order_data['destination']['columnPlacement']
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            transaction_tasks = []
            transaction_tasks.append(
                db_delete_order(
                    order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
                )
            )
            if 0 == len(source_wheelstack_data['wheels']):
                source_wheelstack_data['blocked'] = True
                source_wheelstack_data['status'] = PS_SHIPPED
                transaction_tasks.append(
                    db_storage_delete_placed_wheelstack(
                        source_storage_id, '', source_wheelstack_data['_id'],
                        db, DB_PMK_NAME, CLN_STORAGES, session, True
                    )
                )
            transaction_tasks.append(
                db_update_wheelstack(
                    source_wheelstack_data, source_wheelstack_data['_id'],
                    db, DB_PMK_NAME, CLN_WHEELSTACKS, session
                )
            )
            for new_pos, wheel in enumerate(source_wheelstack_data['wheels']):
                transaction_tasks.append(
                    db_update_wheel_position(
                        wheel, new_pos, db, DB_PMK_NAME, CLN_WHEELS, session
                    )
                )
            lab_wheel_data = await db_find_wheel_by_object_id(
                chosen_wheel_id, db, DB_PMK_NAME, CLN_WHEELS, session
            )
            lab_wheel_data['wheelStack'] = None
            lab_wheel_data['status'] = PS_LABORATORY
            transaction_tasks.append(
                db_update_wheel(
                    chosen_wheel_id, lab_wheel_data, db, DB_PMK_NAME, CLN_WHEELS, session
                )
            )
            # region labRebuild
            target_batch_number = lab_wheel_data['batchNumber']
            test_wheel_record: dict[str, ObjectId | str | None] = {
                '_id': lab_wheel_data['_id'],
                'arrivalDate': completion_time,
                'result': None,
                'testDate': None,
                'confirmedBy': '',
            }
            transaction_tasks.append(
                db_insert_test_wheel(
                    target_batch_number, test_wheel_record,
                    db, DB_PMK_NAME, CLN_BATCH_NUMBERS, session
                )
            )
            # endregion labRebuild
            transaction_tasks.append(
                db_create_order(
                    order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session,
                )
            )
            transaction_tasks_results = await asyncio.gather(*transaction_tasks)
            completed_order = transaction_tasks_results[-1]
            return completed_order.inserted_id


async def get_placement_cell_data(
    placement_id: ObjectId,
    placement_type: str,
    placement_row: str,
    placement_col: str,
    db: AsyncIOMotorClient, 
) -> dict:
    placement_cell_data: dict | None = None
    if PRES_TYPE_GRID == placement_type:
        placement_cell_data = await db_get_grid_cell_data(
            placement_id, placement_row, placement_col, db, DB_PMK_NAME, CLN_GRID
        )
    elif PRES_TYPE_PLATFORM == placement_type:
        placement_cell_data = await db_get_platform_cell_data(
            placement_id, placement_row, placement_col, db, DB_PMK_NAME, CLN_BASE_PLATFORM
        )
    return placement_cell_data


async def validate_cell_data(
    order_data: dict,
    source: bool,
    cell_placement_data: dict | None
) -> dict:
    msg_string: str
    source_filter: str = 'source' if source else 'destination'
    cell_row: str = order_data[source_filter]['rowPlacement']
    cell_col: str = order_data[source_filter]['columnPlacement']
    placement_type: str = order_data[source_filter]['placementType']
    placement_id: ObjectId = order_data[source_filter]['placementId']
    # Cell exists
    if cell_placement_data is None:
        msg_string = f'Corrupted `order` = {cell_row}|{cell_col} <- {source_filter} cell doesnt exist in the `{placement_type}` => {placement_id}' \
                     f'But given order = {order_data['_id']} marks it as {source_filter} cell.'
        logger.error(msg_string)
        raise HTTPException(
            detail=msg_string,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    cell_data = cell_placement_data['rows'][cell_row]['columns'][cell_col]
    # Cell blocked by correct Order
    if cell_data['blockedBy'] != order_data['_id']:
        msg_string = f'Corrupted `order` = {order_data['_id']},' \
                     f' marking cell {cell_row}|{cell_col} in `{placement_type}` => {placement_id}.' \
                     f'But different order is blocking it {cell_data['blockedBy']}'
        logger.error(msg_string)
        raise HTTPException(
            detail=msg_string,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    # Cell contains correct wheelstack
    target_wheelstack = order_data['affectedWheelStacks'][source_filter]
    if cell_data['wheelStack'] != target_wheelstack:
        msg_string = f'{cell_row}|{cell_col} <- {source_filter} cell exists in the `{placement_type}` => {placement_id}.' \
                     f' But it contains non target `wheelstack` => {cell_data['wheelStack']}. While order = {order_data['_id']} target => {target_wheelstack}.'
        logger.error(msg_string)
        raise HTTPException(
            detail=msg_string,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    return cell_data


async def validate_wheelstack_data(
    wheelstack_data: dict,
    source: bool,
    order_data: dict
) -> None:
    msg_string: str
    # 1. Wheelstack should exist
    # 2. Wheelstack should be correctly placed in placement
    # 3. Wheelstack should be correctly blocked by given order
    source_filter: str = 'source' if source else 'destination'
    cell_row: str = order_data[source_filter]['rowPlacement']
    cell_col: str = order_data[source_filter]['columnPlacement']
    placement_type: str = order_data[source_filter]['placementType']
    placement_id: ObjectId = order_data[source_filter]['placementId']
    wheelstack_id: ObjectId = order_data['affectedWheelStacks'][source_filter]
    if wheelstack_data is None:
        msg_string = f'Corrupted cell = {cell_row}|{cell_col} in `{placement_type}` = {placement_id}.' \
                     f' Marks `wheelstack` = {wheelstack_id} as placed on it, but it doesnt exist.'
        logger.error(msg_string)
        raise HTTPException(
            detail=msg_string,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    wheelstack_row: str = wheelstack_data['rowPlacement']
    wheelstack_col: str = wheelstack_data['colPlacement']
    if placement_type != PS_STORAGE and (cell_row != wheelstack_row or cell_col != wheelstack_col):
        msg_string = f'Corrupted cell = {cell_row}|{cell_col} in `{placement_type}` = {placement_id}.' \
                     f' Marks `wheelstack` = {wheelstack_id} as placed on it, but it have a different placement.'
        raise HTTPException(
            detail=msg_string,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    wheelstack_blocking_order = wheelstack_data['lastOrder']
    if wheelstack_blocking_order != order_data['_id']:
        msg_string = f'Corrupted `order` = {order_data['_id']}. Points to `wheelstack` = {wheelstack_id}.' \
                     f' But different `order` blocks it = {wheelstack_blocking_order}'
        raise HTTPException(
            detail=msg_string,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def update_placement_cell(
    cell_coords: dict,
    cell_data: dict,
    db: AsyncIOMotorClient,
    session: AsyncIOMotorClientSession,
    record_change: bool = False,
) -> None:
    placement_id: ObjectId = cell_coords['placementId']
    placement_type: str = cell_coords['placementType']
    placement_row: str = cell_coords['rowPlacement']
    placement_col: str = cell_coords['columnPlacement']
    if PRES_TYPE_GRID == placement_type:
        await db_update_grid_cell_data(
            placement_id, placement_row, placement_col, cell_data,
            db, DB_PMK_NAME, CLN_GRID, session, record_change
        )
    elif PRES_TYPE_PLATFORM == placement_type:
        await db_update_platform_cell_data(
            placement_id, placement_row, placement_col, cell_data,
            db, DB_PMK_NAME, CLN_BASE_PLATFORM, session, record_change
        )


async def check_storage_wheelstack(
        order_data: dict,
        db: AsyncIOMotorClient,
) -> None:
    source_tasks = []
    source_tasks.append(
        get_object_id(order_data['source']['placementId'])
    )
    source_tasks.append(
        get_object_id(order_data['affectedWheelStacks']['source'])
    )
    source_results = await asyncio.gather(*source_tasks)
    source_id: ObjectId = source_results[0]
    source_wheelstack_id: ObjectId = source_results[1]
    storage_data = await db_storage_get_placed_wheelstack(
        source_id, '', source_wheelstack_id, db, DB_PMK_NAME, CLN_STORAGES
    )
    if storage_data is None:
        raise HTTPException(
            detail=f'Corrupted order {order_data['_id']} marks storage for merge with non existing `wheelstack` => {source_wheelstack_id}',
            status_code=status.HTTP_403_FORBIDDEN,
        )


async def orders_complete_merge_wheelstacks(
        order_data: dict,
        db: AsyncIOMotorClient,
) -> ObjectId:
    msg_str: str
    # SOURCE && DEST data gather
    # 1. Placement exists
    # 2. Target cell exists
    # 3. Target cell contains - order target wheelstack
    # 4. Target cell wheelstack exists
    source_id: ObjectId = order_data['source']['placementId']
    source_type: str = order_data['source']['placementType']
    source_row: str = order_data['source']['rowPlacement']
    source_col: str = order_data['source']['columnPlacement']

    destination_id: ObjectId = order_data['destination']['placementId']
    destination_type: str = order_data['destination']['placementType']
    destination_row: str = order_data['destination']['rowPlacement']
    destination_col: str = order_data['destination']['columnPlacement']
    # + Gather source|dest cells data +
    cell_data_tasks = []
    if source_type == PS_STORAGE:
        cell_data_tasks.append(
            check_storage_wheelstack(
                order_data, db,
            )
        )
    else:
        cell_data_tasks.append(
            get_placement_cell_data(
                source_id, source_type, source_row, source_col, db
            )
        )
    cell_data_tasks.append(
        get_placement_cell_data(
            destination_id, destination_type, destination_row, destination_col, db
        )
    )
    cell_data_results = await asyncio.gather(*cell_data_tasks)
    cell_validate_tasks = []
    if source_type != PS_STORAGE:
        source_cell_placement_data = cell_data_results[0]
        cell_validate_tasks.append(
            validate_cell_data(order_data, True, source_cell_placement_data)
        )
    destination_placement_cell_data = cell_data_results[-1]
    # - Gather source|dest cells data -
    # + Validate source|dest cells +
    cell_validate_tasks.append(
        validate_cell_data(order_data, False, destination_placement_cell_data)
    )
    cell_validate_results = await asyncio.gather(*cell_validate_tasks)
    if source_type != PS_STORAGE:
        source_cell_data = cell_validate_results[0]
    destination_cell_data = cell_validate_results[-1]
    # - Validate source|dest cells -
    # + Gather source|dest wheelstacks data +
    source_wheelstack_id: ObjectId = order_data['affectedWheelStacks']['source']
    destination_wheelstack_id: ObjectId = order_data['affectedWheelStacks']['destination']
    wheelstacks_data_tasks = []
    wheelstacks_data_tasks.append(
        db_find_wheelstack_by_object_id(
            source_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
        )
    )
    wheelstacks_data_tasks.append(
        db_find_wheelstack_by_object_id(
            destination_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
        )
    )
    wheelstacks_data_results = await asyncio.gather(*wheelstacks_data_tasks)
    # - Gather source|dest wheelstacks data -
    source_wheelstack_data = wheelstacks_data_results[0]
    destination_wheelstack_data = wheelstacks_data_results[1]
    # + Valide source|dest wheelstacks +
    wheelstacks_validate_tasks = []
    wheelstacks_validate_tasks.append(
        validate_wheelstack_data(
            source_wheelstack_data, True, order_data
        )
    )
    wheelstacks_validate_tasks.append(
        validate_wheelstack_data(
            destination_wheelstack_data, False, order_data
        )
    )
    await asyncio.gather(*wheelstacks_validate_tasks)
    # - Valide source|dest wheelstacks -
    source_wheels_count: int = len(source_wheelstack_data['wheels'])
    destination_wheels_count: int = len(destination_wheelstack_data['wheels'])
    if (source_wheels_count + destination_wheels_count) > destination_wheelstack_data['maxSize']:
        msg_str = f'Corrupted order = {order_data['_id']}. Trying to merge `wheelstacks` with wheels' \
                  f'exceeding destination wheelstack = {destination_wheelstack_id} limit = {destination_wheelstack_data['maxSize']}'
        raise HTTPException(
            detail=msg_str,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    completion_time = await time_w_timezone()
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            update_both = True if source_id != destination_id else False
            transaction_tasks = []
            # + Placement cells update +
            # For now only allow merge FROM `storage`s
            if source_type == PS_STORAGE:
                source_storage_id: ObjectId = await get_object_id(order_data['source']['placementId']) 
                transaction_tasks.append(
                    db_storage_delete_placed_wheelstack(
                        source_storage_id, '', source_wheelstack_id,
                        db, DB_PMK_NAME, CLN_STORAGES, session, True
                    )
                )
            else:
                source_cell_data['blocked'] = False
                source_cell_data['blockedBy'] = None
                source_cell_data['wheelStack'] = None
                transaction_tasks.append(
                    update_placement_cell(
                        order_data['source'], source_cell_data, db, session, update_both
                ))
            destination_cell_data['blocked'] = False
            destination_cell_data['blockedBy'] = None
            destination_cell_data['wheelStack'] = destination_wheelstack_data['_id']
            transaction_tasks.append(
                update_placement_cell(
                    order_data['destination'], destination_cell_data, db, session, True
            ))
            # - Placement cells update - 
            new_merged_wheels = destination_wheelstack_data['wheels'] + source_wheelstack_data['wheels']
            # + Affected wheelstacks update +
            source_wheelstack_data['lastOrder'] = order_data['_id']
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['status'] = PS_DECONSTRUCTED
            transaction_tasks.append(
                db_update_wheelstack(
                    source_wheelstack_data, source_wheelstack_data['_id'], db,
                    DB_PMK_NAME, CLN_WHEELSTACKS, session, True
            ))
            destination_wheelstack_data['lastOrder'] = order_data['_id']
            destination_wheelstack_data['blocked'] = False
            destination_wheelstack_data['wheels'] = new_merged_wheels
            transaction_tasks.append(
                db_update_wheelstack(
                    destination_wheelstack_data, destination_wheelstack_data['_id'], db,
                    DB_PMK_NAME, CLN_WHEELSTACKS, session, True
            ))
            # - Affected wheelstacks update -
            # + Affected wheels update +
            for wheel_pos, wheel_object_id in enumerate(new_merged_wheels):
                new_wheel_data = {
                    'wheelStack': {
                        'wheelStackId': destination_wheelstack_id,
                        'wheelStackPosition': wheel_pos
                    }
                }
                transaction_tasks.append(
                    db_update_wheel(
                        wheel_object_id, new_wheel_data, db, DB_PMK_NAME, CLN_WHEELS, session
                    )
                )
            # - Affected wheels update -
            # + Delete cur order +
            transaction_tasks.append(
                db_delete_order(
                    order_data['_id'], db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
                )
            )
            # - Delete cur order -
            # + Create completed order +
            order_data['status'] = ORDER_STATUS_COMPLETED
            order_data['lastUpdated'] = completion_time
            order_data['completedAt'] = completion_time
            transaction_tasks.append(
                db_create_order(
                    order_data, db, DB_PMK_NAME, CLN_COMPLETED_ORDERS, session
                )
            )
            # - Create completed order -
            transaction_results = await asyncio.gather(*transaction_tasks)
            completed_order = transaction_results[-1]
            return completed_order.inserted_id
