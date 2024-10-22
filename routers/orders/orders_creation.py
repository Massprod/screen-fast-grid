import asyncio

from bson import ObjectId
from loguru import logger
from fastapi import HTTPException, status
from routers.orders.crud import db_create_order
from motor.motor_asyncio import AsyncIOMotorClient

from routers.storages.crud import db_get_storage_by_object_id, db_storage_check_placed_wheelstack
from utility.utilities import get_object_id, time_w_timezone, orders_creation_attempt_string, \
    orders_corrupted_cell_non_existing_wheelstack, orders_corrupted_cell_blocked_wheelstack
from routers.grid.crud import (db_get_grid_cell_data, db_update_grid_cell_data,
                               db_get_grid_extra_cell_data, get_grid_preset_by_object_id,
                               db_append_extra_cell_order, db_append_extra_cell_orders, db_update_grid_cells_data)
from routers.wheelstacks.crud import db_find_wheelstack_by_object_id, db_update_wheelstack, \
    db_find_all_pro_rej_available_in_placement, db_find_all_pro_rej_available
from routers.base_platform.crud import db_get_platform_cell_data, db_update_platform_cell_data
from routers.wheels.crud import db_find_wheel_by_object_id
from constants import (PRES_TYPE_GRID, PRES_TYPE_PLATFORM,
                       DB_PMK_NAME, CLN_GRID, CLN_BATCH_NUMBERS,
                       CLN_WHEELSTACKS, CLN_BASE_PLATFORM,
                       ORDER_STATUS_PENDING, CLN_ACTIVE_ORDERS, ORDER_MOVE_WHOLE_STACK,
                       EE_HAND_CRANE, EE_GRID_ROW_NAME, CLN_WHEELS, EE_LABORATORY,
                       ORDER_MOVE_TO_LABORATORY, ORDER_MOVE_TO_PROCESSING, ORDER_MOVE_TO_REJECTED, MSG_TESTS_NOT_DONE,
                       MSG_TESTS_FAILED, ORDER_MOVE_TO_STORAGE, CLN_STORAGES, PS_STORAGE, PS_GRID, PS_BASE_PLATFORM)
from routers.batch_numbers.crud import db_find_batch_number


# TODO: rebuild this garbage after everything
#  Literal CtrlC+V fiesta, but doesnt have time to make them universal.
#  Just brute_forcing...

async def orders_create_move_whole_wheelstack(db: AsyncIOMotorClient, order_data: dict) -> ObjectId:
    # SOURCE:
    #  1. SourcePlacement EXISTS
    #  2. SourceCell not `blocked` and not `blockedBy`
    #  3. SourceCell CONTAINS `wheelStack` and `wheelStack` EXIST, and it's not `blocked`
    # +++ Source
    source_id: ObjectId = await get_object_id(order_data['source']['placementId'])
    source_row: str = order_data['source']['rowPlacement']
    source_col: str = order_data['source']['columnPlacement']
    source_type: str = order_data['source']['placementType']
    source_cell_data = None
    if PRES_TYPE_GRID == source_type:
        source_cell_data = await db_get_grid_cell_data(
            source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID
        )
    # -1-
    elif PRES_TYPE_PLATFORM == source_type:
        source_cell_data = await db_get_platform_cell_data(
            source_id, source_row, source_col, db, DB_PMK_NAME, CLN_BASE_PLATFORM
        )
    if source_cell_data is None:
        raise HTTPException(
            detail=f'Source cell or placement doesnt exist. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    # -2-
    if source_cell_data['blocked'] or source_cell_data['blockedBy'] is not None:
        raise HTTPException(
            detail=f'Source cell `blocked`. Placed order {source_cell_data['blockedBy']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # -3-
    wheelstack_id: ObjectId = source_cell_data['wheelStack']
    if wheelstack_id is None:
        raise HTTPException(
            detail=f'Source cell doesnt contain any `wheelStack` on it. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_wheelstack_data = await db_find_wheelstack_by_object_id(wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
    # TODO: Think about extra clearing of dependencies.
    #  Because, what if we have corrupted cell with placed `wheelStack` and it's got deleted?
    #  Then we need to clear cell from any order placed on it and clear all dependencies,
    #   inside of this order (`affectedWheels` etc.)
    #  For now, we assume that we're not going to get any corruption,
    #   and we're using it by ourselves == correctly.
    if source_wheelstack_data is None:
        logger.error(f"Corrupted cell: row = {source_row}, col = {source_col}"
                     f" in a {PRES_TYPE_GRID} with `objectId` = {source_id}."
                     f" There's non-existing `wheelStack` placed on it = {wheelstack_id}")
        raise HTTPException(
            detail=f'Corrupted cell: row = {source_row}, col = {source_col}, inform someone to fix it',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if source_wheelstack_data['blocked']:
        logger.error(f"Corrupted cell: row = {source_row}, col = {source_col}"
                     f" in a {PRES_TYPE_GRID} with `objectId` = {source_id}."
                     f" There's `blocked` `wheelStack` placed on it = {wheelstack_id}."
                     f" And cell is marked as free.")
        raise HTTPException(
            detail=f'Source cell `wheelStack` blocked by other order = {source_wheelstack_data['lastOrder']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # Source ---
    # DESTINATION:
    #  1. DestinationPlacement EXISTS
    #  2. If DESTINATION cell is EMPTY and not `blocked`
    # +++ Destination
    destination_id: ObjectId = await get_object_id(order_data['destination']['placementId'])
    # We allow only moving from `basePlatform` -> `grid` or inside the `grid` => only 1 type `grid`.
    destination_row: str = order_data['destination']['rowPlacement']
    destination_col: str = order_data['destination']['columnPlacement']
    # -1-
    destination_cell_data = await db_get_grid_cell_data(
        destination_id, destination_row, destination_col, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        raise HTTPException(
            detail=f'Destination cell or placement doesnt exist. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_cell_data = destination_cell_data['rows'][destination_row]['columns'][destination_col]
    if (destination_cell_data['blocked']
            or destination_cell_data['blockedBy'] is not None
            or destination_cell_data['wheelStack'] is not None):
        raise HTTPException(
            detail=f'Destination cell is `blocked`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # Destination ---
    # +++ Order Creation
    creation_time = await time_w_timezone()
    order_data['source']['placementId'] = source_id
    order_data['destination']['placementId'] = destination_id
    order_data['createdAt'] = creation_time
    order_data['lastUpdated'] = creation_time
    order_data['affectedWheelStacks'] = {
        'source': source_wheelstack_data['_id'],
        'destination': None,
    }
    order_data['affectedWheels'] = {
        'source': source_wheelstack_data['wheels'],
        'destination': [],
    }
    order_data['status'] = ORDER_STATUS_PENDING
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            created_order = await db_create_order(order_data, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session)
            created_order_id: ObjectId = created_order.inserted_id
            # Order Creation ---
            # We need to change in SOURCE:
            #  1. SourceCell should be `blocked` and `order` `objectId` placed in `blockedBy`.
            #  2. SourceWheelstack should be `blocked` and `order` `objectId` placed in `blockedBy`
            # +++ Source Change
            new_source_cell_data = {
                'wheelStack': source_wheelstack_data['_id'],
                'blocked': True,
                'blockedBy': created_order_id,
            }
            if PRES_TYPE_GRID == source_type:
                await db_update_grid_cell_data(
                    source_id, source_row, source_col, new_source_cell_data,
                    db, DB_PMK_NAME, CLN_GRID, session, False
                )
            elif PRES_TYPE_PLATFORM == source_type:
                await db_update_platform_cell_data(
                    source_id, source_row, source_col, new_source_cell_data,
                    db, DB_PMK_NAME, CLN_BASE_PLATFORM, session, True
                )
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['lastOrder'] = created_order_id
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            # Source Change ---
            # We need to change in DESTINATION:
            #  1. DestinationCell should be `blocked` and `order` `objectId` placed in `blockedBy`.
            # +++ Destination change
            destination_cell_data['wheelStack'] = None
            destination_cell_data['blocked'] = True
            destination_cell_data['blockedBy'] = created_order_id
            await db_update_grid_cell_data(
                destination_id, destination_row, destination_col,
                destination_cell_data, db, DB_PMK_NAME, CLN_GRID, session, True
            )
            # Destination change ---
            return created_order_id


async def orders_create_move_to_laboratory(db: AsyncIOMotorClient, order_data: dict) -> ObjectId:
    # SOURCE:
    #  Source can only be a `grid`, because we can't move it from `basePlatform`.
    #  Or any of the `extra` elements.
    #  1. Check for a correct SOURCE TYPE and its EXISTENCE
    #  2. Check for a WheelStack to be present on this cell.
    #  3. Check if it's not `blocked` <- `wheelStack` and cell itself.
    #  4. Check if `chosenWheel` exists and placed in our `wheelStack`.
    # -1-
    if PRES_TYPE_GRID != order_data['source']['placementType']:
        raise HTTPException(
            detail=f'We can only {ORDER_MOVE_TO_LABORATORY} from a `grid`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    source_id = await get_object_id(order_data['source']['placementId'])
    source_row = order_data['source']['rowPlacement']
    source_col = order_data['source']['columnPlacement']
    source_cell_data = await db_get_grid_cell_data(source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID)
    if source_cell_data is None:
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell doesnt exist in the `grid` = {source_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    if source_cell_data['blocked'] or source_cell_data['blockedBy'] is not None:
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell already `blocked` by = {source_cell_data['blockedBy']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # -2-
    wheelstack_id: ObjectId = source_cell_data['wheelStack']
    if wheelstack_id is None:
        raise HTTPException(
            detail=f'Source cell doesnt contain any `wheelStack` on it. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        logger.error(f"Corrupted cell: row = {source_row}, col = {source_col}"
                     f" in a {PRES_TYPE_GRID} with `objectId` = {source_id}."
                     f" There's non-existing `wheelStack` placed on it = {wheelstack_id}")
        raise HTTPException(
            detail=f'Corrupted cell: row = {source_row}, col = {source_col}, inform someone to fix it',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # -3-
    if source_wheelstack_data['blocked']:
        logger.error(f"Corrupted cell: row = {source_row}, col = {source_col}"
                     f" in a {PRES_TYPE_GRID} with `objectId` = {source_id}."
                     f" There's `blocked` `wheelStack` placed on it = {wheelstack_id}."
                     f" And cell is marked as free.")
        raise HTTPException(
            detail=f'Source cell `wheelStack` blocked by other order = {source_wheelstack_data['lastOrder']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    chosen_wheel_id = await get_object_id(order_data['chosenWheel'])
    chosen_wheel_data = await db_find_wheel_by_object_id(
        chosen_wheel_id, db, DB_PMK_NAME, CLN_WHEELS
    )
    if chosen_wheel_data is None:
        raise HTTPException(
            detail=f'`chosenWheel` doesnt exist. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if chosen_wheel_data['wheelStack'] is None:
        raise HTTPException(
            detail=f'`chosenWheel` doesnt have `wheelStack` attached',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    if chosen_wheel_data['wheelStack']['wheelStackId'] != source_wheelstack_data['_id']:
        raise HTTPException(
            detail=f'`chosenWheel` not inside a `wheelStack`on pointed source cell {source_row}|{source_col}',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    # DESTINATION:
    #  Destination can only be one of the `extra` elements of the `grid`.
    #  `rowPlacement` == `extra`, `columnPlacement` == name of the `extra` element
    #  1. Check for a correct DESTINATION and its EXISTENCE
    #  2. Check for it not being `blocked` and being of correct type `EE_HAND_CRANE`.
    destination_id = await get_object_id(order_data['destination']['placementId'])
    extra_name = order_data['destination']['elementName']
    destination_cell_data = await db_get_grid_extra_cell_data(
        destination_id, extra_name, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        raise HTTPException(
            detail=f'Extra element = {extra_name} doesnt exist in the `grid` = {destination_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_cell_data = destination_cell_data['extra'][extra_name]
    if destination_cell_data['blocked']:
        raise HTTPException(
            detail=f'Extra element = {extra_name} extra element is `blocked`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if destination_cell_data['type'] != EE_LABORATORY:
        raise HTTPException(
            detail=f'Extra element = {extra_name} chosen extra element of incorrect type.',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    # +++ Order Creation
    creation_time = await time_w_timezone()
    order_data['source']['placementId'] = source_id
    cor_data = {
        'orderName': order_data['orderName'],
        'orderDescription': order_data['orderDescription'],
        'createdAt': creation_time,
        'lastUpdated': creation_time,
        'source': order_data['source'],
        'destination': {
            'placementType': order_data['destination']['placementType'],
            'placementId': destination_id,
            'rowPlacement': EE_GRID_ROW_NAME,
            'columnPlacement': order_data['destination']['elementName'],
        },
        'affectedWheelStacks': {
            'source': source_wheelstack_data['_id'],
            'destination': None,
        },
        'affectedWheels': {
            'source': [chosen_wheel_id],
            'destination': [],
        },
        'status': ORDER_STATUS_PENDING,
        'orderType': ORDER_MOVE_TO_LABORATORY,
    }
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            created_order = await db_create_order(cor_data, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session)
            created_order_id = created_order.inserted_id
            # Order Creation ---
            # SOURCE change:
            #  1. Block SOURCE cell and `wheelStack` on it.
            new_source_cell_data = {
                'wheelStack': source_wheelstack_data['_id'],
                'blocked': True,
                'blockedBy': created_order_id,
            }
            await db_update_grid_cell_data(
                source_id, source_row, source_col, new_source_cell_data,
                db, DB_PMK_NAME, CLN_GRID, session, False
            )
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['lastOrder'] = created_order_id
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            # DESTINATION change:
            #  1. ADD created order into `extra` element `orders`.
            await db_append_extra_cell_order(
                destination_id, extra_name, created_order_id,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            return created_order_id


async def orders_create_move_to_processing(db: AsyncIOMotorClient, order_data: dict) -> ObjectId:
    # SOURCE:
    #  Source can only be a `grid`, because we can't move it from `basePlatform`.
    #  Or any of the `extra` elements.
    #  1. Check for a correct SOURCE TYPE and its EXISTENCE
    #  2. Check for a WheelStack to be present on this cell.
    #  3. Check if it's not `blocked` <- `wheelStack` and cell itself.
    # -1-
    if PRES_TYPE_GRID != order_data['source']['placementType']:
        raise HTTPException(
            detail=f'We can only {ORDER_MOVE_TO_PROCESSING} from a `grid`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    source_id = await get_object_id(order_data['source']['placementId'])
    source_row = order_data['source']['rowPlacement']
    source_col = order_data['source']['columnPlacement']
    source_cell_data = await db_get_grid_cell_data(source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID)
    if source_cell_data is None:
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell doesnt exist in the `grid` = {source_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    if source_cell_data['blocked'] or source_cell_data['blockedBy'] is not None:
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell already `blocked` by = {source_cell_data['blockedBy']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # -2-
    wheelstack_id: ObjectId = source_cell_data['wheelStack']
    if wheelstack_id is None:
        raise HTTPException(
            detail=f'Source cell doesnt contain any `wheelStack` on it. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        logger.error(f"Corrupted cell: row = {source_row}, col = {source_col}"
                     f" in a {PRES_TYPE_GRID} with `objectId` = {source_id}."
                     f" There's non-existing `wheelStack` placed on it = {wheelstack_id}")
        raise HTTPException(
            detail=f'Corrupted cell: row = {source_row}, col = {source_col}, inform someone to fix it',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if source_wheelstack_data['blocked']:
        logger.error(f"Corrupted cell: row = {source_row}, col = {source_col}"
                     f" in a {PRES_TYPE_GRID} with `objectId` = {source_id}."
                     f" There's `blocked` `wheelStack` placed on it = {wheelstack_id}."
                     f" And cell is marked as free.")
        raise HTTPException(
            detail=f'Source cell `wheelStack` blocked by other order = {source_wheelstack_data['lastOrder']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    batch_number_data = await db_find_batch_number(
        source_wheelstack_data['batchNumber'], db, DB_PMK_NAME, CLN_BATCH_NUMBERS
    )
    if batch_number_data is None:
        logger.error(f'Attempt to use non existing `batchNumber` = {source_wheelstack_data['batchNumber']}')
        raise HTTPException(
            detail=f'Provided `batchNumber` doesnt exist',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if not batch_number_data['laboratoryTestDate']:
        logger.error(f'Attempt to move not tested `wheelstack` = {source_wheelstack_data['_id']}')
        raise HTTPException(
            detail=MSG_TESTS_NOT_DONE,
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if not batch_number_data['laboratoryPassed']:
        logger.error(f'Attempt to move test failed `wheelstack` = {source_wheelstack_data['_id']}')
        raise HTTPException(
            detail=MSG_TESTS_FAILED,
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # DESTINATION:
    #  Destination can only be one of the `extra` elements of the `grid`.
    #  `rowPlacement` == `extra`, `columnPlacement` == name of the `extra` element
    #  1. Check for a correct DESTINATION and its EXISTENCE
    #  2. Check for it not being `blocked` and being of correct type `EE_HAND_CRANE`.
    destination_id = await get_object_id(order_data['destination']['placementId'])
    extra_name = order_data['destination']['elementName']
    destination_cell_data = await db_get_grid_extra_cell_data(
        destination_id, extra_name, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        raise HTTPException(
            detail=f'Extra element = {extra_name} doesnt exist in the `grid` = {destination_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_cell_data = destination_cell_data['extra'][extra_name]
    if destination_cell_data['blocked']:
        raise HTTPException(
            detail=f'Extra element = {extra_name} extra element is `blocked`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if destination_cell_data['type'] != EE_HAND_CRANE:
        raise HTTPException(
            detail=f'Extra element = {extra_name} chosen extra element of incorrect type.',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    # +++ Order creation
    creation_time = await time_w_timezone()
    order_data['source']['placementId'] = source_id
    cor_data = {
        'orderName': order_data['orderName'],
        'orderDescription': order_data['orderDescription'],
        'createdAt': creation_time,
        'lastUpdated': creation_time,
        'source': order_data['source'],
        'destination': {
            'placementType': order_data['destination']['placementType'],
            'placementId': destination_id,
            'rowPlacement': EE_GRID_ROW_NAME,
            'columnPlacement': order_data['destination']['elementName'],
        },
        'affectedWheelStacks': {
            'source': source_wheelstack_data['_id'],
            'destination': None,
        },
        'affectedWheels': {
            'source': source_wheelstack_data['wheels'],
            'destination': [],
        },
        'status': ORDER_STATUS_PENDING,
        'orderType': ORDER_MOVE_TO_PROCESSING,
    }
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            created_order = await db_create_order(cor_data, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session)
            created_order_id = created_order.inserted_id
            # Order creation ---
            # SOURCE change:
            #  1. Block SOURCE cell and `wheelStack` on it.
            new_source_cell_data = {
                'wheelStack': source_wheelstack_data['_id'],
                'blocked': True,
                'blockedBy': created_order_id,
            }
            await db_update_grid_cell_data(
                source_id, source_row, source_col, new_source_cell_data,
                db, DB_PMK_NAME, CLN_GRID, session, False
            )
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['lastOrder'] = created_order_id
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            # DESTINATION change:
            #  1. ADD created order into `extra` element `orders`.
            await db_append_extra_cell_order(
                destination_id, extra_name, created_order_id,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            return created_order_id


# BULK PROCESS/REJECT
async def process_wheelstack(
        db, wheelstack_data, order_req_data,
        destination_id, destination_element_name, session
):
    cur_wheelstack_id = wheelstack_data['_id']
    cur_wheelstack_row = wheelstack_data['rowPlacement']
    cur_wheelstack_col = wheelstack_data['colPlacement']
    cur_wheelstack_placement_id = wheelstack_data['placement']['placementId']
    cur_wheelstack_placement_type = wheelstack_data['placement']['type']
    creation_time = await time_w_timezone()
    order_data = {
        'orderName': order_req_data['orderName'],
        'orderDescription': order_req_data['orderDescription'],
        'createdAt': creation_time,
        'lastUpdated': creation_time,
        'source': {
            'placementType': cur_wheelstack_placement_type,
            'placementId': cur_wheelstack_placement_id,
            'rowPlacement': cur_wheelstack_row,
            'columnPlacement': cur_wheelstack_col,
        },
        'destination': {
            'placementType': PS_GRID,
            'placementId': destination_id,
            'rowPlacement': EE_GRID_ROW_NAME,
            'columnPlacement': destination_element_name,
        },
        'affectedWheelStacks': {
            'source': wheelstack_data['_id'],
            'destination': None,
        },
        'affectedWheels': {
            'source': wheelstack_data['wheels'],
            'destination': [],
        },
        'status': ORDER_STATUS_PENDING,
        'orderType': order_req_data['orderType'],
    }

    created_order = await db_create_order(order_data, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session)
    created_order_id = created_order.inserted_id
    # Order creation ---
    # SOURCE change:
    #  1. Block SOURCE cell and `wheelStack` on it.
    new_source_cell_data = {
        'wheelStack': cur_wheelstack_id,
        'blocked': True,
        'blockedBy': created_order_id,
    }
    wheelstack_data['blocked'] = True
    wheelstack_data['lastOrder'] = created_order_id
    await db_update_wheelstack(
        wheelstack_data, wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
    )
    # DESTINATION change:
    #  1. ADD created order into `extra` element `orders`.
    result = {
        'sourceType': cur_wheelstack_placement_type,
        'sourceId': cur_wheelstack_placement_id,
        'orderId': created_order_id,
        'sourceRow': cur_wheelstack_row,
        'sourceCol': cur_wheelstack_col,
        'newSourceCellData': new_source_cell_data,
    }
    return result


async def orders_create_bulk_move_to_pro_rej_orders(
        from_everywhere: bool, order_req_data: dict, db: AsyncIOMotorClient
):
    batch_number = order_req_data['batchNumber']
    batch_number_data = await db_find_batch_number(batch_number, db, DB_PMK_NAME, CLN_BATCH_NUMBERS)
    if batch_number_data is None:
        raise HTTPException(
            detail=f'Provided `batchNumber` = {batch_number}. Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if not batch_number_data['laboratoryTestDate']:
        logger.error(f'Attempt to use not tested `batchNumber` = {batch_number}')
        raise HTTPException(
            detail=MSG_TESTS_NOT_DONE,
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if (order_req_data['orderType'] == ORDER_MOVE_TO_PROCESSING
            and not batch_number_data['laboratoryPassed']):
        logger.error(f'Attempt to use tests failed `batchNumber` = {batch_number}')
        raise HTTPException(
            detail=MSG_TESTS_FAILED,
            status_code=status.HTTP_403_FORBIDDEN,
        )
    async_tasks = []
    # TODO: Maybe add custom types on request.
    #  But for now, we can take only from GRID and STORAGE's.
    available_statuses: list[str] = [PS_STORAGE, PS_GRID]
    if from_everywhere:
        all_available = await db_find_all_pro_rej_available(
            batch_number, available_statuses, db, DB_PMK_NAME, CLN_WHEELSTACKS
        )
    else:
        source_placement_id = order_req_data['placementId']
        source_placement_type = order_req_data['placementType']
        source_placement_object_id = await get_object_id(source_placement_id)
        all_available = await db_find_all_pro_rej_available_in_placement(
            batch_number, source_placement_object_id, source_placement_type, db, DB_PMK_NAME, CLN_WHEELSTACKS
        )
    destination_id = await get_object_id(order_req_data['destination']['placementId'])
    destination_element_name = order_req_data['destination']['elementName']
    destination_exist = await get_grid_preset_by_object_id(
        destination_id, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_exist is None:
        logger.error(f'Attempt to use not existing placement = {destination_id}')
        raise HTTPException(
            detail='Provided `destinationId`. Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            for wheelstack_data in all_available:
                task = process_wheelstack(
                    db, wheelstack_data, order_req_data,
                    destination_id, destination_element_name, session
                )
                async_tasks.append(task)
            results = await asyncio.gather(*async_tasks)
            orders = [result['orderId'] for result in results]
            await db_append_extra_cell_orders(
                destination_id, destination_element_name, orders,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            grid_cells_to_update = {}
            for result in results:
                if PS_GRID == result['sourceType']:
                    if result['sourceId'] not in grid_cells_to_update:
                        grid_cells_to_update[result['sourceId']] = [result]
                    else:
                        grid_cells_to_update[result['sourceId']].append(result)
            for source_id, results_data in grid_cells_to_update.items():
                await db_update_grid_cells_data(
                    source_id, results_data, db, DB_PMK_NAME, CLN_GRID, session, record_change=True,
                )
            return orders
# ---


async def orders_create_move_to_rejected(db: AsyncIOMotorClient, order_data: dict) -> ObjectId:
    # SOURCE:
    #  Source can only be a `grid`, because we can't move it from `basePlatform`.
    #  Or any of the `extra` elements.
    #  1. Check for a correct SOURCE TYPE and its EXISTENCE
    #  2. Check for a WheelStack to be present on this cell.
    #  3. Check if it's not `blocked` <- `wheelStack` and cell itself.
    # -1-
    if PRES_TYPE_GRID != order_data['source']['placementType']:
        raise HTTPException(
            detail=f'We can only {ORDER_MOVE_TO_REJECTED} from a `grid`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    source_id = await get_object_id(order_data['source']['placementId'])
    source_row = order_data['source']['rowPlacement']
    source_col = order_data['source']['columnPlacement']
    source_cell_data = await db_get_grid_cell_data(source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID)
    if source_cell_data is None:
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell doesnt exist in the `grid` = {source_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    if source_cell_data['blocked'] or source_cell_data['blockedBy'] is not None:
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell already `blocked` by = {source_cell_data['blockedBy']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # -2-
    wheelstack_id: ObjectId = source_cell_data['wheelStack']
    if wheelstack_id is None:
        raise HTTPException(
            detail=f'Source cell doesnt contain any `wheelStack` on it. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        logger.error(f"Corrupted cell: row = {source_row}, col = {source_col}"
                     f" in a {PRES_TYPE_GRID} with `objectId` = {source_id}."
                     f" There's non-existing `wheelStack` placed on it = {wheelstack_id}")
        raise HTTPException(
            detail=f'Corrupted cell: row = {source_row}, col = {source_col}, inform someone to fix it',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if source_wheelstack_data['blocked']:
        logger.error(f"Corrupted cell: row = {source_row}, col = {source_col}"
                     f" in a {PRES_TYPE_GRID} with `objectId` = {source_id}."
                     f" There's `blocked` `wheelStack` placed on it = {wheelstack_id}."
                     f" And cell is marked as free.")
        raise HTTPException(
            detail=f'Source cell `wheelStack` blocked by other order = {source_wheelstack_data['lastOrder']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    batch_number_data = await db_find_batch_number(
        source_wheelstack_data['batchNumber'], db, DB_PMK_NAME, CLN_BATCH_NUMBERS
    )
    if batch_number_data is None:
        logger.error(f'Attempt to use non existing `batchNumber` = {source_wheelstack_data['batchNumber']}')
        raise HTTPException(
            detail=f'Provided `batchNumber` doesnt exist',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if not batch_number_data['laboratoryTestDate']:
        logger.error(f'Attempt to move not tested `wheelstack` = {source_wheelstack_data['_id']}')
        raise HTTPException(
            detail=MSG_TESTS_NOT_DONE,
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # DESTINATION:
    #  Destination can only be one of the `extra` elements of the `grid`.
    #  `rowPlacement` == `extra`, `columnPlacement` == name of the `extra` element
    #  1. Check for a correct DESTINATION and its EXISTENCE
    #  2. Check for it not being `blocked` and being of correct type `EE_HAND_CRANE`.
    destination_id = await get_object_id(order_data['destination']['placementId'])
    extra_name = order_data['destination']['elementName']
    destination_cell_data = await db_get_grid_extra_cell_data(
        destination_id, extra_name, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        raise HTTPException(
            detail=f'Extra element = {extra_name} doesnt exist in the `grid` = {destination_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_cell_data = destination_cell_data['extra'][extra_name]
    if destination_cell_data['blocked']:
        raise HTTPException(
            detail=f'Extra element = {extra_name} extra element is `blocked`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if destination_cell_data['type'] != EE_HAND_CRANE:
        raise HTTPException(
            detail=f'Extra element = {extra_name} chosen extra element of incorrect type.',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    # +++ Order creation
    creation_time = await time_w_timezone()
    order_data['source']['placementId'] = source_id
    cor_data = {
        'orderName': order_data['orderName'],
        'orderDescription': order_data['orderDescription'],
        'createdAt': creation_time,
        'lastUpdated': creation_time,
        'source': order_data['source'],
        'destination': {
            'placementType': order_data['destination']['placementType'],
            'placementId': destination_id,
            'rowPlacement': EE_GRID_ROW_NAME,
            'columnPlacement': order_data['destination']['elementName'],
        },
        'affectedWheelStacks': {
            'source': source_wheelstack_data['_id'],
            'destination': None,
        },
        'affectedWheels': {
            'source': source_wheelstack_data['wheels'],
            'destination': [],
        },
        'status': ORDER_STATUS_PENDING,
        'orderType': ORDER_MOVE_TO_REJECTED,
    }
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            created_order = await db_create_order(cor_data, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session)
            created_order_id = created_order.inserted_id
            # Order creation ---
            # SOURCE change:
            #  1. Block SOURCE cell and `wheelStack` on it.
            new_source_cell_data = {
                'wheelStack': source_wheelstack_data['_id'],
                'blocked': True,
                'blockedBy': created_order_id,
            }
            await db_update_grid_cell_data(
                source_id, source_row, source_col, new_source_cell_data,
                db, DB_PMK_NAME, CLN_GRID, session, False
            )
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['lastOrder'] = created_order_id
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            # DESTINATION change:
            #  1. ADD created order into `extra` element `orders`.
            await db_append_extra_cell_order(
                destination_id, extra_name, created_order_id,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            return created_order_id


async def orders_create_move_to_storage(db: AsyncIOMotorClient, order_data: dict) -> ObjectId:
    chosen_placement: str = order_data['source']['placementType']
    attempt_string: str = await orders_creation_attempt_string(ORDER_MOVE_TO_STORAGE)
    source_id: ObjectId = await get_object_id(order_data['source']['placementId'])
    source_row: str = order_data['source']['rowPlacement']
    source_col: str = order_data['source']['columnPlacement']
    source_cell_data = None
    if PS_GRID == chosen_placement:
        source_cell_data = await db_get_grid_cell_data(
            source_id, source_row, source_col, db, DB_PMK_NAME, CLN_GRID
        )
    elif PS_BASE_PLATFORM == chosen_placement:
        source_cell_data = await db_get_platform_cell_data(
            source_id, source_row, source_col, db, DB_PMK_NAME, CLN_BASE_PLATFORM
        )
    if source_cell_data is None:
        logger.warning(
            f'{attempt_string}'
            f'To move from non existing cell = {source_row}|{source_col}|{source_id}'
        )
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell doesnt exist in the `grid`',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_cell_data = source_cell_data['rows'][source_row]['columns'][source_col]
    if source_cell_data['blocked'] or source_cell_data['blockedBy'] is not None:
        logger.warning(
            f'{attempt_string}'
            f'To move from already blocked cell = {source_row}|{source_col}|{source_id}'
        )
        raise HTTPException(
            detail=f'{source_row}|{source_col} <- source cell already `blocked` by = {source_cell_data['blockedBy']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    wheelstack_id: ObjectId = source_cell_data['wheelStack']
    if wheelstack_id is None:
        logger.warning(
            f'{attempt_string}'
            f'Source cell exists,'
            f'  but doesnt contain `wheelstack`| cell = {source_row}|{source_col}|{source_id}'
        )
        raise HTTPException(
            detail='Source cell exists, but doesnt contain `wheelstack`',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS,
    )
    if source_wheelstack_data is None:
        logger.error(
            attempt_string + await orders_corrupted_cell_non_existing_wheelstack(
                source_row, source_col, chosen_placement, source_id, wheelstack_id
            )
        )
        raise HTTPException(
            detail=f'Corrupted cell {source_row}|{source_col}, inform someone to fix it.',
            status_code=status.HTTP_403_FORBIDDEN
        )
    if source_wheelstack_data['blocked']:
        logger.error(
            attempt_string + await orders_corrupted_cell_blocked_wheelstack(
                source_row, source_col, chosen_placement, source_id, wheelstack_id
            )
        )
        raise HTTPException(
            detail=f'Source cell `wheelstack` is blocked by other order = {source_wheelstack_data['lastOrder']}',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    batch_number_data = await db_find_batch_number(
        source_wheelstack_data['batchNumber'], db, DB_PMK_NAME, CLN_BATCH_NUMBERS,
    )
    if batch_number_data is None:
        logger.warning(
            f'{attempt_string}'
            f'With non existing `batchNumber` = {source_wheelstack_data['batchNumber']}'
        )
        raise HTTPException(
            detail=f'Provided `batchNumber`. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    storage_id = await get_object_id(order_data['storage'])
    storage_data = await db_get_storage_by_object_id(
        storage_id, False, db, DB_PMK_NAME, CLN_STORAGES
    )
    if storage_data is None:
        logger.warning(
            f'{attempt_string}'
            f'With non existing `storage` = {storage_id}'
        )
        raise HTTPException(
            detail=f'Provided `storage`. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    creation_time = await time_w_timezone()
    order_data['source']['placementId'] = source_id
    new_order_data = {
        'orderName': order_data['orderName'],
        'orderDescription': order_data['orderDescription'],
        'createdAt': creation_time,
        'lastUpdated': creation_time,
        'source': order_data['source'],
        'destination': {
            'placementType': PS_STORAGE,
            'placementId': storage_id,
            'rowPlacement': '0',
            'columnPlacement': '0',
        },
        'affectedWheelStacks': {
            'source': source_wheelstack_data['_id'],
            'destination': None,
        },
        'affectedWheels': {
            'source': source_wheelstack_data['wheels'],
            'destination': [],
        },
        'status': ORDER_STATUS_PENDING,
        'orderType': ORDER_MOVE_TO_STORAGE,
    }
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            created_order = await db_create_order(
                new_order_data, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            created_order_id = created_order.inserted_id
            new_source_cell_data = {
                'wheelStack': source_wheelstack_data['_id'],
                'blocked': True,
                'blockedBy': created_order_id,
            }
            if PS_GRID == chosen_placement:
                await db_update_grid_cell_data(
                    source_id, source_row, source_col, new_source_cell_data,
                    db, DB_PMK_NAME, CLN_GRID, session, True,
                )
            elif PS_BASE_PLATFORM == chosen_placement:
                await db_update_platform_cell_data(
                    source_id, source_row, source_col, new_source_cell_data,
                    db, DB_PMK_NAME, CLN_BASE_PLATFORM, session, True,
                )
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['lastOrder'] = created_order_id
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'],
                db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            return created_order_id


async def orders_create_move_from_storage_whole_stack(
        db: AsyncIOMotorClient,
        order_data: dict,
) -> ObjectId:
    source_wheelstack_id: ObjectId = await get_object_id(order_data['source']['wheelstackId'])
    wheelstack_data = await db_find_wheelstack_by_object_id(
        source_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if wheelstack_data is None:
        raise HTTPException(
            detail=f'`wheelstack` = {source_wheelstack_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if wheelstack_data['blocked']:
        raise HTTPException(
            detail=f'`wheelstack` is already blocked',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    storage_id: ObjectId = await get_object_id(order_data['source']['storageId'])
    source_present = await db_storage_check_placed_wheelstack(
        storage_id, source_wheelstack_id, wheelstack_data['batchNumber'],
        db, DB_PMK_NAME, CLN_STORAGES,
    )
    if source_present is None:
        raise HTTPException(
            detail=f'`wheelstack` exists but not placed in the `storage` = {storage_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_id = await get_object_id(order_data['destination']['placementId'])
    destination_row = order_data['destination']['rowPlacement']
    destination_col = order_data['destination']['columnPlacement']
    destination_cell_data = await db_get_grid_cell_data(
        destination_id, destination_row, destination_col,
        db, DB_PMK_NAME, CLN_GRID
    )
    if destination_cell_data is None:
        raise HTTPException(
            detail=f'Destination cell or placement doesnt exist. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_cell_data = destination_cell_data['rows'][destination_row]['columns'][destination_col]
    if (destination_cell_data['blocked']
            or destination_cell_data['blockedBy'] is not None
            or destination_cell_data['wheelStack'] is not None):
        raise HTTPException(
            detail=f'Destination cell is `blocked`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    creation_time = await time_w_timezone()
    new_order_data = {
        'orderName': order_data['orderName'],
        'orderDescription': order_data['orderDescription'],
        'source': {
            'placementType': PS_STORAGE,
            'placementId': storage_id,
            'rowPlacement': '0',
            'columnPlacement': '0',
        },
        'destination': {
            'placementType': PRES_TYPE_GRID,
            'placementId': destination_id,
            'rowPlacement': destination_row,
            'columnPlacement': destination_col,
        },
        'orderType': ORDER_MOVE_WHOLE_STACK,
        'createdAt': creation_time,
        'lastUpdated': creation_time,
        'affectedWheelStacks': {
            'source': wheelstack_data['_id'],
            'destination': None,
        },
        'affectedWheels': {
            'source': wheelstack_data['wheels'],
            'destination': []
        },
        'status': ORDER_STATUS_PENDING,
    }
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            created_order = await db_create_order(
                new_order_data, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            created_order_id = created_order.inserted_id
            # Block wheelstack in STORAGE
            wheelstack_data['blocked'] = True
            wheelstack_data['lastOrder'] = created_order_id
            await db_update_wheelstack(
                wheelstack_data, wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            # Block target cell in GRID
            destination_cell_data['wheelStack'] = None
            destination_cell_data['blocked'] = True
            destination_cell_data['blockedBy'] = created_order_id
            await db_update_grid_cell_data(
                destination_id, destination_row, destination_col,
                destination_cell_data, db, DB_PMK_NAME, CLN_GRID, session, True
            )
            return created_order_id


async def orders_create_move_to_pro_rej_from_storage(
        db: AsyncIOMotorClient,
        order_data: dict,
        processing: bool = True,
) -> ObjectId:
    source_wheelstack_id: ObjectId = await get_object_id(order_data['source']['wheelstackId'])
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        raise HTTPException(
            detail=f'`wheelstack` = {source_wheelstack_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if source_wheelstack_data['blocked']:
        raise HTTPException(
            detail=f'`wheelstack` is already blocked',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    storage_id: ObjectId = await get_object_id(order_data['source']['storageId'])
    source_present = await db_storage_check_placed_wheelstack(
        storage_id, source_wheelstack_id, source_wheelstack_data['batchNumber'],
        db, DB_PMK_NAME, CLN_STORAGES,
    )
    if source_present is None:
        raise HTTPException(
            detail=f'`wheelstack` exists but not placed in the `storage` = {storage_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_id = await get_object_id(order_data['destination']['placementId'])
    # row == extra
    destination_row = order_data['destination']['rowPlacement']
    # col == extraElement name
    destination_col = order_data['destination']['columnPlacement']
    destination_extra_element_data = await db_get_grid_extra_cell_data(
        destination_id, destination_col,
        db, DB_PMK_NAME, CLN_GRID
    )
    if destination_extra_element_data is None:
        raise HTTPException(
            detail=f'Destination cell or placement doesnt exist. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_extra_element_data = destination_extra_element_data['extra'][destination_col]
    if destination_extra_element_data['blocked']:
        raise HTTPException(
            detail=f'Destination element is `blocked`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    batch_number_data = await db_find_batch_number(
        source_wheelstack_data['batchNumber'], db, DB_PMK_NAME, CLN_BATCH_NUMBERS,
    )
    if batch_number_data['laboratoryTestDate'] is None:
        raise HTTPException(
            detail=MSG_TESTS_NOT_DONE,
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if processing and not batch_number_data['laboratoryPassed']:
        raise HTTPException(
            detail=MSG_TESTS_FAILED,
            status_code=status.HTTP_403_FORBIDDEN,
        )
    creation_time = await time_w_timezone()
    new_order_data = {
        'orderName': order_data['orderName'],
        'orderDescription': order_data['orderDescription'],
        'source': {
            'placementType': PS_STORAGE,
            'placementId': storage_id,
            'rowPlacement': '0',
            'columnPlacement': '0',
        },
        'destination': {
            'placementType': PRES_TYPE_GRID,
            'placementId': destination_id,
            'rowPlacement': destination_row,
            'columnPlacement': destination_col,
        },
        'createdAt': creation_time,
        'lastUpdated': creation_time,
        'affectedWheelStacks': {
            'source': source_wheelstack_data['_id'],
            'destination': None,
        },
        'affectedWheels': {
            'source': source_wheelstack_data['wheels'],
            'destination': [],
        },
        'status': ORDER_STATUS_PENDING,
    }
    if processing:
        new_order_data['orderType'] = ORDER_MOVE_TO_PROCESSING
    else:
        new_order_data['orderType'] = ORDER_MOVE_TO_REJECTED
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            # Create order
            created_order_id = await db_create_order(
                new_order_data, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session,
            )
            created_order_id = created_order_id.inserted_id
            # Block source wheelstack
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['lastOrder'] = created_order_id
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'],
                db, DB_PMK_NAME, CLN_WHEELSTACKS, session, True
            )
            # Place order in extraElement.orders update lastChange
            await db_append_extra_cell_order(
                destination_id, destination_col, created_order_id,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            return created_order_id


async def orders_create_move_from_storage_to_storage_whole_stack(
        db: AsyncIOMotorClient,
        order_data: dict,
) -> ObjectId:
    source_wheelstack_id: ObjectId = await get_object_id(order_data['source']['wheelstackId'])
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS,
    )
    if source_wheelstack_data is None:
        raise HTTPException(
            detail='source wheelstack Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if source_wheelstack_data['blocked']:
        raise HTTPException(
            detail=f'`wheelstack` is already blocked',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    source_storage_id: ObjectId = await get_object_id(order_data['source']['storageId'])
    source_present = await db_storage_check_placed_wheelstack(
        source_storage_id, source_wheelstack_id, source_wheelstack_data['batchNumber'],
        db, DB_PMK_NAME, CLN_STORAGES,
    )
    if source_present is None:
        raise HTTPException(
            detail=f'`wheelstack` exists but not placed in the `storage` = {source_storage_id}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_storage_id = await get_object_id(order_data['destination']['placementId'])
    destination_present = await db_get_storage_by_object_id(
        destination_storage_id, False, db, DB_PMK_NAME, CLN_STORAGES
    )
    if source_storage_id == destination_storage_id:
        raise HTTPException(
            detail='Already present in destination',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    if destination_present is None:
        raise HTTPException(
            detail=f'storage` = {destination_storage_id} Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    creation_time = await time_w_timezone()
    new_order_data = {
        'orderName': order_data['orderName'],
        'orderDescription': order_data['orderDescription'],
        'source': {
            'placementType': PS_STORAGE,
            'placementId': source_storage_id,
            'rowPlacement': '0',
            'columnPlacement': '0',
        },
        'destination': {
            'placementType': PS_STORAGE,
            'placementId': destination_storage_id,
            'rowPlacement': '0',
            'columnPlacement': '0',
        },
        'createdAt': creation_time,
        'lastUpdated': creation_time,
        'affectedWheelStacks': {
            'source': source_wheelstack_data['_id'],
            'destination': None,
        },
        'affectedWheels': {
            'source': source_wheelstack_data['wheels'],
            'destination': [],
        },
        'status': ORDER_STATUS_PENDING,
        'orderType': ORDER_MOVE_TO_STORAGE,
    }
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            created_order_id = await db_create_order(
                new_order_data, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session,
            )
            created_order_id = created_order_id.inserted_id
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['lastOrder'] = created_order_id
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'],
                db, DB_PMK_NAME, CLN_WHEELSTACKS, session, True
            )
            return created_order_id


async def orders_create_move_from_storage_to_lab(
        db: AsyncIOMotorClient,
        order_data: dict,
) -> ObjectId:
    source_storage_id: ObjectId = await get_object_id(order_data['source']['storageId'])
    source_storage_data = await db_get_storage_by_object_id(
        source_storage_id, False, db, DB_PMK_NAME, CLN_STORAGES
    )
    if source_storage_data is None:
        raise HTTPException(
            detail='source storage Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    destination_id: ObjectId = await get_object_id(order_data['destination']['placementId'])
    destination_extra_element = order_data['destination']['columnPlacement']
    destination_exists = await db_get_grid_extra_cell_data(
        destination_id, destination_extra_element, db, DB_PMK_NAME, CLN_GRID
    )
    if destination_exists is None:
        raise HTTPException(
            detail='destination extra cell Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    source_wheelstack_id: ObjectId = await get_object_id(order_data['source']['wheelstackId'])
    source_wheelstack_data = await db_find_wheelstack_by_object_id(
        source_wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if source_wheelstack_data is None:
        raise HTTPException(
            detail='source wheelstack Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    creation_time = await time_w_timezone()
    chosen_wheel_id: ObjectId = await get_object_id(order_data['chosenWheel'])
    if chosen_wheel_id not in source_wheelstack_data['wheels']:
        raise HTTPException(
            detail='source wheelstack doesnt contain `chosenWheel`',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    new_order = {
        'orderName': order_data['orderName'],
        'orderDescription': order_data['orderDescription'],
        'createdAt': creation_time,
        'lastUpdated': creation_time,
        'source': {
            'placementType': PS_STORAGE,
            'placementId': source_storage_id,
            'rowPlacement': '0',
            'columnPlacement': '0',
        },
        'destination': {
            'placementType': order_data['destination']['placementType'],
            'placementId': destination_id,
            'rowPlacement': EE_GRID_ROW_NAME,
            'columnPlacement': order_data['destination']['columnPlacement'],
        },
        'affectedWheelStacks': {
            'source': source_wheelstack_data['_id'],
            'destination': None,
        },
        'affectedWheels': {
            'source': [chosen_wheel_id],
            'destination': [],
        },
        'status': ORDER_STATUS_PENDING,
        'orderType': ORDER_MOVE_TO_LABORATORY,
    }
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            created_order = await db_create_order(
                new_order, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS, session
            )
            created_order_id = created_order.inserted_id
            source_wheelstack_data['blocked'] = True
            source_wheelstack_data['lastOrder'] = created_order_id
            await db_update_wheelstack(
                source_wheelstack_data, source_wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
            )
            await db_append_extra_cell_order(
                destination_id, destination_extra_element, created_order_id,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            return created_order_id
