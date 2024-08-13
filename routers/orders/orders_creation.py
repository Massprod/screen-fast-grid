from bson import ObjectId
from loguru import logger
from fastapi import HTTPException, status
from routers.orders.crud import db_create_order
from motor.motor_asyncio import AsyncIOMotorClient
from utility.utilities import get_object_id, time_w_timezone
from routers.grid.crud import (db_get_grid_cell_data, db_update_grid_cell_data,
                               db_get_grid_extra_cell_data, db_update_extra_cell_data)
from routers.wheelstacks.crud import db_find_wheelstack_by_object_id, db_update_wheelstack
from routers.base_platform.crud import db_get_platform_cell_data, db_update_platform_cell_data
from routers.wheels.crud import db_find_wheel_by_object_id
from constants import (PRES_TYPE_GRID, PRES_TYPE_PLATFORM,
                       DB_PMK_NAME, CLN_GRID, CLN_BATCH_NUMBERS,
                       CLN_WHEELSTACKS, CLN_BASE_PLATFORM,
                       ORDER_STATUS_PENDING, CLN_ACTIVE_ORDERS,
                       EE_HAND_CRANE, EE_GRID_ROW_NAME, CLN_WHEELS, EE_LABORATORY,
                       ORDER_MOVE_TO_LABORATORY, ORDER_MOVE_TO_PROCESSING, ORDER_MOVE_TO_REJECTED)
from routers.batch_numbers.crud import db_find_batch_number


async def orders_create_move_whole_wheelstack(db: AsyncIOMotorClient, order_data: dict) -> ObjectId:
    # SOURCE:
    #  1. SourcePlacement EXIST
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
    #  1. DestinationPlacement EXIST
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
    order_data['source']['placementId'] = source_id
    order_data['destination']['placementId'] = destination_id
    order_data['createdAt'] = await time_w_timezone()
    order_data['lastUpdated'] = await time_w_timezone()
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
            destination_cell_data['orders'][str(created_order_id)] = created_order_id
            await db_update_extra_cell_data(
                destination_id, extra_name, destination_cell_data,
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
    if not batch_number_data['laboratoryPassed']:
        logger.error(f'Attempt to move not tested `wheelstack` = {source_wheelstack_data['_id']}')
        raise HTTPException(
            detail="`wheelstack` didn't passed the tests",
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
            destination_cell_data['orders'][str(created_order_id)] = created_order_id
            await db_update_extra_cell_data(
                destination_id, extra_name, destination_cell_data, db, DB_PMK_NAME, CLN_GRID, session, True
            )
            return created_order_id


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
    if batch_number_data['laboratoryTestDate'] is None:
        raise HTTPException(
            detail='`wheelstack` not tested',
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
            destination_cell_data['orders'][str(created_order_id)] = created_order_id
            await db_update_extra_cell_data(
                destination_id, extra_name, destination_cell_data,
                db, DB_PMK_NAME, CLN_GRID, session, True
            )
            return created_order_id
