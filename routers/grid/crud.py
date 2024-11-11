import asyncio

from bson import ObjectId
from loguru import logger
from fastapi import status
from pymongo.errors import PyMongoError
from fastapi.exceptions import HTTPException
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from utility.utilities import get_db_collection, time_w_timezone, log_db_record, get_object_id, log_db_error_record


async def grid_make_json_friendly(grid_data: dict) -> dict:
    grid_data['_id'] = str(grid_data['_id'])
    grid_data['preset'] = str(grid_data['preset'])
    grid_data['createdAt'] = grid_data['createdAt'].isoformat()
    grid_data['lastChange'] = grid_data['lastChange'].isoformat()
    for row in grid_data['rows']:
        for column in grid_data['rows'][row]['columns']:
            field = grid_data['rows'][row]['columns'][column]
            if field['wheelStack'] is not None:
                field['wheelStack'] = str(field['wheelStack'])
            if field['blockedBy'] is not None:
                field['blockedBy'] = str(field['blockedBy'])
    if 'extra' in grid_data:
        for extra_element in grid_data['extra']:
            if 'orders' in grid_data['extra'][extra_element]:
                orders = grid_data['extra'][extra_element]['orders']
                for order in orders:
                    orders[order] = str(orders[order])
    return grid_data


async def get_all_grids_data(
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        res = await collection.find({}).to_list(length=None)
        return res
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_all_grids(
        include_data: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {}
    projection = {}
    if not include_data:
        projection = {
            'rowsOrder': 0,
            'rows': 0,
            'extra': 0,
        }
    try:
        res = await collection.find(query, projection).to_list(length=None)
        return res
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_grid_by_object_id(
        grid_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        grid = await collection.find_one({'_id': grid_object_id})
        return grid
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_grid_preset_by_object_id(
        grid_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        preset_id = await collection.find_one(
            {'_id': grid_object_id},
            {'preset': 1}
        )
        return preset_id
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_grid_by_name(
        grid_name: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        grid = await collection.find_one(
            {'name': grid_name},
        )
        return grid
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


# +++ CREATION
async def collect_wheelstack_cells(preset_data: dict) -> dict:
    empty_record = {
        'wheelStack': None,
        'blocked': False,
        'blockedBy': None,
    }
    new_rows_order = []
    new_rows = {}
    for row in preset_data['rows']:
        new_row = {}
        for col in preset_data['rows'][row]['columnsOrder']:
            cur_cell = preset_data['rows'][row]['columns'][col]
            if not cur_cell['wheelStack']:
                continue
            if 'columns' not in new_row:
                new_row['columns'] = {}
                new_row['columnsOrder'] = []
            new_row['columns'][col] = empty_record
            new_row['columnsOrder'].append(col)
        if new_row:
            new_rows[row] = new_row
            new_rows_order.append(row)
    preset_data['rowsOrder'] = new_rows_order
    preset_data['rows'] = new_rows
    return preset_data


async def create_grid(
        preset_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        assignedPlatforms: list[str] = [],
):
    grid_data = {
        'preset': preset_data['_id'],
        'name': preset_data['name'],
        'createdAt': await time_w_timezone(),
        'lastChange': await time_w_timezone(),
        'rowsOrder': preset_data['rowsOrder'],
        'rows': preset_data['rows'],
        'extra': preset_data['extra'],
    }
    if assignedPlatforms:
        grid_data['assignedPlatforms'] = assignedPlatforms
    collection = await get_db_collection(db, db_name, db_collection)
    db_log_data = await log_db_record(db_name, db_collection)
    logger.info(
        f'Creating a new `grid` record in `{db_collection}` collection'
        f' with `preset` = `{preset_data['_id']}`' + db_log_data
    )
    try:
        res = await collection.insert_one(grid_data)
        logger.info(
            f'Successfully created a new `grid` with `objectId` = {res.inserted_id}' + db_log_data
        )
        return res
    except PyMongoError as error:
        logger.error(f'Error while creating `grid` = {error}' + db_log_data)
        raise HTTPException(
            detail=f'Error while creating `grid',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
# CREATION ---


async def place_wheelstack_in_grid(
        placement_id: ObjectId,
        wheelstack_object_id: ObjectId,
        row: str,
        column: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': placement_id,
        f'rows.{row}.columns.{column}.wheelStack': {
            '$exists': True
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{column}.wheelStack': wheelstack_object_id,
            'lastChange': await time_w_timezone(),
        }
    }
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        logger.error(f'Error while placing `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while placing `wheelStack',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def block_grid_cell(
        placement_id: ObjectId,
        row: str,
        column: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': placement_id,
        f'rows.{row}.columns.{column}.blocked': {
            '$exists': True
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{column}.blocked': True,
            'lastChange': await time_w_timezone(),
        }
    }
    try:
        result = await collection.update_one(query, update)
        return result
    except PyMongoError as error:
        logger.error(f'Error while blocking `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while blocking `cell_data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def unblock_grid_cell(
        placement_id: ObjectId,
        row: str,
        column: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': placement_id,
        f'rows.{row}.columns.{column}.blocked': {
            '$exists': True
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{column}.blocked': False,
            'lastChange': await time_w_timezone(),
        }
    }
    try:
        result = await collection.update_one(query, update)
        return result
    except PyMongoError as error:
        logger.error(f'Error while unblocking `cell` {row}|{column} in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while unblocking `cell_data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def clear_grid_cell(
        placement_id: ObjectId,
        row: str,
        column: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': placement_id,
        f'rows.{row}.columns.{column}': {
            '$exists': True
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{column}.wheelStack': None,
            f'rows.{row}.columns.{column}.blocked': False,
            f'rows.{row}.columns.{column}.blockedBy': None,
            'lastChange': await time_w_timezone(),
        }
    }
    try:
        result = await collection.update_one(query, update)
        return result
    except PyMongoError as error:
        logger.error(f'Error while clearing `cell` {row}|{column} in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while clearing `cell_data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_grid_cell_data(
        grid_id: ObjectId,
        row: str,
        col: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': grid_id,
        f'rows.{row}.columns.{col}': {
            '$exists': True,
        }
    }
    projection = {
        '_id': 1,
        f'rows.{row}.columns.{col}': 1,
    }
    try:
        cell_data = await collection.find_one(query, projection)
        return cell_data
    except PyMongoError as error:
        logger.error(f'Error while searching `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while searching `cell_data`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_update_grid_cell_data(
        grid_id: ObjectId,
        row: str,
        col: str,
        new_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': grid_id,
        f'rows.{row}.columns.{col}': {
            '$exists': True,
        }
    }
    update = {
        '$set': {
            f'rows.{row}.columns.{col}.{key}': value for key, value in new_data.items()
            # f'rows.{row}.columns.{col}.wheelStack': new_data['wheelStack'],
            # f'rows.{row}.columns.{col}.blocked': new_data['blocked'],
            # f'rows.{row}.columns.{col}.blockedBy': new_data['blockedBy'],
        }
    }
    if record_change:
        update['$set']['lastChange'] = await time_w_timezone()
    max_retries = 3
    for attempt in range(max_retries):
        try:
            result = await collection.update_one(query, update, session=session)
            return result
        except PyMongoError as error:
            if error.has_error_label('TransientTransactionError'):
                logger.warning(f'`TransientTransactionError`: {error}. Attempt {attempt} of {max_retries}')
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
            else:
                logger.error(f'Error while updating `cell_data` in {db_collection}: {error}')
                raise HTTPException(
                    detail=f'Error while updating `cell_data`',
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )


async def db_get_grid_extra_cell_data(
        grid_id: ObjectId,
        extra_element_name: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': grid_id,
        f'extra.{extra_element_name}': {
            '$exists': True,
        }
    }
    projection = {
        '_id': 1,
        f'extra.{extra_element_name}': 1,
    }
    try:
        result = await collection.find_one(query, projection)
        return result
    except PyMongoError as error:
        logger.error(f'Error while searching extra `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while searching extra `cell_data`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_update_extra_cell_data(
        grid_id: ObjectId,
        extra_element_name: str,
        new_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': grid_id,
        f'extra.{extra_element_name}': {
            '$exists': True,
        }
    }
    update = {
        '$set': {
            f'extra.{extra_element_name}': new_data,
        }
    }
    if record_change:
        update['$set']['lastChange'] = await time_w_timezone()
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        logger.error(f'Error while updating extra `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while updating extra `cell_data`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_append_extra_cell_order(
        grid_id: ObjectId,
        extra_element_name: str,
        new_order: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': grid_id,
        f'extra.{extra_element_name}': {
            '$exists': True,
        }
    }
    update = {
        '$set': {
            f'extra.{extra_element_name}.orders.{new_order}': new_order,
        }
    }
    if  record_change:
        update['$set']['lastChange'] = await time_w_timezone()
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        logger.error(f'Error while appending new order to the extra element = {extra_element_name}'
                     f' in grid = {grid_id} | Error = {error}')
        raise HTTPException(
            detail='Error while trying to append new order',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_append_extra_cell_orders(
        grid_id: ObjectId,
        extra_element_name: str,
        new_orders,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': grid_id,
        f'extra.{extra_element_name}': {
            '$exists': True,
        }
    }
    update = {
        '$set': {
            **{f'extra.{extra_element_name}.orders.{order_id}': order_id for order_id in new_orders},
        }
    }
    if record_change:
        update['$set']['lastChange'] = await time_w_timezone()
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        logger.error(f'Error while appending new orders to the extra element = {extra_element_name}'
                     f' in grid = {grid_id} | Error = {error}')
        raise HTTPException(
            detail='Error while trying to append new orders',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_update_grid_cells_data(
        grid_id: ObjectId,
        new_cells_data: list,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': grid_id,
    }
    update = {
        '$set': {}
    }
    for cell_data in new_cells_data:
        update['$set'][
            f'rows.{cell_data['sourceRow']}.columns.{cell_data['sourceCol']}'
        ] = cell_data['newSourceCellData']
    if record_change:
        update['$set']['lastChange'] = await time_w_timezone()
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        logger.error(f'Error while updating multiple cells data in the grid {grid_id} = {error}')
        raise HTTPException(
            detail=f'Error while updating multiple cells data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_delete_extra_cell_order(
        grid_id: ObjectId,
        extra_element_name: str,
        order_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': grid_id,
        f'extra.{extra_element_name}.orders.{str(order_object_id)}': {
            '$exists': True,
        }
    }
    update = {
        '$unset': {
            f'extra.{extra_element_name}.orders.{str(order_object_id)}': 1
        },
    }
    if record_change:
        update['$set'] = {
            'lastChange': await time_w_timezone()
        }
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        logger.error(f'Error while updating extra `cell_data` in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while updating extra `cell_data`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_grid_last_change_time(
        grid_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': grid_id,
    }
    projection = {
        '_id': 1,
        'lastChange': 1,
    }
    try:
        result = await collection.find_one(query, projection)
        return result
    except PyMongoError as error:
        logger.error(f'Error while searching in {db_collection}: {error}')
        raise HTTPException(
            detail=f'Error while getting `lastChange` time',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_grid_get_custom_fields(
        grid_id: ObjectId,
        custom_fields: list[str],
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(
        db, db_name, db_collection
    )
    db_info = await log_db_record(db_name, db_collection)
    log_str: str = f'Attempt to gather `grid` records data => {grid_id} | With custom fields: '
    query = {
        '_id': grid_id
    }
    projection = {'_id': 1}
    for field in custom_fields:
        projection[field] = 1
        log_str += f'{field} | '
    logger.info(
        log_str + db_info
    )
    try:
        result = await collection.find_one(query, projection)
        logger.info(f'Successfully gathered `grid` records data => {grid_id}')
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        error_str: str = f'Error while gathering `grid` data => {grid_id}'
        logger.error(error_str + db_info + error_extra)
        raise HTTPException(
            detail=error_str,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_grid_add_assigned_platforms(
        grid_id: ObjectId,
        platforms: list[str],
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(
        db, db_name, db_collection
    )
    db_info = await log_db_record(db_name, db_collection)
    log_str: str = f'Attempt to assign new `platform`s to `grid` => {grid_id} | New platforms: {platforms}'
    query = {
        '_id': grid_id
    }
    update = {
        '$push': {
            'assignedPlatforms': {
                '$each': platforms,
            }
        }
    }
    logger.info(
        log_str + db_info
    )
    try:
        result = await collection.update_one(query, update)
        logger.info(f'Successfully updated `grid` with new `assignedPlatforms` data => {grid_id}')
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        error_str: str = f'Error while updating `grid` data => {grid_id}'
        logger.error(error_str + db_info + error_extra)
        raise HTTPException(
            detail=error_str,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
