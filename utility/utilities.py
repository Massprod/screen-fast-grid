import asyncio
from functools import wraps
from loguru import logger
from bson import ObjectId
from pymongo import errors
from bson.errors import InvalidId
from datetime import datetime, timezone
from pymongo.errors import PyMongoError
from fastapi import HTTPException, WebSocketException, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession

from constants import WS_CODES



async def get_object_id(
        object_id: str
):
    try:
        object_id = ObjectId(object_id)
        return object_id
    except InvalidId as e:
        status_code = status.HTTP_400_BAD_REQUEST
        logger.error(f"Invalid ObjectId format: {object_id} - {e}")
        raise HTTPException(detail=str(e), status_code=status_code)


async def log_db_record(
        db_name: str,
        db_collection: str
) -> str:
    return f' | DB: {db_name}\nDB_Collection:{db_collection}'


async def log_db_error_record(
        error: errors.PyMongoError
) -> str:
    return f' | ERROR: {error}'


# TODO: Move it all to a separated part
# Orders
async def orders_creation_attempt_string(
        order_type: str,
) -> str:
    return f'Attempt to create an order of type {order_type} | '


async def orders_corrupted_cell_non_existing_wheelstack(
        cell_row: str,
        cell_col: str,
        placement_type: str,
        placement_id: str | ObjectId,
        wheelstack_id: str | ObjectId,
) -> str:
    return (f'Corrupted data on cell: row = {cell_row}, col = {cell_col}'
            f' in a placement of type {placement_type} with `ObjectId` = {placement_id}.'
            f' Non existing `wheelstack` placed on it = {wheelstack_id}')


async def orders_corrupted_cell_blocked_wheelstack(
        cell_row: str,
        cell_col: str,
        placement_type: str,
        placement_id: str | ObjectId,
        wheelstack_id: str | ObjectId
) -> str:
    return (f'Corrupted data on cell: row = {cell_row}, col = {cell_col}'
            f' in a placement of type {placement_type} with `ObjectId` = {placement_id}.'
            f" There's blocked `wheelstack` currently placed on it = {wheelstack_id},"
            f" but cell is marked as Free.")


async def get_db_collection(
        client: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    """
       Utility function to get a MongoDB collection based on database name and collection name,
       with error handling.

       Parameters:
        client(AsyncIOMotorClient): Pymongo DB client to use.
        db_name (str): The name of the database.
        db_collection (str): The name of the collection.

       Returns:
       pymongo.collection.Collection: The MongoDB collection.

       Raises:
       HTTPException: If there is an error accessing the database or collection.
    """
    try:
        # Check if the database exists
        if db_name not in await client.list_database_names():
            logger.error(f"Database '{db_name}' not found")
            raise HTTPException(status_code=404, detail=f"Database '{db_name}' not found")

        db = client[db_name]

        # Check if the collection exists
        if db_collection not in await db.list_collection_names():
            logger.error(f"Collection '{db_collection}' not found in database '{db_name}'")
            raise HTTPException(status_code=404,
                                detail=f"Collection '{db_collection}' not found in database '{db_name}'")

        collection = db[db_collection]
        return collection

    except errors.PyMongoError as e:
        logger.error(f"MongoDB error: {e}")
        raise HTTPException(status_code=500, detail="An error occurred with MongoDB")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")


async def time_w_timezone() -> datetime:
    return datetime.now(timezone.utc)


async def get_correct_datetime(date_string) -> datetime | None:
    try:
        cor_date = datetime.strptime(date_string, '%Y-%m-%d')
        cor_date = cor_date.replace(hour=0, minute=0, second=0, microsecond=0)
        return cor_date
    except ValueError:
        return None


def convert_object_id_and_datetime_to_str(doc):
    if isinstance(doc, dict):
        return {k: convert_object_id_and_datetime_to_str(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [convert_object_id_and_datetime_to_str(v) for v in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()
    else:
        return doc

async def async_convert_object_id_and_datetime_to_str(doc):
    if isinstance(doc, dict):
        # Convert dictionary values concurrently
        tasks = {k: async_convert_object_id_and_datetime_to_str(v) for k, v in doc.items()}
        return {k: await v for k, v in tasks.items()}
    elif isinstance(doc, list):
        # Convert list values concurrently
        tasks = [async_convert_object_id_and_datetime_to_str(v) for v in doc]
        return await asyncio.gather(*tasks)
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()
    else:
        return doc


async def async_convert_object_records(doc, type_converters: dict):
    for type, converter in type_converters.items():
        if isinstance(doc, type):
            return converter(doc)
    if isinstance(doc, dict):
        # Convert dictionary values concurrently
        tasks = {k: async_convert_object_records(v, type_converters) for k, v in doc.items()}
        return {k: await v for k, v in tasks.items()}
    elif isinstance(doc, list):
        # Convert list values concurrently
        tasks = [async_convert_object_records(v, type_converters) for v in doc]
        return await asyncio.gather(*tasks)
    return doc


async def db_execute_free_find_one_query(
        query: dict,
        project: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to search `wheel`s with free query' + db_info
    )
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        result = await collection.find_one(query, project, session=session)
        logger.info(
            f'Succesfully executed free query on {db_collection}'
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while executing free query on collection => {db_collection}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while executing free query',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def handle_basic_exceptions(msg: str, code: int, is_websocket: bool = False):
    if is_websocket:
        raise WebSocketException(
            code=WS_CODES.get(code, status.WS_1008_POLICY_VIOLATION),
            reason=msg,
        )
    else:
        raise HTTPException(
            detail=msg,
            status_code=code,
        )


async def handle_http_exceptions_for_websocket(func, *args, **kwargs):
    try:
        # Call the function that might raise an HTTPException
        return await func(*args, **kwargs)
    except HTTPException as http_exc:
        # Convert HTTPException to WebSocketException with an appropriate code
        status_code = http_exc.status_code
        raise WebSocketException(
            code=WS_CODES.get(status_code, status.WS_1008_POLICY_VIOLATION),
            reason=http_exc.detail
        )
