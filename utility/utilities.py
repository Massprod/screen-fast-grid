from loguru import logger
from bson import ObjectId
from pymongo import errors
from bson.errors import InvalidId
from datetime import datetime, timezone
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient


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
