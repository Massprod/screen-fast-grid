from bson import ObjectId
from loguru import logger
from datetime import datetime
from fastapi import HTTPException, status
from utility.utilities import time_w_timezone
from motor.motor_asyncio import AsyncIOMotorClient
from constants import PLACEMENT_COLLECTIONS, DB_PMK_NAME, CLN_WHEELSTACKS, CLN_ACTIVE_ORDERS
from routers.history.crud import db_history_get_placement_data
from routers.wheelstacks.crud import db_history_get_placement_wheelstacks
from routers.orders.crud import db_history_get_orders_by_placement


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
    # Only care about `activeOrders` <- we actually need to combine all orders in one, but later.
    orders_data = await db_history_get_orders_by_placement(
        placement_id, placement_type, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS
    )
    history_record_data: dict = {
        'createdAt': history_record_date,
        'placementType': placement_type,
        'placementData': placement_data,
        'wheelstacksData': wheelstacks_data,
        'placementOrders': orders_data,
    }
    return history_record_data