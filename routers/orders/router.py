from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.responses import JSONResponse, Response
from routers.orders.models.models import CreateMoveOrderRequest, CreateLabOrderRequest
from routers.orders.order_actions import orders_create_move_whole_wheelstack, orders_create_move_to_laboratory
from fastapi import APIRouter, Depends, HTTPException, status, Body, Path
from constants import (ORDER_MOVE_WHOLE_STACK, ORDER_MOVE_TO_LABORATORY, PRES_TYPE_GRID,
                       PRES_TYPE_PLATFORM, DB_PMK_NAME, CLN_GRID,
                       CLN_WHEELSTACKS, CLN_BASE_PLATFORM, ORDER_STATUS_PENDING, CLN_ACTIVE_ORDERS)
from routers.grid.crud import db_get_grid_cell_data, db_update_grid_cell_data
from routers.base_platform.crud import db_get_platform_cell_data, db_update_platform_cell_data
from routers.wheelstacks.crud import db_find_wheelstack_by_object_id, db_update_wheelstack
from routers.orders.crud import db_create_order
from utility.utilities import get_object_id, time_w_timezone
from loguru import logger

router = APIRouter()


# TODO: We need to know more about how orders should be processed.
#  Because we can move it to some `extra` element and place it here for a use.
#  Or we can delete it instantly when it's placed here.
#  For, now I will delete them, because that's what I been told.
#  But I guess, it's better to place them on the `extra` element an then
#  create a different order to move it from here to w.e the place we want.
#  In this case, we can always know where's is it and manipulate it easier.
#  So, it can be a potential rebuild of all of the ORDERS, but let's stick for a thing we been told.


@router.post(
    path='/create/move',
    description='Creates a new order with a chosen type, validates if it can be executed',
    name='New Order',
)
async def route_post_create_order(
        order_data: CreateMoveOrderRequest = Body(...,
                                                  description='all required data for a new `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.get_client),
):
    # TODO: `moveTopWheel` and `mergeWheelStacks` are extra orders with is not required.
    #   but it's going to be a good practice and useful to do.
    #   Return and add them, after completing everything else (maybe).
    data = order_data.model_dump()
    if ORDER_MOVE_WHOLE_STACK == data['orderType']:
        logger.info(f'Creating order of type = `{ORDER_MOVE_WHOLE_STACK}`')
        created_order_id = await orders_create_move_whole_wheelstack(db, data)
        return JSONResponse(
            content={
                '_id': str(created_order_id),
            },
            status_code=status.HTTP_201_CREATED,
        )


@router.post(
    path='/create/lab',
    description='Creates a new order of type `moveToLaboratory`',
    name='New Order',
)
async def route_post_create_order_move_to_lab(
        order_data: CreateLabOrderRequest = Body(...,
                                                 description='all required data for a new lab `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.get_client),
):
    data = order_data.model_dump()
    logger.info(f'Creating order of type = `{ORDER_MOVE_TO_LABORATORY}`')
    created_order_id = await orders_create_move_to_laboratory(db, data)
    return JSONResponse(
        content={
            '_id': str(created_order_id),
        },
        status_code=status.HTTP_201_CREATED,
    )
