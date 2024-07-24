from bson import ObjectId
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.responses import JSONResponse, Response
from routers.orders.crud import db_find_order_by_object_id
from routers.orders.orders_cancelation import orders_cancel_move_to_laboratory
from routers.orders.models.models import CreateMoveOrderRequest, CreateLabOrderRequest, CreateProcessingOrderRequest
from routers.orders.orders_creation import (orders_create_move_whole_wheelstack,
                                            orders_create_move_to_laboratory,
                                            orders_create_move_to_processing,
                                            orders_create_move_to_rejected,
                                            )
from fastapi import APIRouter, Depends, HTTPException, status, Body, Path, Query
from constants import (ORDER_MOVE_WHOLE_STACK, ORDER_MOVE_TO_LABORATORY,
                       ORDER_MOVE_TO_PROCESSING, ORDER_MOVE_TO_REJECTED,
                       DB_PMK_NAME, CLN_ACTIVE_ORDERS)
from utility.utilities import get_object_id
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
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
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
    path='/create/laboratory',
    description=f'Creates a new order of type {ORDER_MOVE_TO_LABORATORY}',
    name='New Order',
)
async def route_post_create_order_move_to_lab(
        order_data: CreateLabOrderRequest = Body(...,
                                                 description='all required data for a new lab `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
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


@router.post(
    path='/create/process',
    description=f'Creates a new order of type {ORDER_MOVE_TO_PROCESSING}',
    name='New Order',
)
async def route_post_create_order_move_to_processing(
        order_data: CreateProcessingOrderRequest = Body(...,
                                                        description='all required data for a new processing `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    data = order_data.model_dump()
    logger.info(f'Creating order of type = {ORDER_MOVE_TO_PROCESSING}')
    created_order_id = await orders_create_move_to_processing(db, data)
    return JSONResponse(
        content={
            '_id': str(created_order_id),
        },
        status_code=status.HTTP_201_CREATED,
    )


@router.post(
    path='/create/reject',
    description=f'Creates a new order of type {ORDER_MOVE_TO_REJECTED}',
    name='New Order',
)
async def route_post_create_order_move_to_rejected(
        # TODO: If we change logic for this order, change MODEL
        order_data: CreateProcessingOrderRequest = Body(...,
                                                        description='all required data for a new processing `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    data = order_data.model_dump()
    logger.info(f'Creating order of type = {ORDER_MOVE_TO_REJECTED}')
    created_order_id = await orders_create_move_to_rejected(db, data)
    return JSONResponse(
        content={
            '_id': str(created_order_id),
        },
        status_code=status.HTTP_201_CREATED,
    )


@router.delete(
    path='/cancel/{order_object_id}',
    description=f'Cancels existing order',
    name='Cancel Order',
)
async def route_delete_cancel_order(
        order_object_id: str = Path(...,
                                    description='`objectId` of the order to cancel'),
        cancellation_reason: str = Query('',
                                         description='reason of cancellation'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    order_id: ObjectId = await get_object_id(order_object_id)
    order_data = await db_find_order_by_object_id(order_id, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS)
    if order_data is None:
        raise HTTPException(
            detail=f'Order with `objectId` = {order_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if ORDER_MOVE_TO_LABORATORY == order_data['orderType']:
        result = await orders_cancel_move_to_laboratory(order_data, cancellation_reason, db)
        logger.info(f'Order canceled and move to `canceledOrders` with `_id` = {result}')
        return Response(status_code=status.HTTP_204_NO_CONTENT)


