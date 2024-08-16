from bson import ObjectId
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.responses import JSONResponse, Response
from routers.orders.crud import (db_find_order_by_object_id,
                                 db_get_all_orders,
                                 order_make_json_friendly,
                                 db_get_order_by_object_id, )
from routers.orders.orders_completion import (orders_complete_move_wholestack,
                                              orders_complete_move_to_rejected,
                                              orders_complete_move_to_processing,
                                              orders_complete_move_to_laboratory, orders_complete_move_to_storage,
                                              orders_complete_move_wholestack_from_storage)
from routers.orders.orders_cancelation import orders_cancel_basic_extra_element_moves, orders_cancel_move_wholestack, \
    orders_cancel_move_to_storage
from routers.orders.models.models import CreateMoveOrderRequest, CreateLabOrderRequest, CreateProcessingOrderRequest, \
    CreateBulkProcessingOrderRequest, CreateMoveToStorageRequest, CreateMoveFromStorageRequest
from routers.orders.orders_creation import (orders_create_move_whole_wheelstack,
                                            orders_create_move_to_laboratory,
                                            orders_create_move_to_processing,
                                            orders_create_move_to_rejected, orders_create_bulk_move_to_pro_rej_orders,
                                            orders_create_move_to_storage, orders_create_move_from_storage_whole_stack,
                                            orders_create_move_from_storage_whole_stack,
                                            orders_create_move_to_pro_rej_from_storage,
                                            )
from fastapi import APIRouter, Depends, HTTPException, status, Body, Path, Query
from constants import (
    ORDER_MOVE_WHOLE_STACK, ORDER_MOVE_TO_LABORATORY,
    ORDER_MOVE_TO_PROCESSING, ORDER_MOVE_TO_REJECTED,
    DB_PMK_NAME, CLN_ACTIVE_ORDERS, BASIC_EXTRA_MOVES,
    CLN_COMPLETED_ORDERS, CLN_CANCELED_ORDERS, ORDER_MOVE_TO_STORAGE, PS_STORAGE,
)
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


@router.get(
    path='/order/{order_object_id}',
    description='Get a single order data by its `objectId`.'
                ' Can be filtered by `orderType`.'
                ' Searching for all `orderType`s by default.',
    name='Get Order',
)
async def route_get_order(
        order_object_id: str = Path(...,
                                    description='`objectId` of the order to search'),
        active_orders: bool = Query(True,
                                    description='False to exclude `activeOrders`'),
        completed_orders: bool = Query(True,
                                       description='False to exclude `completedOrders`'),
        canceled_orders: bool = Query(True,
                                      description='False to exclude `canceledOrders`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    order_id = await get_object_id(order_object_id)
    order_filter = {
        CLN_ACTIVE_ORDERS: active_orders,
        CLN_COMPLETED_ORDERS: completed_orders,
        CLN_CANCELED_ORDERS: canceled_orders,
    }
    for collection, include in order_filter.items():
        if not include:
            continue
        order_data = await db_get_order_by_object_id(
            order_id, db, DB_PMK_NAME, collection
        )
        if order_data is not None:
            order_data = await order_make_json_friendly(order_data)
            return JSONResponse(
                content=order_data,
                status_code=status.HTTP_200_OK,
            )
    raise HTTPException(
        detail=f'Order with `objectId` = {order_object_id}. Not Found.',
        status_code=status.HTTP_404_NOT_FOUND,
    )


# TODO: We need to think about changing platform and grid,
#  because should we even differ them? Why not just use their id + name to differ.
#  Also we need extra endpoint to differ sourceId and destinationId for different platforms and grids.
@router.get(
    path='/all',
    description='Get all of the order types, or filter them with query.'
                'Returns all types by default.',
    name='Get Orders',
)
async def route_get_all_orders(
        active_orders: bool = Query(True,
                                    description='False to exclude `activeOrders'),
        completed_orders: bool = Query(True,
                                       description='False to exclude `completedOrders`'),
        canceled_orders: bool = Query(True,
                                      description='False to exclude `canceledOrders`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    all_data = {}
    if active_orders:
        active = await db_get_all_orders(db, DB_PMK_NAME, CLN_ACTIVE_ORDERS)
        active_json = {}
        for order_data in active:
            order_json = await order_make_json_friendly(order_data)
            active_json[order_json['_id']] = order_json
        all_data['activeOrders'] = active_json
    if completed_orders:
        completed = await db_get_all_orders(db, DB_PMK_NAME, CLN_COMPLETED_ORDERS)
        completed_json = {}
        for order_data in completed:
            order_json = await order_make_json_friendly(order_data)
            completed_json[order_json['_id']] = order_json
        all_data['completedOrders'] = completed_json
    if canceled_orders:
        canceled = await db_get_all_orders(db, DB_PMK_NAME, CLN_CANCELED_ORDERS)
        canceled_json = {}
        for order_data in canceled:
            order_json = await order_make_json_friendly(order_data)
            canceled_json[order_json['_id']] = order_json
        all_data['canceledOrders'] = canceled_json
    return JSONResponse(
        content=all_data,
        status_code=status.HTTP_200_OK,
    )


@router.post(
    path='/create/move',
    description='Creates a new order with a chosen type, validates if it can be executed',
    name='New Order',
)
async def route_post_create_order_move(
        order_data: CreateMoveOrderRequest = Body(...,
                                                  description='all required data for a new `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    created_order_id: ObjectId | None = None
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
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    data = order_data.model_dump()
    logger.info(f'Request to create order of type = {ORDER_MOVE_TO_PROCESSING}')
    created_order_id = await orders_create_move_to_processing(db, data)
    return JSONResponse(
        content={
            '_id': str(created_order_id),
        },
        status_code=status.HTTP_201_CREATED,
    )


@router.post(
    path='/create/bulk/',
    description=f'Creates orders for every available `wheelstack` of the batch',
    name='New Bulk Orders',
)
async def route_post_create_bulk_orders_move_to_pro_rej(
        order_data: CreateBulkProcessingOrderRequest = Body(...,
                                                            description='basic data'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    order_req_data = order_data.model_dump()
    created_orders = await orders_create_bulk_move_to_pro_rej_orders(order_req_data, db)
    return JSONResponse(
        content={
            'createdOrders': [str(orderId) for orderId in created_orders],
        },
        status_code=status.HTTP_200_OK,
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


@router.post(
    path='/create/storage/move_to',
    description=f'Creates a new order of type {ORDER_MOVE_TO_STORAGE}',
    name='New Order',
)
async def route_post_create_order_move_to_storage(
        order_data: CreateMoveToStorageRequest,
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    data = order_data.model_dump()
    created_order_id = await orders_create_move_to_storage(db, data)
    return JSONResponse(
        content={
            'createdOrder': str(created_order_id),
        },
        status_code=status.HTTP_201_CREATED,
    )


@router.post(
    path='/create/storage/move_from',
    description=f'Creates a new order of type {ORDER_MOVE_WHOLE_STACK}',
    name='New Order',
)
async def route_post_create_order_move_from_storage(
        order_data: CreateMoveFromStorageRequest = Body(...),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    data = order_data.model_dump()
    created_order_id: ObjectId | None = None
    if ORDER_MOVE_WHOLE_STACK == data['orderType']:
        created_order_id = await orders_create_move_from_storage_whole_stack(db, data)
    elif ORDER_MOVE_TO_PROCESSING == data['orderType'] or ORDER_MOVE_TO_REJECTED == data['orderType']:
        created_order_id = await orders_create_move_to_pro_rej_from_storage(db, data)
    return JSONResponse(
        content={
            'createdId': str(created_order_id)
        },
        status_code=status.HTTP_201_CREATED,
    )


@router.post(
    path='/cancel/{order_object_id}',
    description=f'Cancels existing order',
    name='Cancel Order',
)
async def route_post_cancel_order(
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
    if order_data['orderType'] == ORDER_MOVE_WHOLE_STACK:
        result = await orders_cancel_move_wholestack(order_data, cancellation_reason, db)
        logger.info(f'Order canceled and moved to `canceledOrders` with `_id` = {result}')
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    elif order_data['orderType'] in BASIC_EXTRA_MOVES:
        result = await orders_cancel_basic_extra_element_moves(order_data, cancellation_reason, db)
        logger.info(f'Order canceled and moved to `canceledOrders` with `_id` = {result}')
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    elif order_data['orderType'] == ORDER_MOVE_TO_STORAGE:
        result = await orders_cancel_move_to_storage(
            order_data, cancellation_reason, db
        )
        logger.info(f'Order canceled and moved to `canceledOrders` with `_id` = {result}')
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    path='/complete/{order_object_id}',
    description=f'Completes existing order, applies all dependencies',
    name='Complete Order',
)
async def route_post_complete_order(
        order_object_id: str = Path(...,
                                    description='`objectId` of the order to complete'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    order_id: ObjectId = await get_object_id(order_object_id)
    order_data = await db_find_order_by_object_id(order_id, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS)
    if order_data is None:
        raise HTTPException(
            detail=f'Order with `objectId` = {order_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    log_record: str = f'Order completed and moved to `completedOrders` with `_id` = '
    result: str | ObjectId = ''
    if order_data['orderType'] == ORDER_MOVE_WHOLE_STACK:
        if PS_STORAGE == order_data['source']['placementType']:
            result = await orders_complete_move_wholestack_from_storage(order_data, db)
        else:
            result = await orders_complete_move_wholestack(order_data, db)
    elif order_data['orderType'] == ORDER_MOVE_TO_PROCESSING:
        result = await orders_complete_move_to_processing(order_data, db)
    elif order_data['orderType'] == ORDER_MOVE_TO_REJECTED:
        result = await orders_complete_move_to_rejected(order_data, db)
    elif order_data['orderType'] == ORDER_MOVE_TO_LABORATORY:
        result = await orders_complete_move_to_laboratory(order_data, db)
    elif order_data['orderType'] == ORDER_MOVE_TO_STORAGE:
        result = await orders_complete_move_to_storage(order_data, db)
    logger.info(log_record + str(result))
    return Response(status_code=status.HTTP_204_NO_CONTENT)
