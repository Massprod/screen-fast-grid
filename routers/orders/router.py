from bson import ObjectId
from loguru import logger
from utility.utilities import get_object_id
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.responses import JSONResponse, Response
from auth.jwt_validation import get_role_verification_dependency
from routers.history.history_actions import background_history_record
from fastapi import APIRouter, Depends, HTTPException, status, Body, Path, Query, BackgroundTasks
from routers.orders.crud import (
    db_find_order_by_object_id,
    db_get_all_orders,
    order_make_json_friendly,
    db_get_order_by_object_id,
)
from routers.orders.orders_completion import (
    orders_complete_move_wholestack,
    orders_complete_move_to_rejected,
    orders_complete_move_to_processing,
    orders_complete_move_to_laboratory, orders_complete_move_to_storage,
    orders_complete_move_wholestack_from_storage,
    orders_complete_move_to_pro_rej_from_storage,
    orders_complete_move_from_storage_to_storage,
    orders_complete_move_from_storage_to_lab
)
from routers.orders.orders_cancelation import (
    orders_cancel_basic_extra_element_moves,
    orders_cancel_move_wholestack,
    orders_cancel_move_to_storage,
    orders_cancel_move_from_storage_to_grid,
    orders_cancel_move_from_storage_to_extras,
    orders_cancel_move_from_storage_to_storage,
)
from routers.orders.models.models import (
    CreateMoveOrderRequest,
    CreateLabOrderRequest,
    CreateProcessingOrderRequest,
    CreateBulkProcessingOrderRequest,
    CreateMoveToStorageRequest,
    CreateMoveFromStorageRequest,
)
from routers.orders.orders_creation import (
    orders_create_move_whole_wheelstack,
    orders_create_move_to_laboratory,
    orders_create_move_to_processing,
    orders_create_move_to_rejected,
    orders_create_bulk_move_to_pro_rej_orders,
    orders_create_move_to_storage,
    orders_create_move_from_storage_whole_stack,
    orders_create_move_to_pro_rej_from_storage,
    orders_create_move_from_storage_to_storage_whole_stack,
    orders_create_move_from_storage_to_lab,
)
from constants import (
    ORDER_MOVE_WHOLE_STACK,
    ORDER_MOVE_TO_LABORATORY,
    ORDER_MOVE_TO_PROCESSING,
    ORDER_MOVE_TO_REJECTED,
    DB_PMK_NAME,
    CLN_ACTIVE_ORDERS,
    BASIC_EXTRA_MOVES,
    CLN_COMPLETED_ORDERS,
    CLN_CANCELED_ORDERS,
    ORDER_MOVE_TO_STORAGE,
    PS_STORAGE,
    BASIC_PAGE_VIEW_ROLES,
)


router = APIRouter()


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
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
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
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
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
        background_tasks: BackgroundTasks,
        order_data: CreateMoveOrderRequest = Body(...,
                                                  description='all required data for a new `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    created_order_id: ObjectId | None = None
    data = order_data.model_dump()
    if ORDER_MOVE_WHOLE_STACK == data['orderType']:
        logger.info(f'Creating order of type = `{ORDER_MOVE_WHOLE_STACK}`')
        created_order_id = await orders_create_move_whole_wheelstack(db, data)
    # + BG record +
    source_id = await get_object_id(data['source']['placementId'])
    source_type = data['source']['placementType']
    destination_id = await get_object_id(data['destination']['placementId'])
    destination_type = data['destination']['placementType']
    if source_id == destination_id:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
    else:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
        background_tasks.add_task(background_history_record, destination_id, destination_type, db)
    # - BG record -
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
        background_tasks: BackgroundTasks,
        order_data: CreateLabOrderRequest = Body(...,
                                                 description='all required data for a new lab `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    data = order_data.model_dump()
    logger.info(f'Creating order of type = `{ORDER_MOVE_TO_LABORATORY}`')
    created_order_id = await orders_create_move_to_laboratory(db, data)
    # + BG record +
    source_id = await get_object_id(data['source']['placementId'])
    source_type = data['source']['placementType']
    destination_id = await get_object_id(data['destination']['placementId'])
    destination_type = data['destination']['placementType']
    if source_id == destination_id:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
    else:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
        background_tasks.add_task(background_history_record, destination_id, destination_type, db)
    # - BG record -
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
        background_tasks: BackgroundTasks,
        order_data: CreateProcessingOrderRequest = Body(...,
                                                        description='all required data for a new processing `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    data = order_data.model_dump()
    logger.info(f'Request to create order of type = {ORDER_MOVE_TO_PROCESSING}')
    created_order_id = await orders_create_move_to_processing(db, data)
    # + BG record +
    source_id = await get_object_id(data['source']['placementId'])
    source_type = data['source']['placementType']
    destination_id = await get_object_id(data['destination']['placementId'])
    destination_type = data['destination']['placementType']
    if source_id == destination_id:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
    else:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
        background_tasks.add_task(background_history_record, destination_id, destination_type, db)
    # - BG record -
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
        background_tasks: BackgroundTasks,
        order_data: CreateBulkProcessingOrderRequest = Body(...,
                                                            description='basic data'),
        from_everywhere: bool = Query(False,
                                      description='Gather `wheelstack`s from `everywhere`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    order_req_data = order_data.model_dump()
    created_orders = await orders_create_bulk_move_to_pro_rej_orders(
        from_everywhere, order_req_data, db
    )
    # + BG record +
    source_id = await get_object_id(order_req_data['placementId'])
    source_type = order_req_data['placementType']
    destination_id = await get_object_id(order_req_data['destination']['placementId'])
    destination_type = order_req_data['destination']['placementType']
    if source_id == destination_id:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
    else:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
        background_tasks.add_task(background_history_record, destination_id, destination_type, db)
    # - BG record -
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
        background_tasks: BackgroundTasks,
        order_data: CreateProcessingOrderRequest = Body(...,
                                                        description='all required data for a new processing `order`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    data = order_data.model_dump()
    logger.info(f'Creating order of type = {ORDER_MOVE_TO_REJECTED}')
    created_order_id = await orders_create_move_to_rejected(db, data)
    # + BG record +
    source_id = await get_object_id(data['source']['placementId'])
    source_type = data['source']['placementType']
    destination_id = await get_object_id(data['destination']['placementId'])
    destination_type = data['destination']['placementType']
    if source_id == destination_id:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
    else:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
        background_tasks.add_task(background_history_record, destination_id, destination_type, db)
    # - BG record -
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
        background_tasks: BackgroundTasks,
        order_data: CreateMoveToStorageRequest,
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    data = order_data.model_dump()
    created_order_id = await orders_create_move_to_storage(db, data)
    # + BG record +
    source_id = await get_object_id(data['source']['placementId'])
    source_type = data['source']['placementType']
    destination_id = await get_object_id(data['storage'])
    destination_type = PS_STORAGE
    if source_id == destination_id:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
    else:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
        background_tasks.add_task(background_history_record, destination_id, destination_type, db)
    # - BG record -
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
        background_tasks: BackgroundTasks,
        order_data: CreateMoveFromStorageRequest = Body(...),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    data = order_data.model_dump()
    created_order_id: ObjectId | None = None
    if ORDER_MOVE_WHOLE_STACK == data['orderType']:
        created_order_id = await orders_create_move_from_storage_whole_stack(db, data)
    elif ORDER_MOVE_TO_PROCESSING == data['orderType']:
        created_order_id = await orders_create_move_to_pro_rej_from_storage(db, data, True)
    elif ORDER_MOVE_TO_REJECTED == data['orderType']:
        created_order_id = await orders_create_move_to_pro_rej_from_storage(db, data, False)
    elif ORDER_MOVE_TO_STORAGE == data['orderType']:
        created_order_id = await orders_create_move_from_storage_to_storage_whole_stack(db, data)
    elif ORDER_MOVE_TO_LABORATORY == data['orderType']:
        created_order_id = await orders_create_move_from_storage_to_lab(db, data)
    # + BG record +
    source_id = await get_object_id(data['source']['storageId'])
    source_type = PS_STORAGE
    destination_id = await get_object_id(data['destination']['placementId'])
    destination_type = data['destination']['placementType']
    if source_id == destination_id:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
    else:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
        background_tasks.add_task(background_history_record, destination_id, destination_type, db)
    # - BG record -
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
        background_tasks: BackgroundTasks,
        order_object_id: str = Path(...,
                                    description='`objectId` of the order to cancel'),
        cancellation_reason: str = Query('',
                                         description='reason of cancellation'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    order_id: ObjectId = await get_object_id(order_object_id)
    order_data = await db_find_order_by_object_id(order_id, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS)
    if order_data is None:
        raise HTTPException(
            detail=f'Order with `objectId` = {order_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    result = None
    if order_data['orderType'] == ORDER_MOVE_WHOLE_STACK:
        if order_data['source']['placementType'] == PS_STORAGE:
            result = await orders_cancel_move_from_storage_to_grid(
                order_data, cancellation_reason, db
            )
        else:
            result = await orders_cancel_move_wholestack(order_data, cancellation_reason, db)
    elif order_data['orderType'] in BASIC_EXTRA_MOVES:
        if order_data['source']['placementType'] == PS_STORAGE:
            result = await orders_cancel_move_from_storage_to_extras(order_data, cancellation_reason, db)
        else:
            result = await orders_cancel_basic_extra_element_moves(order_data, cancellation_reason, db)
    elif order_data['orderType'] == ORDER_MOVE_TO_STORAGE:
        if (order_data['source']['placementType'] == PS_STORAGE
                and order_data['destination']['placementType'] == PS_STORAGE):
            result = await orders_cancel_move_from_storage_to_storage(order_data, cancellation_reason, db)
        else:
            result = await orders_cancel_move_to_storage(
                order_data, cancellation_reason, db
            )
    logger.info(f'Order canceled and moved to `canceledOrders` with `_id` = {result}')
    # + BG record +
    source_id = order_data['source']['placementId']
    source_type = order_data['source']['placementType']
    destination_id = order_data['destination']['placementId']
    destination_type = order_data['destination']['placementType']
    if source_id == destination_id:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
    else:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
        background_tasks.add_task(background_history_record, destination_id, destination_type, db)
    # - BG record -
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    path='/complete/{order_object_id}',
    description=f'Completes existing order, applies all dependencies',
    name='Complete Order',
)
async def route_post_complete_order(
        background_tasks: BackgroundTasks,
        order_object_id: str = Path(...,
                                    description='`objectId` of the order to complete'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    order_id: ObjectId = await get_object_id(order_object_id)
    order_data = await db_find_order_by_object_id(order_id, db, DB_PMK_NAME, CLN_ACTIVE_ORDERS)
    if order_data is None:
        raise HTTPException(
            detail=f'Order with `objectId` = {order_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    log_record: str = f'Order completed and moved to `completedOrders` with `_id` => '
    result: str | ObjectId = ''
    if order_data['orderType'] == ORDER_MOVE_WHOLE_STACK:
        if PS_STORAGE == order_data['source']['placementType']:
            result = await orders_complete_move_wholestack_from_storage(order_data, db)
        else:
            result = await orders_complete_move_wholestack(order_data, db)
    elif order_data['orderType'] == ORDER_MOVE_TO_PROCESSING:
        if PS_STORAGE == order_data['source']['placementType']:
            result = await orders_complete_move_to_pro_rej_from_storage(db, order_data, True)
        else:
            result = await orders_complete_move_to_processing(order_data, db)
    elif order_data['orderType'] == ORDER_MOVE_TO_REJECTED:
        if PS_STORAGE == order_data['source']['placementType']:
            result = await orders_complete_move_to_pro_rej_from_storage(db, order_data, False)
        else:
            result = await orders_complete_move_to_rejected(order_data, db)
    elif order_data['orderType'] == ORDER_MOVE_TO_LABORATORY:
        if PS_STORAGE == order_data['source']['placementType']:
            result = await orders_complete_move_from_storage_to_lab(order_data, db)
        else:
            result = await orders_complete_move_to_laboratory(order_data, db)
    elif order_data['orderType'] == ORDER_MOVE_TO_STORAGE:
        if (order_data['source']['placementType'] == PS_STORAGE
                and order_data['destination']['placementType'] == PS_STORAGE):
            result = await orders_complete_move_from_storage_to_storage(order_data, db)
        else:
            result = await orders_complete_move_to_storage(order_data, db)
    logger.info(log_record + str(result))
    # + BG record +
    source_id = order_data['source']['placementId']
    source_type = order_data['source']['placementType']
    destination_id = order_data['destination']['placementId']
    destination_type = order_data['destination']['placementType']
    if source_id == destination_id:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
    else:
        background_tasks.add_task(background_history_record, source_id, source_type, db)
        background_tasks.add_task(background_history_record, destination_id, destination_type, db)
    # - BG record -
    return Response(status_code=status.HTTP_204_NO_CONTENT)
