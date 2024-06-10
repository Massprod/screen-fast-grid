from bson import ObjectId
from bson.errors import InvalidId
from routers.orders.crud import db_create_order, db_find_order, db_delete_order, db_update_order
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.responses import JSONResponse, Response
from fastapi import APIRouter, Depends, HTTPException, status, Body, Path
from routers.orders.models.models import CreateOrderRequest, OrderType
from routers.orders.models.response_models import OrderStandardResponse
from datetime import datetime, timezone
from routers.wheelstacks.crud import db_find_wheelstack_id_by_placement, db_find_wheelstack
from utility.db_utilities import get_preset, get_object_id, time_w_timezone
from routers.orders.order_actions import (orders_get_placement_data, orders_complete_whole_wheelstack_move,
                                          orders_block_wheelstack, orders_block_placement,
                                          orders_unblock_placement, orders_cancel_unblock_wheelstack)
from constants import *


router = APIRouter()


@router.post(
    path='/move',
    description='Create New FullMove Order',
    status_code=status.HTTP_201_CREATED,
    response_description='Details of the newly created order',
    response_model=OrderStandardResponse,
)
async def create_full_move_order(
        order: CreateOrderRequest = Body(
            ...,
            description='Order creation data',
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    # We can create full Move order.
    # 1 - if SOURCE is not empty and not blocked
    # 2 - destination is empty, it's not a `whiteSpace` and not blocked
    # We always should block both SOURCE and DESTINATION, until order is completed or canceled.
    order_data = order.dict()
    status_code = status.HTTP_201_CREATED
    if ORDER_MOVE_WHOLE_STACK != order_data['orderType']:
        status_code = status.HTTP_400_BAD_REQUEST
        raise HTTPException(detail=f'Only {OrderType.moveWholeStack} available', status_code=status_code)
    source = order_data['source']['type']
    source_row, source_column = order_data['source']['identifier'].split(',')
    source_preset = await get_preset(source)
    source_data = await orders_get_placement_data(
        db, source_row, source_column, source, source_preset
    )
    source_whitespace = source_data['whiteSpace']
    if source_whitespace:
        status_code = status.HTTP_403_FORBIDDEN
        raise HTTPException(detail="Whitespaces can't be used", status_code=status_code)
    source_wheelstack_object_id = source_data['wheelStack']
    if source_wheelstack_object_id is None:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail='No wheelstack on chosen place to move from', status_code=status_code)
    source_wheelstack_data = await db_find_wheelstack(db, source_wheelstack_object_id)
    if source_wheelstack_data['blocked']:
        status_code = status.HTTP_409_CONFLICT
        raise HTTPException(detail='Source `wheelStack` is already waiting for order completion',
                            status_code=status_code)
    destination = order_data['destination']['type']
    destination_row, destination_column = order_data['destination']['identifier'].split(',')
    destination_preset = await get_preset(destination)
    destination_data = await orders_get_placement_data(
        db, destination_row, destination_column, destination, destination_preset
    )
    if destination_data['whiteSpace']:
        status_code = status.HTTP_403_FORBIDDEN
        raise HTTPException(detail="Whitespaces can't be used", status_code=status_code)
    if destination_data['blocked']:
        status_code = status.HTTP_409_CONFLICT
        raise HTTPException(detail="Destination is already in `pending` state for another order",
                            status_code=status_code)
    destination_wheelstack_object_id = destination_data['wheelStack']
    if destination_wheelstack_object_id is not None:
        status_code = status.HTTP_409_CONFLICT
        raise HTTPException(detail='Destination is not empty', status_code=status_code)
    order_data['affectedWheelStacks'] = {
        'source': source_wheelstack_object_id,
        'destination': destination_wheelstack_object_id,
    }
    order_data['affectedWheels'] = {
        'source': source_wheelstack_data['wheels'],
        'destination': [],
    }
    order_data['status'] = 'pending'
    order_data['createdAt'] = datetime.now(timezone.utc)
    order_data['lastUpdated'] = datetime.now(timezone.utc)
    created_order = await db_create_order(db, order_data)

    await orders_block_wheelstack(db, source_wheelstack_object_id, created_order.inserted_id)
    await orders_block_placement(db, destination_row, destination_column, destination, destination_preset)

    created_order_id = str(created_order.inserted_id)
    resp = OrderStandardResponse()
    resp.set_status(status_code)
    resp.set_create_message(created_order_id)
    resp.data = {
        'orderObjectId': created_order_id,
    }
    return JSONResponse(content=resp.dict(), status_code=status_code)


@router.put(
    path='/{order_object_id}/start',
    description='Start Any Active Order',
    status_code=status.HTTP_200_OK,
    response_description='Simple status code responses. For now.'
)
async def start_order(
        order_object_id: str = Path(..., description='`objectId` of the order'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    status_code = status.HTTP_200_OK
    order_id = await get_object_id(order_object_id)
    order_data = await db_find_order(db, order_id)
    if order_data is None:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail=f'No active order with ID: {order_object_id}', status_code=status_code)
    order_update = {
        'status': ORDER_STATUS_IN_PROGRESS,
        'lastUpdated': await time_w_timezone(),
    }

    result = await db_update_order(db, order_id, order_update)
    if 0 == result.modified_count:
        status_code = status.HTTP_304_NOT_MODIFIED
        raise HTTPException(detail=f'already {ORDER_STATUS_IN_PROGRESS}', status_code=status_code)
    return Response(status_code=status_code)


@router.put(
    path='/{order_object_id}/stop',
    description=f'Stop Any {ORDER_STATUS_IN_PROGRESS} Order',
    status_code=status.HTTP_200_OK,
    response_description='Simple status code responses. For now',
)
async def stop_order(
        order_object_id: str = Path(..., description='`objectId` of the order'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    status_code = status.HTTP_200_OK
    order_id = await get_object_id(order_object_id)
    order_data = await db_find_order(db, order_id, db_collection=CLN_ACTIVE_ORDERS)
    if order_data is None:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail=f'No active order with ID: {order_object_id}', status_code=status_code)
    order_update = {
        'status': ORDER_STATUS_PENDING,
        'lastUpdated': await time_w_timezone(),
    }
    result = await db_update_order(db, order_id, order_update, db_name=DB_PMK_NAME, db_collection=CLN_ACTIVE_ORDERS)
    if 0 == result.modified_count:
        status_code = status.HTTP_304_NOT_MODIFIED
        raise HTTPException(detail=f'already {ORDER_STATUS_IN_PROGRESS}', status_code=status_code)
    return Response(status_code=status_code)


@router.post(
    path='/{order_object_id}/complete',
    description='Complete Any Active Order',
    status_code=status.HTTP_200_OK,
    response_description='Simple status code responses. For now.'
)
async def complete_order(
        order_object_id: str = Path(..., description='`objectId` of the order'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    status_code = status.HTTP_200_OK
    order_id = await get_object_id(order_object_id)
    order_data = await db_find_order(db, order_id, db_name=DB_PMK_NAME, db_collection=CLN_ACTIVE_ORDERS)
    if order_data is None:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail=f'No active order with ID: {order_object_id}', status_code=status_code)
    if ORDER_STATUS_IN_PROGRESS != order_data['status']:
        status_code = status.HTTP_409_CONFLICT
        raise HTTPException(detail=f"Order exist, but didn't started", status_code=status_code)
    # Dunno about rechecking SOURCE and DESTINATION.
    # Like if they're corrupted in some way and changed before we complete it.
    # Maybe we should recheck them and see if affected WheelStacks and Wheels are still here.
    # But it's extra moves, and I think it's too much.
    # Because we should be able to just complete order, and never create False ones.
    # So, stick to just completing order for now.
    if ORDER_MOVE_WHOLE_STACK == order_data['orderType']:
        result = await orders_complete_whole_wheelstack_move(db, order_data)
        print(result)
        return Response(status_code=status_code)


@router.post(
    path='/{order_object_id}/cancel',
    description='Cancel Any Active Order',
    status_code=status.HTTP_200_OK,
    response_description='Simple status code responses.'
)
async def cancel_order(
        order_object_id: str = Path(..., description='objectId of the order'),
        cancellation_reason: str = Body(default='N/A', description='Basic description of why it was cancelled'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    status_code = status.HTTP_200_OK
    order_id = await get_object_id(order_object_id)
    order_data = await db_find_order(db, order_id)
    if order_data is None:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail=f'No active order with ID: {order_object_id}', status_code=status_code)
    # What if order `inProgress`?
    # We should return them to the previous state.
    # So, we need to create an order with placement of already
    #  moved WheelStack to its previous place.
    #  For now, let's block cancellation for `inProgress`.
    # Later, we will need to create a return Order which we will need to mark
    #  `wheelStack` which was `inProgress` of transportation to its original place.
    if ORDER_STATUS_IN_PROGRESS == order_data['status']:
        status_code = status.HTTP_409_CONFLICT
        raise HTTPException(detail=f"Can't cancel currently `inProgress` orders", status_code=status_code)
    del_result = await db_delete_order(db, order_id, 'activeOrders')
    #  Actually, we're always search for order before deletion.
    #  So, it always should be here for deletion, but let's leave it.
    if 0 == del_result.deleted_count:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail=f'No active order with ID: {order_object_id}', status_code=status_code)
    # MOVE_WHOLE_STACK == block of destination cell and block of the source wheelStack
    if ORDER_MOVE_WHOLE_STACK == order_data['orderType']:
        destination = order_data['destination']['type']
        destination_row, destination_column = order_data['destination']['identifier'].split(',')
        destination_preset = await get_preset(destination)
        await orders_unblock_placement(db, destination_row, destination_column, destination, destination_preset)
        wheelstack_object_id = order_data['affectedWheelStacks']['source']
        await orders_cancel_unblock_wheelstack(db, wheelstack_object_id)
    order_data['status'] = 'canceled'
    order_data['canceledAt'] = datetime.now(timezone.utc)
    order_data['cancellationReason'] = cancellation_reason
    order_data['lastUpdated'] = datetime.now(timezone.utc)
    await db_create_order(db, order_data, db_collection='canceledOrders')
    return Response(status_code=status_code)
