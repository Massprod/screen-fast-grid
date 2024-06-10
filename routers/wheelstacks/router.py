from fastapi import APIRouter, Depends, HTTPException, status, Path, Body
from fastapi.responses import JSONResponse, Response
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger
from .models.models import CreateWheelStackRequest, UpdateWheelStackRequest
from .models.response_models import WheelsStackStandardResponse
from .crud import (db_insert_wheelstack, db_delete_wheelstack, wheelstacks_make_json_friendly,
                   db_update_wheelstack, db_find_wheelstack, db_find_wheelstack_by_pis)
from bson.errors import InvalidId
from bson import ObjectId


router = APIRouter()


@router.post(
    path='/',
    description='Create New Wheelstack',
    status_code=status.HTTP_201_CREATED,
    response_description='The details of the newly created wheelstack',
    response_model=WheelsStackStandardResponse,
    # responses={},  # add examples
)
async def create_wheelstack(
        wheel_stack: CreateWheelStackRequest = Body(
            ...,
            description="Every parameter of the `wheelStack` is mandatory,"
                        " except the `lastChange`. Because this `wheelStack` might be never changed.",
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheel_stack_data = wheel_stack.dict()
    resp = WheelsStackStandardResponse()
    status_code = status.HTTP_201_CREATED
    original_pis_id = wheel_stack_data['originalPisId']
    duplicate = await db_find_wheelstack_by_pis(db, original_pis_id)
    if duplicate is not None:
        status_code = status.HTTP_302_FOUND
        resp.set_status(status_code)
        resp.set_pis_duplicate_message(original_pis_id)
        return JSONResponse(content=resp.dict(), status_code=status_code)
    # Check if placement in GRID is FREE.
    # Otherwise, raise 409.
    result = await db_insert_wheelstack(db, wheel_stack_data)
    resp.set_status(status_code)
    resp.set_create_message(result.inserted_id)
    wheel_stack_data['_id'] = result.inserted_id
    resp.data = await wheelstacks_make_json_friendly(wheel_stack_data)
    return JSONResponse(content=resp.dict(), status_code=status_code)


@router.get(
    path='/{wheelstack_object_id}',
    description='Search Created Wheelstack',
    response_description='All the data of searched `wheelStack`',
    response_model=WheelsStackStandardResponse,
)
async def find_wheelstack(
        wheelstack_object_id: str = Path(description='`objectId` of stored wheelstack'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    status_code = status.HTTP_200_OK
    # change to a utility function, because it should be used everywhere.
    try:
        object_id = ObjectId(wheelstack_object_id)
    except InvalidId as e:
        status_code = status.HTTP_400_BAD_REQUEST
        raise HTTPException(detail=str(e), status_code=status_code)
    resp = WheelsStackStandardResponse()
    result = await db_find_wheelstack(db, object_id)
    if result is None:
        status_code = status.HTTP_404_NOT_FOUND
        resp.set_duplicate_message(wheelstack_object_id)
        raise HTTPException(detail=resp.dict(), status_code=status_code)
    resp.set_status(status_code)
    resp.set_found_message(wheelstack_object_id)
    resp.data = await wheelstacks_make_json_friendly(result)
    return JSONResponse(content=resp.dict(), status_code=status_code)


@router.put(
    path='/{wheelstack_object_id}',
    description='Update Created Wheelstack',
    response_description='Simple 200 or 404',
)
async def update_wheelstack(
        wheelstack_new_data: UpdateWheelStackRequest,
        wheelstack_object_id: str = Path(description='`objectId` of stored wheelstack'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    status_code = status.HTTP_200_OK
    try:
        object_id = ObjectId(wheelstack_object_id)
    except InvalidId as e:
        status_code = status.HTTP_400_BAD_REQUEST
        raise HTTPException(detail=str(e), status_code=status_code)
    result = await db_update_wheelstack(db, object_id, wheelstack_new_data.dict())
    if 0 == result.modified_count:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail='Not found', status_code=status_code)
    return Response(status_code=status_code)


@router.delete(
    path='/{wheelstack_object_id}',
    description='Delete Created Wheelstack',
    response_description='Simple 200 or 404'
)
async def delete_wheelstack(
        wheelstack_object_id: str = Path(description='`objectId` of a stored wheelstack'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    status_code = status.HTTP_200_OK
    try:
        object_id = ObjectId(wheelstack_object_id)
    except InvalidId as e:
        status_code = status.HTTP_400_BAD_REQUEST
        raise HTTPException(detail=str(e), status_code=status_code)
    result = await db_delete_wheelstack(db, object_id)
    if 0 == result.deleted_count:
        status_code = status.HTTP_404_NOT_FOUND
        return Response(status_code=status_code)
    return Response(status_code=status_code)
