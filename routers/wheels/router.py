from .crud import db_insert_wheel, db_find_wheel, db_update_wheel, db_delete_wheel, wheels_make_json_friendly
from fastapi import APIRouter, Depends, HTTPException, status, Path, Body
from fastapi.responses import JSONResponse, Response
from database.mongo_connection import mongo_client
from .models.models import CreateWheelRequest
from .models.response_models import (WheelsStandardResponse,
                                     update_response_examples,
                                     find_response_examples,
                                     create_response_examples)
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger


router = APIRouter()


@router.get(
    path='/{wheel_id}',
    name='Find Wheel',
    description="Retrieve the details of a wheel by it's ID",
    response_class=JSONResponse,
    status_code=status.HTTP_200_OK,
    response_description='The details of the wheel, if found',
    responses={
        status.HTTP_200_OK: find_response_examples[status.HTTP_200_OK],
        status.HTTP_404_NOT_FOUND: find_response_examples[status.HTTP_404_NOT_FOUND],
    }
)
async def find_wheel(
        wheel_id: str = Path(description="The ID of the wheel to retrieve", example="W12345"),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    logger.info(f"Searching for a wheel with ID: {wheel_id}")
    result = await db_find_wheel(db, wheel_id)
    resp = WheelsStandardResponse()
    status_code = status.HTTP_200_OK
    if result is None:
        logger.warning(f"Wheel with ID: {wheel_id} not found")
        status_code = status.HTTP_404_NOT_FOUND
        resp.set_status(status_code)
        resp.set_not_found_message(wheel_id)
        raise HTTPException(detail=resp.dict(), status_code=status_code)
    logger.info(f"Wheel with ID: {wheel_id} found")
    resp.set_status(status_code)
    resp.set_found_message(wheel_id)
    resp.data = await wheels_make_json_friendly(result)
    return JSONResponse(content=resp.dict(), status_code=status_code)


@router.put(
    path='/',
    name='Update Wheel',
    description="Update the details of a wheel",
    response_class=JSONResponse,
    status_code=status.HTTP_200_OK,
    response_description='The details of the wheel after update',
    responses={
        status.HTTP_200_OK: update_response_examples[status.HTTP_200_OK],
        status.HTTP_404_NOT_FOUND: update_response_examples[status.HTTP_404_NOT_FOUND],
        status.HTTP_304_NOT_MODIFIED: update_response_examples[status.HTTP_304_NOT_MODIFIED],
    }
)
async def update_wheel(
        wheel: CreateWheelRequest = Body(
            ...,
            description='Every parameter of the wheel is mandatory,'
                        ' except the `wheelStack`.'
                        ' It can be a `null` otherwise it should contain data of the existing `wheelStack` in a DB.',
            example={
                "wheelId": "W12345",
                "batchNumber": "B54321",
                "wheelDiameter": 650,
                "receiptDate": "2024-05-30T11:56:16.209000+00:00",
                "status": "laboratory",
                "wheelStack": None
            }
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheel_data = wheel.dict()
    wheel_id = wheel_data['wheelId']
    logger.info(f"Updating wheel with ID: {wheel_id}")
    result = await db_update_wheel(db, wheel_data)
    resp = WheelsStandardResponse()
    status_code: int = status.HTTP_200_OK
    if result.matched_count == 0:
        logger.warning(f"Wheel with ID: {wheel_id} not found")
        status_code = status.HTTP_404_NOT_FOUND
        resp.set_status(status_code)
        resp.set_not_found_message(wheel_id)
        raise HTTPException(detail=resp.dict(), status_code=status_code)
    elif result.modified_count == 0:
        logger.info(f"Wheel with ID: {wheel_id} is already up to date")
        status_code = status.HTTP_304_NOT_MODIFIED
        return Response(status_code=status_code)
    logger.info(f"Wheel with ID: {wheel_id} has been updated")
    resp.set_status(status_code)
    resp.set_update_message(wheel_id)
    return JSONResponse(content=resp.dict(), status_code=status_code)


@router.post(
    path='/',
    description="Create New Wheel",
    response_class=JSONResponse,
    status_code=status.HTTP_201_CREATED,
    response_description='The details of the newly created wheel',
    responses={
        status.HTTP_201_CREATED: create_response_examples[status.HTTP_201_CREATED],
        status.HTTP_302_FOUND: create_response_examples[status.HTTP_302_FOUND],
        status.HTTP_500_INTERNAL_SERVER_ERROR: create_response_examples[status.HTTP_500_INTERNAL_SERVER_ERROR],
    }
)
async def create_wheel(
        wheel: CreateWheelRequest = Body(
            ...,
            description='Every parameter of the wheel is mandatory,'
                        ' except the `wheelStack`.'
                        ' It can be a `null` otherwise it should contain data of the existing `wheelStack` in a DB.',
            example={
                "wheelId": "W12345",
                "batchNumber": "B54321",
                "wheelDiameter": 650,
                "receiptDate": "2024-05-30T11:56:16.209000+00:00",
                "status": "laboratory"
            }
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheel_data = wheel.dict()
    wheel_id = wheel_data['wheelId']
    resp = WheelsStandardResponse()
    status_code: int = status.HTTP_201_CREATED
    result = await db_find_wheel(db, wheel_id)
    if result is not None:
        logger.warning(f"Wheel with provided `wheelId`={wheel_id}, already exists")
        status_code = status.HTTP_302_FOUND
        resp.set_status(status_code)
        resp.set_duplicate_message(wheel_id)
        raise HTTPException(detail=resp.dict(), status_code=status_code)
    result = await db_insert_wheel(db, wheel_data)
    wheel_data['_id'] = result.inserted_id
    resp.data = await wheels_make_json_friendly(wheel_data)
    resp.set_status(status_code)
    resp.set_create_message(wheel_id)
    return JSONResponse(content=resp.dict(), status_code=status_code)


@router.delete(
    path='/{wheel_id}',
    name='Delete wheel',
    description='Delete wheel from the DB, by its ID',
    response_class=JSONResponse,
    status_code=status.HTTP_200_OK,
    response_description='Simple 200 for correct deletion, or 404 if wheel doesnt exist',
)
async def delete_wheel(
        wheel_id: str = Path(description='The ID of the wheel to delete', example='W12345'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    result = await db_delete_wheel(db, wheel_id)
    status_code: int = status.HTTP_200_OK
    if 0 == result.deleted_count:
        status_code = status.HTTP_404_NOT_FOUND
        return Response(status_code=status_code)
    return Response(status_code=status_code)
