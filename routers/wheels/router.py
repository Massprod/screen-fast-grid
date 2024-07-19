from bson import ObjectId
from loguru import logger
from utility.utilities import get_object_id
from .models.models import CreateWheelRequest
from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_connection import mongo_client
from fastapi.responses import JSONResponse, Response
from fastapi import APIRouter, Depends, HTTPException, status, Path, Body
from constants import DB_PMK_NAME, CLN_WHEELS, PS_BASE_PLATFORM
from .models.response_models import (
                                     update_response_examples,
                                     find_response_examples,
                                     )
from .crud import (db_insert_wheel, db_find_wheel,
                   db_update_wheel, db_delete_wheel,
                   wheel_make_json_friendly, db_find_wheel_by_object_id,
                   db_get_all_wheels)


router = APIRouter()


@router.get(
    path='/all',
    name='Get All',
    description='Return every wheel present in the `wheel`s collection',
)
async def route_get_all_wheels(
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    result = await db_get_all_wheels(db, DB_PMK_NAME, CLN_WHEELS)
    cor_data: dict = {}
    for wheel in result:
        cor_data[wheel['_id']] = await wheel_make_json_friendly(wheel)
    return JSONResponse(
        content=cor_data,
        status_code=status.HTTP_200_OK,
    )


@router.get(
    path='/wheel_id/{wheel_id}',
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
async def route_find_wheel(
        wheel_id: str = Path(description="The ID of the wheel to retrieve"),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    logger.info(f"Searching for a wheel with ID: {wheel_id}")
    result = await db_find_wheel(wheel_id, db, DB_PMK_NAME, CLN_WHEELS)
    if result is None:
        logger.warning(f"Wheel with `wheelId`: {wheel_id}. Not found")
        raise HTTPException(
            detail=f'Wheel with `wheelId`: {wheel_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    logger.info(f"Wheel with ID: {wheel_id} found")
    result = await wheel_make_json_friendly(result)
    return JSONResponse(content=result, status_code=status.HTTP_200_OK)


@router.get(
    path='/object_id/{wheel_object_id}',
    name='Find Wheel ObjectId',
    description='Retrieve the details of a wheel by `objectId`',
    response_class=JSONResponse,
)
async def route_find_wheel_by_object_id(
        wheel_object_id: str = Path(description='`objectId` of the wheel to retrieve'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheel_id: ObjectId = await get_object_id(wheel_object_id)
    result = await db_find_wheel_by_object_id(wheel_id, db, DB_PMK_NAME, CLN_WHEELS)
    if result is None:
        logger.warning(f"Wheel with `objectId``: {wheel_id}. Not found")
        raise HTTPException(
            detail=f'Wheel with `objectId`: {wheel_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    result = await wheel_make_json_friendly(result)
    return JSONResponse(content=result, status_code=status.HTTP_200_OK)


@router.put(
    path='/{wheel_object_id}',
    name='Force Update',
    description="`WARNING`"
                "\nForce updating of the `wheel` data in DB"
                "\nNo dependencies will be changed, only `wheel` record itself",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_200_OK: update_response_examples[status.HTTP_200_OK],
        status.HTTP_404_NOT_FOUND: update_response_examples[status.HTTP_404_NOT_FOUND],
        status.HTTP_304_NOT_MODIFIED: update_response_examples[status.HTTP_304_NOT_MODIFIED],
    }
)
async def route_force_update_wheel(
        wheel_object_id: str = Path(...,
                                    description='`objectId` of the stored `wheel`'),
        wheel: CreateWheelRequest = Body(
            ...,
            description='Every parameter of the wheel is mandatory,'
                        ' except the `wheelStack`.'
                        ' It can be a `null` otherwise it should contain `objectId`'
                        ' of the existing `wheelStack` in a DB.',
            example={
                "wheelId": "W12345",
                "batchNumber": "B54321",
                "wheelDiameter": 650,
                "receiptDate": "2024-05-30T11:56:16.209000+00:00",
                "status": PS_BASE_PLATFORM,
                "wheelStack": None
            }
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheel_id = await get_object_id(wheel_object_id)
    wheel_data = wheel.model_dump()
    exist = await db_find_wheel_by_object_id(wheel_id, db, DB_PMK_NAME, CLN_WHEELS)
    if exist is None:
        raise HTTPException(
            detail='Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    logger.info(f"Updating wheel with `objectId`: {wheel_id}")
    if 'wheelStack' in wheel_data:
        wheel_data['wheelStack']['wheelStackId'] = await get_object_id(wheel_data['wheelStack']['wheelStackId'])
        if not 0 <= wheel_data['wheelStack']['wheelStackPosition'] < 7:
            raise HTTPException(
                detail=f'Incorrect position. 0 -> 6 (inclusive)',
                status_code=status.HTTP_400_BAD_REQUEST,
            )
    result = await db_update_wheel(wheel_id, wheel_data, db, DB_PMK_NAME, CLN_WHEELS)
    if result.modified_count == 0:
        logger.info(f'Wheel with `objectId`: {wheel_id}. Not Modified')
        return Response(
            status_code=status.HTTP_304_NOT_MODIFIED
        )
    logger.info(f"Wheel with ID: {wheel_id} has been updated")
    return Response(status_code=status.HTTP_200_OK)


@router.post(
    path='/',
    description="Create a new Wheel",
    response_class=JSONResponse,
    response_description='`objectId` of created `wheel`',
    name='Create Wheel',
)
async def route_create_wheel(
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
                "status": PS_BASE_PLATFORM,
            }
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheel_data = wheel.model_dump()
    wheel_id = wheel_data['wheelId']
    result = await db_find_wheel(wheel_id, db, DB_PMK_NAME, CLN_WHEELS)
    if result is not None:
        logger.warning(f"Wheel with provided `wheelId` = {wheel_id}. Already exists")
        raise HTTPException(
            detail=f'Wheel with `wheelId` = {wheel_id}. Already exists.',
            status_code=status.HTTP_302_FOUND
        )
    cor_data: dict = {
        'wheelId': wheel_data['wheelId'],
        'batchNumber': wheel_data['batchNumber'],
        'wheelDiameter': wheel_data['wheelDiameter'],
        'receiptDate': wheel_data['receiptDate'],
        'status': wheel_data['status'],
    }
    if 'wheelStack' in wheel_data:
        wheelstack_id = await get_object_id(wheel_data['wheelStack']['wheelStackId'])
        wheelstack_position = wheel_data['wheelStack']['wheelStackPosition']
        cor_data['wheelStack'] = {
            'wheelStackId': wheelstack_id,
            'wheelStackPosition': wheelstack_position,
        }
    else:
        cor_data['wheelStack'] = None
    result = await db_insert_wheel(cor_data, db, DB_PMK_NAME, CLN_WHEELS)
    cor_data['_id'] = result.inserted_id
    cor_data = await wheel_make_json_friendly(cor_data)
    return JSONResponse(
        content=cor_data,
        status_code=status.HTTP_200_OK
    )


@router.delete(
    path='/{wheel_object_id}',
    name='Force Delete',
    description='`WARNING`'
                '\nDeletes wheel from the DB, by its `objectId`.'
                '\nWithout clearing any dependencies.',
    response_class=JSONResponse,
    status_code=status.HTTP_200_OK,
    response_description='Simple 200 for correct deletion, or 404 if wheel doesnt exist',
)
async def route_delete_wheel(
        wheel_object_id: str = Path(description='`objectId` of the wheel to delete'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheel_id: ObjectId = await get_object_id(wheel_object_id)
    result = await db_delete_wheel(wheel_id, db, DB_PMK_NAME, CLN_WHEELS)
    if 0 == result.deleted_count:
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    return Response(status_code=status.HTTP_200_OK)
