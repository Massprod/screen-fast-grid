from bson import ObjectId
from loguru import logger
from utility.utilities import get_object_id
from .models.models import CreateWheelRequest
from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_connection import mongo_client
from fastapi.responses import JSONResponse, Response
from ..base_platform.crud import db_update_platform_last_change
from auth.jwt_validation import get_role_verification_dependency
from ..batch_numbers.crud import db_find_batch_number, db_create_batch_number
from fastapi import APIRouter, Depends, HTTPException, status, Path, Body, Query
from routers.wheelstacks.crud import db_find_wheelstack_by_object_id, db_update_wheelstack
from constants import (
    DB_PMK_NAME,
    CLN_WHEELS,
    PS_BASE_PLATFORM,
    CLN_WHEELSTACKS,
    CLN_BASE_PLATFORM,
    CLN_BATCH_NUMBERS,
    BASIC_PAGE_VIEW_ROLES,
    ADMIN_ACCESS_ROLES,
    BASIC_PAGE_ACTION_ROLES,
)
from .models.response_models import (
    update_response_examples,
    find_response_examples,
)
from .crud import (
    db_insert_wheel,
    db_find_wheel,
    db_update_wheel,
    db_delete_wheel,
    wheel_make_json_friendly,
    db_find_wheel_by_object_id,
    db_get_all_wheels
)


router = APIRouter()


@router.get(
    path='/all',
    name='Get All',
    description='Return every wheel present in the `wheel`s collection',
)
async def route_get_all_wheels(
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        batch_number: str = Query('',
                                  description='Select all with given `batchNumber`'),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    result = await db_get_all_wheels(batch_number, db, DB_PMK_NAME, CLN_WHEELS)
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
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
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
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
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
                "Forcing update without any dependencies."
                "It should be used, only when we need to change DB record by `Hand`",
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
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_ACTION_ROLES),
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
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_ACTION_ROLES),
):
    # TODO: We need to extra check wheel Diameters when we creating and placing wheel in a stack.
    #  Because STACK can only contain same sized wheels.
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
    # We only create wheel, either placed in a fresh `wheelStack` or with empty placement.
    wheelstack_data = None
    if 'wheelStack' in wheel_data and wheel_data['wheelStack'] is not None:
        wheelstack_id = await get_object_id(wheel_data['wheelStack']['wheelStackId'])
        wheelstack_position = wheel_data['wheelStack']['wheelStackPosition']
        # So, they should be placed in correct order if we somehow decided to create
        #  wheels earlier than `wheelStack`.
        # Correctly, we should create an empty `wheelStack` => create all wheels and place them.
        wheelstack_data = await db_find_wheelstack_by_object_id(wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
        if wheelstack_data is None:
            raise HTTPException(
                detail=f'Given `wheelStack` with `objectId` = {wheelstack_id} doesnt exist. Not Found',
                status_code=status.HTTP_404_NOT_FOUND,
            )
        if wheelstack_data['batchNumber'] != wheel_data['batchNumber']:
            raise HTTPException(
                detail=f'`wheelStacks` can only contain same `batchNumber` wheels = {wheelstack_data['batchNumber']}',
                status_code=status.HTTP_403_FORBIDDEN,
            )
        current_wheels: list[ObjectId] = wheelstack_data['wheels']
        if len(current_wheels) == wheelstack_data['maxSize']:
            raise HTTPException(
                detail=f'Wheelstack doesnt have empty positions, `maxSize`: {wheelstack_data['maxSize']}',
                status_code=status.HTTP_403_FORBIDDEN,
            )
        # Correct order => place1 -> place2 etc. and we always use position as (last_index + 1)
        if wheelstack_position != len(current_wheels):
            raise HTTPException(
                detail=f'Incorrect `wheelStackPosition` wheels should be placed one after another.'
                       f'Current # of placed wheels: {len(current_wheels)}',
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if wheelstack_data['status'] != wheel_data['status']:
            raise HTTPException(
                detail=f'Incorrect `status` it should correspond with `wheelStack` status',
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        cor_data['wheelStack'] = {
            'wheelStackId': wheelstack_id,
            'wheelStackPosition': wheelstack_position,
        }
    else:
        cor_data['wheelStack'] = None
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            result = await db_insert_wheel(cor_data, db, DB_PMK_NAME, CLN_WHEELS, session)
            cor_data['_id'] = result.inserted_id
            if wheelstack_data is None:
                batch_number_exist = await db_find_batch_number(
                    cor_data['batchNumber'], db, DB_PMK_NAME, CLN_BATCH_NUMBERS, session
                )
                logger.error(f'found existing batch {batch_number_exist}')
                if batch_number_exist is None:
                    new_batch_number_data: dict = {
                        'batchNumber': cor_data['batchNumber'],
                        'laboratoryPassed': False,
                        'laboratoryTestDate': None,
                    }
                    await db_create_batch_number(
                        new_batch_number_data, db, DB_PMK_NAME, CLN_BATCH_NUMBERS
                    )
            if wheelstack_data is not None:
                wheelstack_data['wheels'].append(result.inserted_id)
                await db_update_wheelstack(
                    wheelstack_data, wheelstack_data['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session
                )
                await db_update_platform_last_change(
                    wheelstack_data['placement']['placementId'], db, DB_PMK_NAME, CLN_BASE_PLATFORM, session
                )
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
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_ACTION_ROLES),
):
    wheel_id: ObjectId = await get_object_id(wheel_object_id)
    result = await db_delete_wheel(wheel_id, db, DB_PMK_NAME, CLN_WHEELS)
    if 0 == result.deleted_count:
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    return Response(status_code=status.HTTP_200_OK)
