from fastapi import APIRouter, Depends, HTTPException, status, Path, Body
from fastapi.responses import JSONResponse, Response
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from .models.models import CreateWheelStackRequest, ForceUpdateWheelStackRequest
from .crud import (db_find_all_wheelstacks, db_insert_wheelstack,
                   db_delete_wheelstack, wheelstack_make_json_friendly,
                   all_make_json_friendly, db_update_wheelstack,
                   db_find_wheelstack_by_object_id, db_find_wheelstack_by_pis,
                   db_get_wheelstack_last_change)
from bson import ObjectId
from utility.utilities import get_object_id, time_w_timezone
from constants import DB_PMK_NAME, CLN_WHEELSTACKS, CLN_BASE_PLATFORM, CLN_WHEELS, CLN_BATCH_NUMBERS
from routers.wheels.crud import db_find_wheel_by_object_id, db_update_wheel
from routers.base_platform.crud import cell_exist, place_wheelstack_in_platform, get_platform_by_object_id
from routers.batch_numbers.crud import db_find_batch_number, db_create_batch_number


router = APIRouter()


# TODO: Currently, we don't need to search by anything except `objectId`.
#  but it still good to add some options, to search by `originalPisId` or `row|col` placement etc.
#  Maybe add these after first correct version.
#  Or if it's needed.


@router.post(
    path='/',
    description='Create and place a new `wheelStack` in the chosen `basePlatform`',
    status_code=status.HTTP_201_CREATED,
    response_description='`objectId` of the created wheelstack',
    response_class=JSONResponse,
    # responses={},  # add examples
    name='Create Wheelstack'
)
async def route_create_wheelstack(
        wheelstack: CreateWheelStackRequest = Body(
            ...,
            description="Every parameter of the `wheelStack` is mandatory,"
                        " except the `lastChange`. Because this `wheelStack` might be never changed.",
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheelstack_data = wheelstack.model_dump()
    # +++ Placement exist
    placement_id: ObjectId = await get_object_id(wheelstack_data['placementId'])
    platform = await get_platform_by_object_id(placement_id, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if platform is None:
        raise HTTPException(
            detail='Incorrect `placementId` it doesnt exist',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    # Placement exist ---
    original_pis_id = wheelstack_data['originalPisId']
    # +++ Duplicate
    duplicate = await db_find_wheelstack_by_pis(original_pis_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
    if duplicate is not None:
        return JSONResponse(
            content={
                'duplicate': f'`wheelStack` with `originalPisId` = {original_pis_id}. Already exist.'
            },
            status_code=status.HTTP_302_FOUND
        )
    # Duplicate ---
    # +++ Wheels:
    # Checking for a correct `objectId` type and `wheel` should already exist,
    #   and it shouldn't be yet assigned.
    cor_wheels: list[ObjectId] = []
    for wheel in wheelstack_data['wheels']:
        wheel_id: ObjectId = await get_object_id(wheel)
        exist = await db_find_wheel_by_object_id(wheel_id, db, DB_PMK_NAME, CLN_WHEELS)
        if exist is None:
            raise HTTPException(
                detail=f'`wheel` with given `objectId` = {wheel}. Not Found.',
                status_code=status.HTTP_404_NOT_FOUND,
            )
        if exist['wheelStack'] is not None:
            raise HTTPException(
                detail=f'`wheel` with given `objectId` = {wheel}.'
                       f' Already Assigned to = {exist['wheelStack']}',
                status_code=status.HTTP_409_CONFLICT,
            )
        if exist['batchNumber'] != wheelstack_data['batchNumber']:
            raise HTTPException(
                detail=f'All of the wheels in the `wheelstack` should have the same `batchNumber`.'
                       f' And it should be equal to the `batchNumber` of the `wheelstack`.',
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        cor_wheels.append(wheel_id)
    # Wheels ---
    # +++ Placement empty
    row: str = wheelstack_data['rowPlacement']
    col: str = wheelstack_data['colPlacement']
    platform_id: ObjectId = await get_object_id(wheelstack_data['placementId'])
    placement_data = await cell_exist(platform_id, row, col, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if placement_data is None:
        raise HTTPException(
            detail=f'Incorrect placement cell data,'
                   f' no such cell exist in placement with `objectId` = {str(platform_id)}',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    placement_wheelstack = placement_data['rows'][row]['columns'][col]['wheelStack']
    if placement_wheelstack is not None:
        raise HTTPException(
            detail=f'Placement cell already taken by `wheelStack` with `objectId` = {placement_wheelstack}',
            status_code=status.HTTP_302_FOUND,
        )
    # Placement empty ---
    # +++ Placement blocked
    placement_blocked = placement_data['rows'][row]['columns'][col]['blocked']
    if placement_blocked:
        raise HTTPException(
            detail='Placement cell marked as `blocked`. Waiting for order',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    # Placement blocked ---
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            batch_number_exist = await db_find_batch_number(
                wheelstack_data['batchNumber'], db, DB_PMK_NAME, CLN_BATCH_NUMBERS, session
            )
            if batch_number_exist is None:
                new_batch_number_data: dict = {
                    'batchNumber': wheelstack_data['batchNumber'],
                    'laboratoryPassed': False,
                    'laboratoryTestDate': None,
                }
                await db_create_batch_number(
                    new_batch_number_data, db, DB_PMK_NAME, CLN_BATCH_NUMBERS, session
                )
            creation_time = await time_w_timezone()
            correct_data = {
                'originalPisId': wheelstack_data['originalPisId'],
                'batchNumber': wheelstack_data['batchNumber'],
                'placement': {
                    'type': wheelstack_data['placementType'],
                    'placementId': placement_id,
                },
                'rowPlacement': wheelstack_data['rowPlacement'],
                'colPlacement': wheelstack_data['colPlacement'],
                'createdAt': creation_time,
                'lastChange': creation_time,
                'lastOrder': None,
                'maxSize': wheelstack_data['maxSize'],
                'blocked': False,
                'wheels': cor_wheels,
                'status': wheelstack_data['status'],
            }
            result = await db_insert_wheelstack(correct_data, db, DB_PMK_NAME, CLN_WHEELSTACKS)
            created_id: ObjectId = result.inserted_id
            await place_wheelstack_in_platform(placement_id, created_id, row, col, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
            # +++ Marking Wheels
            for index, wheel_id in enumerate(cor_wheels):
                record: dict = {
                    'wheelStack': {
                        'wheelStackId': created_id,
                        'wheelStackPosition': index,
                    }
                }
                await db_update_wheel(wheel_id, record, db, DB_PMK_NAME, CLN_WHEELS)
            # Marking Wheels ---
            return JSONResponse(
                content={
                    '_id': str(created_id)
                },
                status_code=status.HTTP_201_CREATED,
            )


@router.get(
    path='/id/{wheelstack_object_id}',
    description='Search Created Wheelstack',
    response_description='All the data of searched `wheelStack`',
    response_class=JSONResponse,
    name='Find Wheelstack',
)
async def route_find_wheelstack(
        wheelstack_object_id: str = Path(description='`objectId` of the wheelstack'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    wheelstack_id: ObjectId = await get_object_id(wheelstack_object_id)
    result = await db_find_wheelstack_by_object_id(wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
    if result is None:
        raise HTTPException(
            detail=f'`wheelStack` with `objectId` = {wheelstack_object_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND
        )
    result = await wheelstack_make_json_friendly(result)
    return JSONResponse(
        content=result,
        status_code=status.HTTP_200_OK,
    )


@router.put(
    path='/{wheelstack_object_id}',
    description=" `WARNING` "
                "\nCompletely free way of updating Wheelstack,"
                " that's why it crucial to use it with caution."
                "\nBecause it should be used only for the First time filling process."
                "\nIt doesn't take any effects on actual placement of the `wheelStack`s in DB."
                "\nIt's only changes `wheelStack` record in DB, no dependencies with anything.",
    name='Force Update',
)
async def route_force_update_wheelstack(
        wheelstack_new_data: ForceUpdateWheelStackRequest,
        wheelstack_object_id: str = Path(description='`objectId` of stored wheelstack'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    new_data = wheelstack_new_data.model_dump()
    wheelstack_id = await get_object_id(wheelstack_object_id)
    wheelstack_exist = await db_find_wheelstack_by_object_id(wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
    if wheelstack_exist is None:
        raise HTTPException(
            detail=f'`wheelStack` with `objectId` = {wheelstack_object_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    cor_wheels: list[ObjectId] = []
    # Because it's a forced update, without dependencies, we don't check anything.
    # Except for correct data_type.
    for wheel in new_data['wheels']:
        cor_wheels.append(await get_object_id(wheel))
    force_data = {
        'originalPisId': new_data['originalPisId'],
        'batchNumber': new_data['batchNumber'],
        'placement': {
            'type': new_data['placementType'],
            'placementId': await get_object_id(new_data['placementId']),
        },
        'rowPlacement': new_data['rowPlacement'],
        'colPlacement': new_data['colPlacement'],
        'lastChange': await time_w_timezone(),
        'lastOrder': new_data['lastOrder'],
        'blocked': new_data['blocked'],
        'wheels': cor_wheels,
        'status': new_data['status'],
    }
    result = await db_update_wheelstack(force_data, wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
    if 0 == result.modified_count:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)
    return Response(status_code=status.HTTP_200_OK)


@router.delete(
    path='/{wheelstack_object_id}',
    description='`WARNING`'
                '\nDeletion of the `wheelStack` without dependencies.'
                '\nOnly deletes record of provided `objectId` from a collection.',
    name='Force Delete',
)
async def route_force_delete_wheelstack(
        wheelstack_object_id: str = Path(description='`objectId` of a stored wheelstack'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheelstack_id: ObjectId = await get_object_id(wheelstack_object_id)
    result = await db_delete_wheelstack(wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
    if 0 == result.deleted_count:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)
    return Response(status_code=status.HTTP_200_OK)


@router.get(
    path='/all',
    description='Getting all of the `wheelStack`s present in DB',
    name='Get All',
)
async def route_get_all_wheelstacks(
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    data = await db_find_all_wheelstacks(db, DB_PMK_NAME, CLN_WHEELSTACKS)
    resp = await all_make_json_friendly(data)
    return JSONResponse(
        content=resp,
        status_code=status.HTTP_200_OK,
    )


@router.get(
    path='/change_time/{wheelstack_object_id}',
    description='Getting `lastChange` of the chosen `wheelstack`',
    name='Get Last Change',
)
async def route_get_last_change(
        wheelstack_object_id: str = Path(...,
                                         description='`objectId` of the `wheelStack` to search'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    wheelstack_id: ObjectId = await get_object_id(wheelstack_object_id)
    res = await db_get_wheelstack_last_change(wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
    if res is None:
        raise HTTPException(
            detail=f'grid with `objectId` = {wheelstack_object_id}. Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    res['_id'] = str(res['_id'])
    res['lastChange'] = res['lastChange'].isoformat()
    return JSONResponse(
        content=res,
        status_code=status.HTTP_200_OK,
    )
