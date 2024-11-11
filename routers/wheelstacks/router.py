import asyncio
from bson import ObjectId
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.responses import JSONResponse, Response
from routers.grid.crud import place_wheelstack_in_grid
from motor.motor_asyncio import AsyncIOMotorClientSession
from routers.storages.crud import db_storage_place_wheelstack
from auth.jwt_validation import get_role_verification_dependency
from routers.history.history_actions import background_history_record
from routers.wheels.crud import db_find_wheel_by_object_id, db_update_wheel
from routers.batch_numbers.crud import db_find_batch_number, db_create_batch_number
from utility.utilities import get_object_id, time_w_timezone
from fastapi import APIRouter, Depends, HTTPException, status, Path, Body, BackgroundTasks
from .models.models import CreateWheelStackRequest, ForceUpdateWheelStackRequest, WheelsData
from routers.base_platform.crud import cell_exist, place_wheelstack_in_platform, get_platform_by_object_id
from constants import (
    CLN_GRID,
    CLN_STORAGES,
    DB_PMK_NAME,
    CLN_WHEELSTACKS,
    CLN_BASE_PLATFORM,
    CLN_WHEELS,
    CLN_BATCH_NUMBERS,
    BASIC_PAGE_VIEW_ROLES,
    ADMIN_ACCESS_ROLES,
    CELERY_ACTION_ROLES,
    BASIC_PAGE_ACTION_ROLES,
    PT_BASE_PLATFORM,
    PT_GRID,
    PT_STORAGE,
    WH_UNPLACED,
)
from .crud import (
    db_find_all_wheelstacks,
    db_insert_wheelstack,
    db_delete_wheelstack,
    wheelstack_make_json_friendly,
    all_make_json_friendly,
    db_update_wheelstack,
    db_find_wheelstack_by_object_id,
    db_get_wheelstack_last_change,
)


router = APIRouter()


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
        background_tasks: BackgroundTasks,
        wheelstack: CreateWheelStackRequest = Body(
            ...,
            description="Every parameter of the `wheelStack` is mandatory,"
                        " except the `lastChange`. Because this `wheelStack` might be never changed.",
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES | CELERY_ACTION_ROLES),
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
            # + BG record +
            placement_id = correct_data['placement']['placementId']
            placement_type = correct_data['placement']['type']
            background_tasks.add_task(background_history_record, placement_id, placement_type, db)
            # - BG record -
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
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
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
        background_tasks: BackgroundTasks,
        wheelstack_new_data: ForceUpdateWheelStackRequest,
        wheelstack_object_id: str = Path(description='`objectId` of stored wheelstack'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(ADMIN_ACCESS_ROLES),
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
    # + BG record +
    previous_placement_id = wheelstack_exist['placement']['placementId']
    previous_placement_type = wheelstack_exist['placement']['type']
    new_placement_id = force_data['placement']['placementId']
    new_placement_type = force_data['placement']['type']
    if new_placement_id == previous_placement_id:
        background_tasks.add_task(
            background_history_record, new_placement_id, new_placement_type, db
        )
    else:
        background_tasks.add_task(
            background_history_record, new_placement_id, new_placement_type, db
        )
        background_tasks.add_task(
            background_history_record, previous_placement_id, previous_placement_type, db
        )
    # - BG record -
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
        token_data: dict = get_role_verification_dependency(ADMIN_ACCESS_ROLES),
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
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
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
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    wheelstack_id: ObjectId = await get_object_id(wheelstack_object_id)
    res = await db_get_wheelstack_last_change(wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
    if res is None:
        raise HTTPException(
            detail=f'`wheelstack` with provided `objectId` = {wheelstack_object_id}. Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    res['_id'] = str(res['_id'])
    res['lastChange'] = res['lastChange'].isoformat()
    return JSONResponse(
        content=res,
        status_code=status.HTTP_200_OK,
    )


# TODO: move to normal place :)
async def place_wheelstack_filter(
        wheelstack_data: dict,
        db: AsyncIOMotorClient,
        session: AsyncIOMotorClientSession,
):
    placement_id: ObjectId = wheelstack_data['placement']['placementId']
    placement_type: str = wheelstack_data['placement']['type']
    placement_row: str = wheelstack_data['rowPlacement']
    placement_col: str = wheelstack_data['colPlacement']
    if PT_BASE_PLATFORM == placement_type:
        await place_wheelstack_in_platform(
            placement_id, wheelstack_data['_id'], placement_row, placement_col, db, DB_PMK_NAME, CLN_BASE_PLATFORM, session
        )
    elif PT_GRID == placement_type:
        await place_wheelstack_in_grid(
            placement_id, wheelstack_data['_id'], placement_row, placement_col, db, DB_PMK_NAME, CLN_GRID, session 
        )
    elif PT_STORAGE == placement_type:
        await db_storage_place_wheelstack(
            placement_id, '', wheelstack_data['_id'], db, DB_PMK_NAME, CLN_STORAGES, session, True
        )



@router.patch(
    path='/reconstruct/{target_object_id}',
    description='Reconstruct `wheelstack` with another wheels. Setting changed `wheel`s to `unplaced`',
    name='Reconstruct',
)
async def route_patch_rebuild_wheelstack(
    new_wheels_data: WheelsData = Body(...,
                                       description='New reconstructed wheels data'),
    target_object_id: str = Path(...,
                                     description='`ObjectId` of the `wheelstack` to use'),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
    token_data: dict = get_role_verification_dependency(BASIC_PAGE_ACTION_ROLES),
):
    new_wheels_data = new_wheels_data.model_dump()['wheels']
    new_wheels: set[ObjectId] = set()
    # 1. Correct `ObjectId`s
    for index, wheel in enumerate(new_wheels_data):
        wheel_object_id: ObjectId = await get_object_id(wheel)
        new_wheels_data[index] = wheel_object_id
        new_wheels.add(wheel_object_id)
    # 2. Wheelstack exists
    wheelstack_id: ObjectId = await get_object_id(target_object_id)
    exists: dict = await db_find_wheelstack_by_object_id(
        wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if exists is None:
        raise HTTPException(
            detail=f'`wheelstack` with provided `objectId` = {wheelstack_id}. Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if exists['blocked']:
        raise HTTPException(
            detail=f'`wheelstack` with provided `objectId` = {wheelstack_id}. Cant be altered while its blocked.',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    wheelstack_batch: str = exists['batchNumber']
    # 3. All wheels exists + correct batch.
    async def check_wheel(wheel_object_id: ObjectId):
        get_wheel = await db_find_wheel_by_object_id(wheel, db, DB_PMK_NAME, CLN_WHEELS)
        if get_wheel is None:
            raise HTTPException(
                detail=f'One of provided wheels doesnt exist => {wheel_object_id}',
                status_code=status.HTTP_404_NOT_FOUND,
            )
        if wheelstack_batch != get_wheel['batchNumber']:
            raise HTTPException(
                detail=f'One of provided wheels doesnt have correct wheelstack `batchNumber` => {wheel_object_id}',
                status_code=status.HTTP_403_FORBIDDEN,
            )

    get_wheel_tasks = []
    for wheel in new_wheels:
        get_wheel_tasks.append(check_wheel(wheel))
    get_wheel_results = await asyncio.gather(*get_wheel_tasks)
    # 4. Check for unplaced wheels. Wheels which is not present in new_wheels => goes to unplaced.
    current_wheels: set[ObjectId] = exists['wheels']
    unplaced_wheels: set[ObjectId] = set()
    for wheel in current_wheels:
        if wheel not in new_wheels:
            unplaced_wheels.add(wheel)
    # 5. Update wheelstack + Update unplaced wheels + Update placement change time
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            # 5.1 Update placed
            transaction_tasks = []
            for index, wheel in enumerate(new_wheels_data):
                new_wheel_data = {
                    'wheelStack': {
                        'wheelStackId': exists['_id'],
                        'wheelStackPosition': index,
                    },
                    'status': exists['status']
                }
                transaction_tasks.append(
                    db_update_wheel(
                        wheel, new_wheel_data, db, DB_PMK_NAME, CLN_WHEELS, session
                ))
            # 5.2 Update unplaced
            for wheel in unplaced_wheels:
                new_wheel_data = {
                    'wheelStack': None,
                    'status': WH_UNPLACED
                }
                transaction_tasks.append(
                    db_update_wheel(
                        wheel, new_wheel_data, db, DB_PMK_NAME, CLN_WHEELS, session
                ))
            # 5.3 Update wheelstack
            exists['wheels'] = new_wheels_data
            transaction_tasks.append(
                db_update_wheelstack(
                    exists, exists['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session, True
                )
            )
            # 5.4 Update placement
            transaction_tasks.append(
                place_wheelstack_filter(
                    exists, db, session
                )
            )
            await asyncio.gather(*transaction_tasks)
    return Response(status_code=status.HTTP_200_OK)
