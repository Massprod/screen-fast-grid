import asyncio
from bson import ObjectId
from database.mongo_connection import mongo_client
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.responses import JSONResponse, Response
from routers.grid.crud import clear_grid_cell, db_get_grid_cell_data, place_wheelstack_in_grid
from motor.motor_asyncio import AsyncIOMotorClientSession
from routers.storages.crud import db_get_storage_name_id, db_storage_delete_placed_wheelstack, db_storage_place_wheelstack
from auth.jwt_validation import get_role_verification_dependency
from routers.history.history_actions import background_history_record
from routers.wheels.crud import db_find_wheel_by_object_id, db_update_wheel
from routers.batch_numbers.crud import db_find_batch_number, db_create_batch_number
from utility.utilities import get_object_id, time_w_timezone, handle_basic_exceptions
from fastapi import APIRouter, Depends, HTTPException, status, Path, Body, BackgroundTasks
from .models.models import CreateWheelStackRequest, ForceUpdateWheelStackRequest, WheelsData
from routers.base_platform.crud import (
    clear_platform_cell,
    db_get_platform_cell_data,
    place_wheelstack_in_platform,
)
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
    PS_DECONSTRUCTED,
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

# TODO: Move to appropriate place.
#       Also `WebsockeException`s will be dealt with `handle_http_exceptions_for_websocket`.
#       So, either remove `handle_basic_exceptions` or w.e it's not actually going to differ.
async def create_new_wheelstack_check_batch(
        batch_data: dict, db: AsyncIOMotorClient, session: AsyncIOMotorClientSession
):
    batch_number: str = batch_data['batchNumber']
    # Using transaction, so we can't just `insert_one` and ignore Duplicate.
    # Transaction abrupts at any error...
    exists: dict = await db_find_batch_number(
        batch_number, db, DB_PMK_NAME, CLN_BATCH_NUMBERS, session
    )
    if exists is None:
        await db_create_batch_number(
            batch_data, db, DB_PMK_NAME, CLN_BATCH_NUMBERS, session
        )


async def create_new_wheelstack_cell_check(cell_data, is_websocket: bool = False):
    if cell_data['blocked']:
        msg_cell_blocked: str = 'Chosen placement cell blocked'
        await handle_basic_exceptions(msg_cell_blocked, status.HTTP_403_FORBIDDEN, is_websocket)
    elif cell_data['wheelStack']:
        msg_cell_taken: str = 'Chosen placement cell already taken by different wheelstack'
        await handle_basic_exceptions(msg_cell_taken, status.HTTP_403_FORBIDDEN, is_websocket)


async def create_new_wheelstack_check_placement(
        db: AsyncIOMotorClient, placement_data: dict, is_websocket: bool = False
) -> dict:
    msg_non_existing_placement = 'Chosen placement doesnt exist'
    placement = None
    placement_id: ObjectId = placement_data['placementId']
    placement_name: str = placement_data['placementName']
    placement_type: str = placement_data['placementType']
    placement_row: str = placement_data['placementRow']
    placement_col: str = placement_data['placementCol']
    if PT_BASE_PLATFORM == placement_type:
        placement = await db_get_platform_cell_data(
            placement_id, placement_row, placement_col, db, DB_PMK_NAME, CLN_BASE_PLATFORM, placement_name
        )
    elif PT_GRID == placement_data['placementType']:
        placement = await db_get_grid_cell_data(
            placement_id, placement_row, placement_col, db, DB_PMK_NAME, CLN_GRID, placement_name
        )
    elif PT_STORAGE == placement_data['placementType']:
        placement = await db_get_storage_name_id(
            placement_id, placement_name, db, DB_PMK_NAME, CLN_STORAGES
        )
    if placement is None:
        await handle_basic_exceptions(msg_non_existing_placement, status.HTTP_404_NOT_FOUND, is_websocket)
    if PT_BASE_PLATFORM == placement_data['placementType'] or PT_GRID == placement_data['placementType']:
        cell_data = placement['rows'][placement_data['placementRow']]['columns'][placement_data['placementCol']]
        await create_new_wheelstack_cell_check(cell_data)
    return placement


async def create_new_wheelstack_wheel_check(
        db: AsyncIOMotorClient, wheel_id: str,
        wheelstack_data: dict, is_websocket: bool = False
):
    wheel_object_id: ObjectId = await get_object_id(wheel_id)
    wheel_data: dict = await db_find_wheel_by_object_id(
        wheel_object_id, db, DB_PMK_NAME, CLN_WHEELS
    )
    if wheel_data is None:
        msg_missing: str = f'Provided wheel doesnt exist {wheel_id}'
        await handle_basic_exceptions(
            msg_missing, status.HTTP_404_NOT_FOUND, is_websocket
        )
    # Same batc
    wheelstack_batch: str = wheelstack_data['batchNumber'] 
    wheel_batch: str = wheel_data['batchNumber']
    if wheel_batch != wheelstack_batch:
        msg_incorrect_batch: str = f'Provided wheel = {wheel_id} have different `batchNumber` => {wheel_batch}'
        await handle_basic_exceptions(
            msg_incorrect_batch, status.HTTP_400_BAD_REQUEST, is_websocket
        )
    wheel_status: str = wheel_data['status']
    if wheel_status != WH_UNPLACED and wheel_status != PT_BASE_PLATFORM:
        msg_incorrect_status: str = f'Provided wheel = {wheel_id} have incorrect `status` => {wheel_status} | Only `unplaced` allowed'
        await handle_basic_exceptions(
            msg_incorrect_status, status.HTTP_400_BAD_REQUEST, is_websocket
        )
    wheel_wheelstack: dict = wheel_data['wheelStack']
    if wheel_wheelstack:
        msg_already_used: str = f'Provided wheel = {wheel_id} already used in different `wheelstack` => {wheel_wheelstack['_id']}'
        await handle_basic_exceptions(
            msg_already_used, status.HTTP_400_BAD_REQUEST, is_websocket
        )


async def update_placement_filter(wheelstack_data: dict, db: AsyncIOMotorClient, session: AsyncIOMotorClientSession):
    wheelstack_id: ObjectId = wheelstack_data['_id']
    placement_id: str = wheelstack_data['placement']['placementId']
    placement_type: str = wheelstack_data['placement']['type']
    placement_row: str = wheelstack_data['rowPlacement']
    placement_col: str = wheelstack_data['colPlacement']
    if PT_STORAGE == placement_type:
        await db_storage_place_wheelstack(
            placement_id, '', wheelstack_id,
            db, DB_PMK_NAME, CLN_STORAGES, session
        )
    elif PT_GRID == placement_type:
        await place_wheelstack_in_grid(
            placement_id, wheelstack_id, placement_row, placement_col,
            db, DB_PMK_NAME, CLN_GRID, session
        )
    elif PT_BASE_PLATFORM == placement_type:
        await place_wheelstack_in_platform(
            placement_id, wheelstack_id, placement_row, placement_col,
            db, DB_PMK_NAME, CLN_BASE_PLATFORM, session 
        )


async def create_new_wheelstack_action(
        db: AsyncIOMotorClient, wheelstack_data: dict, is_websocket: bool = False
    ):
    placement_id: str = wheelstack_data['placementId']
    if placement_id:
        placement_id: ObjectId = await get_object_id(wheelstack_data['placementId'])
    placement_name: str = wheelstack_data['placementName']
    placement_type: str = wheelstack_data['placementType']
    placement_row: str = wheelstack_data['rowPlacement']
    placement_col: str = wheelstack_data['colPlacement']
    placement_data: dict = {
        'placementId': placement_id,
        'placementName': placement_name,
        'placementType': placement_type,
        'placementRow': placement_row,
        'placementCol': placement_col,
    }
    check_tasks = []
    # Placement check: should exist and desired placement cell is empty
    check_tasks.append(
        create_new_wheelstack_check_placement(db, placement_data)
    )
    # Wheels check: all should exist and not placed in something else.
    for wheel_id in wheelstack_data['wheels']:
        check_tasks.append(
            create_new_wheelstack_wheel_check(db, wheel_id, wheelstack_data, is_websocket)
        )
    check_results = await asyncio.gather(*check_tasks)
    placement_result = check_results[0]
    placement_id: ObjectId = placement_result['_id']
    batch_number: str = wheelstack_data['batchNumber']
    cor_wheels: list[ObjectId] = [await get_object_id(wheel_id) for wheel_id in wheelstack_data['wheels']]
    transaction_tasks = []
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            # Batch create or ignore if exists
            new_batch_number_data: dict = {
                'batchNumber': batch_number,
                'laboratoryPassed': False,
                'laboratoryTestDate': None,
            }
            transaction_tasks.append(
                create_new_wheelstack_check_batch(
                    new_batch_number_data, db, session
                )
            )
            # Wheelstack data gather
            creation_time = await time_w_timezone()
            correct_data = {
                'batchNumber': batch_number,
                'placement': {
                    'type': placement_type,
                    'placementId': placement_id,
                },
                'rowPlacement': placement_row,
                'colPlacement': placement_col,
                'createdAt': creation_time,
                'lastChange': creation_time,
                'lastOrder': None,
                'maxSize': wheelstack_data['maxSize'],
                'blocked': False,
                'wheels': cor_wheels,
                'status': wheelstack_data['status'],
            }
            transaction_tasks.append(
                db_insert_wheelstack(
                    correct_data, db, DB_PMK_NAME, CLN_WHEELSTACKS, session
                )
            )
            # Update wheels
            transaction_results = await asyncio.gather(*transaction_tasks)
            creation_result = transaction_results[1]
            created_id: ObjectId = creation_result.inserted_id
            transaction_tasks = []
            correct_data['_id'] = created_id
            for index, wheel_object_id in enumerate(cor_wheels):
                wheelstack_wheel_record: dict = {
                    'wheelStack': {
                        'wheelStackId': created_id,
                        'wheelStackPosition': index,
                    },
                    'status': wheelstack_data['status'],
                }
                transaction_tasks.append(
                    db_update_wheel(
                        wheel_object_id, wheelstack_wheel_record, db, DB_PMK_NAME, CLN_WHEELS, session
                    )
                )
            # Place in correct placement
            transaction_tasks.append(
                update_placement_filter(
                    correct_data, db, session
                )
            )
            transaction_results = await asyncio.gather(*transaction_tasks)
            result_data: dict = {
                'createdId': created_id,
                'usedData': correct_data
            }
            return result_data
# ---


@router.post(
    path='/',
    description='Create and place a new `wheelStack` in chosen placement',
    status_code=status.HTTP_201_CREATED,
    response_description='`objectId` of the created wheelstack',
    response_class=JSONResponse,
    name='Create Wheelstack',
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
    # TODO: Async changes for checks + We should be able to create Wheelstack in `tempoStorage`.
    wheelstack_data = wheelstack.model_dump()
    created_data: dict = await create_new_wheelstack_action(
        db, wheelstack_data
    )
    cor_wheelstack_data: dict = created_data['usedData']
    created_id: ObjectId = created_data['createdId']
    # + BG HISTORY RECORD +
    placement_id: ObjectId = cor_wheelstack_data['placement']['placementId']
    placement_type: str = cor_wheelstack_data['placement']['type']
    background_tasks.add_task(
        background_history_record, placement_id, placement_type, db
    )
    # - BG HISTORY RECORD -
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


async def clear_wheelstack_place_filter(
        wheelstack_data: dict,
        db: AsyncIOMotorClient,
        session: AsyncIOMotorClientSession,
):
    placement_id: ObjectId = wheelstack_data['placement']['placementId']
    placement_type: str = wheelstack_data['placement']['type']
    placement_row: str = wheelstack_data['rowPlacement']
    placement_col: str = wheelstack_data['colPlacement']
    if PT_STORAGE == placement_type:
        await db_storage_delete_placed_wheelstack(
            placement_id, '', wheelstack_data['_id'], db, DB_PMK_NAME, CLN_STORAGES, session, True   
        )
    elif PT_GRID == placement_type:
        await clear_grid_cell(
            placement_id, placement_row, placement_col, db, DB_PMK_NAME, CLN_GRID, session, True
        )
    elif PT_BASE_PLATFORM == placement_type:
        await clear_platform_cell(
            placement_id, placement_row, placement_col, db, DB_PMK_NAME, CLN_BASE_PLATFORM, session, True
        )
# ---


@router.patch(
    path='/reconstruct/{target_object_id}',
    description='Reconstruct `wheelstack` with another wheels. Setting changed `wheel`s to `unplaced`',
    name='Reconstruct',
)
async def route_patch_rebuild_wheelstack(
    background_tasks: BackgroundTasks,
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
    transaction_tasks = []
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            # 5.1 Update placed
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
    # + BG record +
    source_id = await get_object_id(exists['placement']['placementId'])
    source_type = exists['placement']['type']
    background_tasks.add_task(background_history_record, source_id, source_type, db)
    # - BG record -
    return Response(status_code=status.HTTP_200_OK)


@router.patch(
    path='/deconstruct/{target_object_id}',
    description='Deconstruct `wheelstacks` => changing all of its `wheel` statuses to `unplaced` and removing it from current placement',
    name='Deconstruct',
)
async def route_patch_deconstruct_wheelstack(
    background_tasks: BackgroundTasks,
    target_object_id: str = Path(...,
                                 description='`ObjectId` of the targeted `wheelstack`'),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
    token_data: dict = get_role_verification_dependency(BASIC_PAGE_ACTION_ROLES),
):
    wheelstack_object_id: ObjectId = await get_object_id(target_object_id)
    # 1. Wheelstack exists and not blocked
    exists: dict = await db_find_wheelstack_by_object_id(
        wheelstack_object_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
    )
    if exists is None:
        raise HTTPException(
            detail=f'`wheelstack` with provided `objectId` = {wheelstack_object_id}. Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if exists['blocked']:
        raise HTTPException(
            detail=f'`wheelstack` with provided `objectId` = {wheelstack_object_id}. Cant be altered while its blocked.',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    transaction_tasks = []
    async with (await db.start_session()) as session:
        async with session.start_transaction():
            # 2. Setting all wheel statuses to `unplaced`
            clear_wheel_data = {
                'status': WH_UNPLACED,
                'wheelStack': None
            }
            for wheel_object_id in exists['wheels']:
                transaction_tasks.append(
                    db_update_wheel(
                        wheel_object_id, clear_wheel_data, db, DB_PMK_NAME, CLN_WHEELS, session
                    )
                )
            # 3. Clearing current placement cell
            transaction_tasks.append(
                clear_wheelstack_place_filter(exists, db, session)
            )
            # 4. Setting `wheelstack` status to `deconstructed`
            #   Only changing status, because it should never be used with that status set.
            #   And we will still have LastState data of this wheelstack.
            exists['status'] = PS_DECONSTRUCTED
            transaction_tasks.append(
                db_update_wheelstack(
                    exists, exists['_id'], db, DB_PMK_NAME, CLN_WHEELSTACKS, session, True
                )
            )
            await asyncio.gather(*transaction_tasks)
    # + BG record +
    source_id = await get_object_id(exists['placement']['placementId'])
    source_type = exists['placement']['type']
    background_tasks.add_task(background_history_record, source_id, source_type, db)
    # - BG record -
    return Response(status_code=status.HTTP_200_OK)
