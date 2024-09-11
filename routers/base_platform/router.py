from bson import ObjectId
from loguru import logger
from fastapi.responses import JSONResponse
from routers.presets.crud import get_preset_by_id
from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_connection import mongo_client
from routers.grid.crud import collect_wheelstack_cells
from routers.wheelstacks.crud import db_find_wheelstack_by_object_id
from auth.jwt_validation import get_role_verification_dependency
from utility.utilities import get_object_id, convert_object_id_and_datetime_to_str
from fastapi import APIRouter, Depends, status, Query, Path, HTTPException, Response
from .crud import get_platform_by_object_id, place_wheelstack_in_platform, db_get_platform_last_change_time
from constants import (
    DB_PMK_NAME,
    CLN_BASE_PLATFORM,
    CLN_PRESETS,
    PRES_TYPE_PLATFORM,
    CLN_WHEELSTACKS,
    BASIC_PAGE_VIEW_ROLES,
    ADMIN_ACCESS_ROLES,
)
from routers.base_platform.crud import (
    platform_make_json_friendly,
    get_platform_preset_by_object_id,
    create_platform,
    get_platform_by_name,
    block_platform_cell,
    unblock_platform_cell,
    clear_platform_cell,
    get_all_platforms,
    get_all_platforms_data,
)

router = APIRouter()


@router.get(
    path='/all',
    description='Get all `basePlatform`s present in DB',
    response_class=JSONResponse,
    name='Get All'
)
async def route_get_all_grids(
        include_data: bool = Query(False,
                                   description='Include `row`s and `extra`s data'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    cor_data: list[dict] = []
    if not include_data:
        result = await get_all_platforms(include_data, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
        if 0 == len(result):
            raise HTTPException(
                detail='No `grid`s in DB',
                status_code=status.HTTP_404_NOT_FOUND,
            )
        for record in result:
            cor_data.append(
                convert_object_id_and_datetime_to_str(record)
            )
        return JSONResponse(
            content=cor_data,
            status_code=status.HTTP_200_OK,
        )
    result = await get_all_platforms_data(db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if 0 == len(result):
        raise HTTPException(
            detail='No `grid`s in DB',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    for record in result:
        cor_data.append(
            convert_object_id_and_datetime_to_str(record)
        )
    return JSONResponse(
        content=cor_data,
        status_code=status.HTTP_200_OK,
    )


@router.get(
    path='/{platform_object_id}',
    description='Get current `basePlatform` state in DB by `objectId`',
    response_class=JSONResponse,
    name='Get Platform State',
)
async def route_get_platform_by_object_id(
        platform_object_id: str = Path(..., description='`objectId` of stored `basePlatform`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    platform_id: ObjectId = await get_object_id(platform_object_id)
    res = await get_platform_by_object_id(platform_id, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if res is None:
        raise HTTPException(
            detail=f'`basePlatform` with `objectId` = {platform_object_id} not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    cor_res = await platform_make_json_friendly(res)
    return JSONResponse(content=cor_res, status_code=status.HTTP_200_OK)


@router.get(
    path='/name/{name}',
    description='get current `basePlatform` state in DB by `name`',
    response_class=JSONResponse,
    name='Get Platform state'
)
async def route_get_platform_by_name(
        name: str = Path(...,
                         description='`name` of stored `basePlatform`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    exist = await get_platform_by_name(name, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if exist is None:
        raise HTTPException(
            detail=f'`basePlatform` with `name` = {name}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    cor_res = await platform_make_json_friendly(exist)
    return JSONResponse(content=cor_res, status_code=status.HTTP_200_OK)


@router.get(
    path='/preset/{platform_object_id}',
    description='get `objectId` of used `preset` to build the `basePlatform`',
    response_class=JSONResponse,
    name='Get used `preset'
)
async def route_get_platform_preset_by_object_id(
        platform_object_id: str = Path(
            ...,
            description='`objectId` of the `basePlatform` for which we need to get used `preset`',
        ),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    logger.info(f'Receiver request for a `basePlatform` with `objectId` = {platform_object_id}')
    platform_id: ObjectId = await get_object_id(platform_object_id)
    res = await get_platform_preset_by_object_id(platform_id, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if res is None:
        logger.warning(f'`basePlatform` with `objectId` = {platform_object_id}. Not found')
        raise HTTPException(
            detail=f'`basePlatform` with `objectId` = {platform_object_id}. Not found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    preset_id = str(res['preset'])
    logger.info(
        f'Successfully found `preset` `objectId` for `basePlatform` = `{platform_object_id}'
        f' Returning `preset` `objectId` {platform_object_id}'
    )
    return JSONResponse(
        content={
            'preset_id': preset_id,
        },
        status_code=status.HTTP_200_OK,
    )


@router.post(
    path='/create/{preset_object_id}',
    description='Creating an empty `basePlatform` accordingly with provided `preset`',
    response_class=JSONResponse
)
async def route_create_empty_platform(
        preset_object_id: str = Path(..., description='`objectId` of the `preset` to use'),
        name: str = Query(...,
                          description='unique name of `basePlatform` to use'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(ADMIN_ACCESS_ROLES),
):
    logger.info(f'Receiver request to create a new `basePlatform` by `objectId` of a `preset` = {preset_object_id}')
    cor_name: str = name.strip()
    if not cor_name:
        raise HTTPException(
            detail=f"Query parameter `name` shouldn't be empty",
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    preset_id: ObjectId = await get_object_id(preset_object_id)
    preset_data = await get_preset_by_id(preset_id, db, DB_PMK_NAME, CLN_PRESETS)
    if preset_data is None:
        raise HTTPException(
            detail=f'Preset with provided `objectID` = `{preset_object_id}`. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if preset_data['presetType'] != PRES_TYPE_PLATFORM:
        raise HTTPException(
            detail=f'Incorrect preset type, only `{PRES_TYPE_PLATFORM}` is allowed.',
            status_code=status.HTTP_400_BAD_REQUEST
        )
    exist = await get_platform_by_name(cor_name, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if exist:
        raise HTTPException(
            detail=f'`basePlatform` with such `name` already exist',
            status_code=status.HTTP_302_FOUND,
        )
    correct_data = await collect_wheelstack_cells(preset_data)
    correct_data['name'] = cor_name
    res = await create_platform(correct_data, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    logger.info(
        f'Successfully created a `basePlatform` from `preset` with `objectId` = {preset_object_id}.'
        f' Returning a new `basePlatform` `objectId` = {res.inserted_id}'
    )
    return JSONResponse(
        content={
            'object_id': str(res.inserted_id),
        },
        status_code=status.HTTP_200_OK,
    )


@router.put(
    path='/place/{platform_object_id}/{wheelstack_object_id}',
    description="`WARNING`"
                "\n Force placement on some cell without any dependencies."
                "\n We're just placing `objectId` of the given `wheelStack` in chosen cell."
                "\n If it's empty and not blocked, otherwise reject."
                "\n Without any dependency changes.",
    name='Force Cell Placement',
)
async def route_force_place_wheelstack_in_the_platform(
        platform_object_id: str = Path(...,
                                       description="`objectId` of the `basePlatform` to place into"),
        wheelstack_object_id: str = Path(...,
                                         description='`objectId` of the `wheelStacks` to place'),
        row: str = Query(...,
                         description='`row` of the cell placement'),
        column: str = Query(...,
                            description='`column` of the cell placement'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(ADMIN_ACCESS_ROLES),
):
    # There's only 2 checks we need to check even with our FORCE method.
    # Because, we shouldn't be able to place on non-existing cells
    #  some non-existing wheelstacks.
    # And if order already blocks placement, or it's already taken by other `wheelStack`.
    # Otherwise, we're just placing `wheelStack` on this cell.
    # Again, without any dependencies, because everything should be done in ORDERS.
    # This is like HAND-Control, and foundation to use with extra functions.
    platform_id: ObjectId = await get_object_id(platform_object_id)
    wheelstack_id: ObjectId = await get_object_id(wheelstack_object_id)
    exist = await db_find_wheelstack_by_object_id(wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS)
    if exist is None:
        raise HTTPException(
            detail='Wheelstack doesnt exist',
            status_code=status.HTTP_404_NOT_FOUND
        )
    if exist['blocked']:
        raise HTTPException(
            detail='Wheelstack blocked',
            status_code=status.HTTP_403_FORBIDDEN
        )
    platform = await get_platform_by_object_id(platform_id, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    # We can take it from DB, but we already have taken data for the `basePlatform`.
    # And we can ignore all the data and leave only `_id` of the platform.
    # But I think it's just simpler to check what we already got, without any extra DB calls.
    try:
        cell = platform['rows'][row]['columns'][column]
    except KeyError as error:
        logger.error(f'Attempt to force placement on non existing cell: `row` = {row} | `column` = {column}: {error}')
        raise HTTPException(
            detail=f"{row}|{column} cell doesnt exist",
            status_code=status.HTTP_404_NOT_FOUND,
        )
    # Order should block by itself with `blocked`, extra check if it doesn't.
    if cell['blocked'] or cell['blockedBy'] is not None:
        raise HTTPException(
            detail='Cell is blocked',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if cell['wheelStack'] is not None:
        stored = cell['wheelStack']
        # Already placed.
        if stored == wheelstack_id:
            return Response(status_code=status.HTTP_200_OK)
        raise HTTPException(
            detail='Cell is already taken',
            status_code=status.HTTP_403_FORBIDDEN
        )
    res = await place_wheelstack_in_platform(
        platform_id, wheelstack_id, row, column, db, DB_PMK_NAME, CLN_BASE_PLATFORM
    )
    # Leaving it as extra check. Actually don't need it, we already know placement exists or not.
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f'{row}|{column} cell doesnt exist',
            status_code=status.HTTP_404_NOT_FOUND
        )
    if 0 == res.modified_count:
        raise HTTPException(detail='Not modified', status_code=status.HTTP_304_NOT_MODIFIED)
    return Response(status_code=status.HTTP_200_OK)


# BLock|Unblock|Clear cells
@router.put(
    path='/block/{platform_object_id}',
    description='Force block state on a chosen cell without order',
    name='Force Block',
)
async def route_force_block_of_cell(
        row: str = Query(...,
                         description='`row` of desired cell'),
        column: str = Query(...,
                            description='`column` of desired cell'),
        platform_object_id: str = Path(...,
                                       description='`objectId` of the placement'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(ADMIN_ACCESS_ROLES),
):
    platform_id: ObjectId = await get_object_id(platform_object_id)
    res = await block_platform_cell(platform_id, row, column, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f"{row}|{column} Cell or placement with given `objectId` = `{platform_object_id}` doesn't exist.",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return Response(status_code=status.HTTP_200_OK)


@router.put(
    path='/unblock/{platform_object_id}',
    description='Force unblock status on a chosen cell, not deleting `blockedBy`,'
                ' only use for unblocking blocked cells, which was blocked not by order, but something else.'
                'Like: we block cells, so its going to be inactive and unusable for orders (for w.e the reason)',
    name='Force Unblock',
)
async def route_force_unblock_of_cell(
        row: str = Query(...,
                         description='`row` of desired cell'),
        column: str = Query(...,
                            description='`column` of desired cell'),
        platform_object_id: str = Path(...,
                                       description='`objectId` of the placement'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(ADMIN_ACCESS_ROLES),
):
    platform_id: ObjectId = await get_object_id(platform_object_id)
    res = await unblock_platform_cell(platform_id, row, column, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f"{row}|{column} Cell or placement with given `objectId` = `{platform_object_id}` doesn't exist.",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return Response(status_code=status.HTTP_200_OK)


@router.put(
    path='/clear/{platform_object_id}',
    description='Force clearing on a chosen cell. Making it empty and unblocked, deleting assigned Order.'
                ' Without dependencies, order will still be here. You need to extra clear dependencies.',
    name='Force Cell Clear',
)
async def route_force_clear_of_cell(
        row: str = Query(...,
                         description='`row` of desired cell'),
        column: str = Query(...,
                            description='`column` of desired cell'),
        platform_object_id: str = Path(...,
                                       description='`objectId` of the placement'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(ADMIN_ACCESS_ROLES),
):
    platform_id: ObjectId = await get_object_id(platform_object_id)
    res = await clear_platform_cell(platform_id, row, column, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f"{row}|{column} Cell or placement with given `objectId` = `{platform_object_id}` doesn't exist.",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return Response(status_code=status.HTTP_200_OK)


@router.get(
    path='/change_time/{platform_object_id}',
    description='Get stored `lastChange` timestamp when `basePlatform` was last time changed.',
    name='Get Last Change',
)
async def route_get_change_time(
        platform_object_id: str = Path(...,
                                       description='`objectId` of the `grid` to search'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    platform_id = await get_object_id(platform_object_id)
    res = await db_get_platform_last_change_time(platform_id, db, DB_PMK_NAME, CLN_BASE_PLATFORM)
    if res is None:
        raise HTTPException(
            detail=f'platform with `objectId` = {platform_object_id}. Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    res['_id'] = str(res['_id'])
    res['lastChange'] = res['lastChange'].isoformat()
    return JSONResponse(
        content=res,
        status_code=status.HTTP_200_OK,
    )
