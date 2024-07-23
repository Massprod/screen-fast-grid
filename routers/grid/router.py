from database.mongo_connection import mongo_client
from fastapi.responses import JSONResponse, Response
from fastapi import APIRouter, Depends, HTTPException, status, Path, Query
from motor.motor_asyncio import AsyncIOMotorClient
from .crud import (get_grid_by_object_id, grid_make_json_friendly,
                   place_wheelstack_in_grid, create_grid, block_grid_cell, unblock_grid_cell,
                   collect_wheelstack_cells, get_grid_preset_by_object_id, get_grid_by_name,
                   clear_grid_cell, get_all_grid_ids, get_all_grids_data)
from bson import ObjectId
from ..wheelstacks.crud import db_find_wheelstack_by_object_id
from constants import DB_PMK_NAME, CLN_GRID, CLN_PRESETS, PRES_TYPE_GRID, CLN_WHEELSTACKS
from utility.utilities import get_object_id
from routers.presets.crud import get_preset_by_id
from loguru import logger


router = APIRouter()


@router.get(
    path='/all',
    description='Get all `grid`s present in DB',
    response_class=JSONResponse,
    name='Get All'
)
async def route_get_all_grids(
        only_id: bool = Query(False,
                              description='Provide only `objectId` of the elements'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    if only_id:
        result = await get_all_grid_ids(db, DB_PMK_NAME, CLN_GRID)
        for index, grid_id in enumerate(result):
            result[index] = str(grid_id)
        if 0 == len(result):
            raise HTTPException(
                detail='No `grid`s in DB',
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return JSONResponse(
            content=result,
            status_code=status.HTTP_200_OK,
        )
    cor_data: dict = {}
    result = await get_all_grids_data(db, DB_PMK_NAME, CLN_GRID)
    if 0 == len(result):
        raise HTTPException(
            detail='No `grid`s in DB',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    for grid in result:
        cor_data[str(grid['_id'])] = await grid_make_json_friendly(grid)
    return JSONResponse(
        content=cor_data,
        status_code=status.HTTP_200_OK,
    )


@router.get(
    path='/{grid_object_id}',
    description='Get current `grid` state in DB by `objectId`',
    response_class=JSONResponse,
    name='Get Grid State',
)
async def route_get_grid_by_object_id(
        grid_object_id: str = Path(..., description='`objectId` of stored `grid`'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    grid_id: ObjectId = await get_object_id(grid_object_id)
    res = await get_grid_by_object_id(grid_id, db, DB_PMK_NAME, CLN_GRID)
    if res is None:
        raise HTTPException(
            detail=f'`grid` with `objectId` = {grid_object_id} not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    cor_res = await grid_make_json_friendly(res)
    return JSONResponse(content=cor_res, status_code=status.HTTP_200_OK)


@router.get(
    path='/name/{name}',
    description='Get current `grid` state in DB by `name`',
    response_class=JSONResponse,
    name='Get Grid State'
)
async def route_get_grid_by_name(
        name: str = Path(...,
                         description='`name` of stored `grid`'),
        db=Depends(mongo_client.depend_client),
):
    exist = await get_grid_by_name(name, db, DB_PMK_NAME, CLN_GRID)
    if exist is None:
        raise HTTPException(
            detail=f'`grid` with `name` = {name}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    cor_res = await grid_make_json_friendly(exist)
    return JSONResponse(content=cor_res, status_code=status.HTTP_200_OK)


@router.get(
    path='/preset/{grid_object_id}',
    description='get `objectId` of used `preset` to build the `grid`',
    response_class=JSONResponse,
    name='Get used `preset`',
)
async def route_get_grid_preset_by_object_id(
        grid_object_id: str = Path(
            ...,
            description='`objectId` of the `grid` for which we need to get `preset` `objectId` used to create it',
        ),
        db=Depends(mongo_client.depend_client),
):
    logger.info(f'Received request for a `grid` with `objectId` = {grid_object_id}')
    grid_id: ObjectId = await get_object_id(grid_object_id)
    res = await get_grid_preset_by_object_id(grid_id, db, DB_PMK_NAME, CLN_GRID)
    if res is None:
        logger.warning(f'`grid` with `objectId` = {grid_object_id}. Not found.')
        raise HTTPException(
            detail=f'`grid` with `objectId` = {grid_object_id}. Not found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    preset_id = str(res['preset'])
    logger.info(
        f'Successfully found `preset` `objectId` for `grid` = {grid_object_id}.'
        f' Returning `preset` `objectId` {preset_id}'
    )
    return JSONResponse(
        content={
            'preset_id': preset_id,
        },
        status_code=status.HTTP_200_OK,
    )


@router.post(
    path='/create/{preset_object_id}',
    description='Creating empty `grid` accordingly with provided `preset`',
    response_class=JSONResponse,
    name='Create `grid`',
)
async def route_create_empty_grid(
        preset_object_id: str = Path(..., description='`objectId` of the `preset` to use'),
        name: str = Query(...,
                          description='required unique name of the `grid`'),
        db=Depends(mongo_client.depend_client),
):
    logger.info(f'Received request to create a new `grid` by `objectId` of a `preset` = {preset_object_id}')
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
    if preset_data['presetType'] != PRES_TYPE_GRID:
        raise HTTPException(
            detail=f'Incorrect preset type, only `{PRES_TYPE_GRID}` is allowed.',
            status_code=status.HTTP_400_BAD_REQUEST
        )
    exist = await get_grid_by_name(cor_name, db, DB_PMK_NAME, CLN_GRID)
    if exist:
        raise HTTPException(
            detail=f'`grid` with such `name` already exist',
            status_code=status.HTTP_302_FOUND,
        )
    correct_data = await collect_wheelstack_cells(preset_data)
    correct_data['name'] = cor_name
    res = await create_grid(correct_data, db, DB_PMK_NAME, CLN_GRID)
    logger.info(
        f'Successfully created a `grid` from `preset` with `objectId` = {preset_object_id}.'
        f' Returning a new `grid` `objectId` = {res.inserted_id}'
    )
    return JSONResponse(
        content={
            'object_id': str(res.inserted_id),
        },
        status_code=status.HTTP_200_OK
    )


@router.put(
    path='/place/{grid_object_id}/{wheelstack_object_id}',
    description="`WARNING`"
                "\n Force placement on some cell without any dependencies."
                "\n We're just placing `objectId` of the given `wheelStack` in chosen cell."
                "\n If it's empty and not blocked, otherwise reject."
                "\n Without any dependency changes.",
    name='Force Cell Placement',
)
async def route_force_place_wheelstack_in_the_grid(
        grid_object_id: str = Path(...,
                                   description='`objectId` of the `grid` to place into'),
        wheelstack_object_id: str = Path(...,
                                         description='`objectId` of the `wheelStacks` to place'),
        row: str = Query(...,
                         description='`row` of the cell placement'),
        column: str = Query(...,
                            descriptions='`column` of the cell placement'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    grid_id: ObjectId = await get_object_id(grid_object_id)
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
    grid = await get_grid_by_object_id(grid_id, db, DB_PMK_NAME, CLN_GRID)
    if grid is None:
        raise HTTPException(
            detail=f'`grid` with `objectId` = {grid_id}. Not Found.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    try:
        cell = grid['rows'][row]['columns'][column]
    except KeyError as error:
        logger.error(f'Attempt to force placement on non existing cell: `row` = {row} | `column` = {column}: {error}')
        raise HTTPException(
            detail=f"{row}|{column} cell doesnt exist",
            status_code=status.HTTP_404_NOT_FOUND,
        )
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
    res = await place_wheelstack_in_grid(
        grid_id, wheelstack_id, row, column, db, DB_PMK_NAME, CLN_GRID
    )
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f'{row}|{column} cell doesnt exist',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f'Not modified',
            status_code=status.HTTP_304_NOT_MODIFIED,
        )
    return Response(status_code=status.HTTP_200_OK)


# Block|Unblock|Clear cells
@router.put(
    path='/block/{grid_object_id}',
    description='Force block state on a chosen cell',
    name='Force Block',
)
async def route_force_block_of_cell(
        row: str = Query(...,
                         description='`row` of desired cell'),
        column: str = Query(...,
                            description='`column` of desired cell'),
        grid_object_id: str = Path(...,
                                   description='`objectId` of the placement'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    grid_id: ObjectId = await get_object_id(grid_object_id)
    res = await block_grid_cell(grid_id, row, column, db, DB_PMK_NAME, CLN_GRID)
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f"{row}|{column} Cell or placement with given `objectId` = `{grid_object_id}` doesn't exist.",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return Response(status_code=status.HTTP_200_OK)


@router.put(
    path='/unblock/{grid_object_id}',
    description='Force unblock state on a chosen cell',
    name='Force Unblock',
)
async def route_force_unblock_of_cell(
        row: str = Query(...,
                         description='`row` of desired cell'),
        column: str = Query(...,
                            description='`column` of desired cell'),
        grid_object_id: str = Path(...,
                                   description='`objectId` of the placement'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    grid_id: ObjectId = await get_object_id(grid_object_id)
    res = await unblock_grid_cell(grid_id, row, column, db, DB_PMK_NAME, CLN_GRID)
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f"{row}|{column} Cell or placement with given `objectId` = `{grid_id}` doesn't exist.",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return Response(status_code=status.HTTP_200_OK)


@router.put(
    path='/clear/{grid_object_id}',
    description='Force clearing on a chosen cell. Making it empty and unblcoked.',
    name='Force Cell Clear',
)
async def route_force_clear_of_cell(
        row: str = Query(...,
                         description='`row` of desired cell'),
        column: str = Query(...,
                            description='`column` of desired cell'),
        grid_object_id: str = Path(...,
                                   description='`objectId` of the placement'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    grid_id: ObjectId = await get_object_id(grid_object_id)
    res = await clear_grid_cell(grid_id, row, column, db, DB_PMK_NAME, CLN_GRID)
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f"{row}|{column} Cell or placement with given `objectId` = `{grid_object_id}` doesn't exist.",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return Response(status_code=status.HTTP_200_OK)
