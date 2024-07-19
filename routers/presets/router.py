from bson import ObjectId
from .crud import (get_preset_by_id, preset_make_json_friendly,
                   get_preset_by_name, get_all_presets, presets_make_json_friendly)
from utility.utilities import get_object_id
from database.mongo_connection import mongo_client
from constants import DB_PMK_NAME, CLN_GRID, CLN_PRESETS
from fastapi import APIRouter, Depends, HTTPException, status, Path, Body, Query
from fastapi.responses import JSONResponse
from loguru import logger


router = APIRouter()



@router.get(
    path='/all',
    description='Get all currently present `preset`s in DB, without their structure',
    response_class=JSONResponse,
    name='Get all presets',
)
async def route_get_all_presets(
        db=Depends(mongo_client.depend_client),
):
    logger.info(f'Received request to get all presets from `{CLN_PRESETS}` collection')
    result = await get_all_presets(db, DB_PMK_NAME, CLN_PRESETS)
    result = await presets_make_json_friendly(result)
    logger.info(f'Successfully found all presets in DB = {DB_PMK_NAME}, collection = {CLN_PRESETS}. Returning ')
    return JSONResponse(
        content=result,
        status_code=status.HTTP_200_OK,
    )


@router.get(
    path='/by_id/{preset_object_id}',
    description='Search for a `preset` with provided `objectId`',
    response_class=JSONResponse,
    name='Get preset by `objectId`'
)
async def route_get_preset_by_object_id(
        preset_object_id: str = Path(..., description='preset `objectId` to find'),
        db=Depends(mongo_client.depend_client)
):
    logger.info(f"Received request to get preset with `objectId` = {preset_object_id}")
    preset_id: ObjectId = await get_object_id(preset_object_id)
    result = await get_preset_by_id(preset_id, db, DB_PMK_NAME, CLN_PRESETS)
    if result is None:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(status_code=status_code)
    resp_data = await preset_make_json_friendly(result)
    logger.info(f"Successfully found preset for `objectId` = {preset_object_id}. Returning `preset` data")
    return JSONResponse(content=resp_data, status_code=status.HTTP_200_OK)


@router.get(
    path='/by_name/{preset_name}',
    description='Search for a `preset` with provided `presetName`',
    response_class=JSONResponse,
    name='Get preset by `presetName`'
)
async def route_get_preset_by_preset_name(
        preset_name: str = Path(..., description='preset `presetName` to find'),
        db=Depends(mongo_client.depend_client)
):
    logger.info(f'Received request to get preset with `presetName`: {preset_name}')
    status_code = status.HTTP_200_OK
    result = await get_preset_by_name(preset_name, db, DB_PMK_NAME, CLN_PRESETS)
    if result is None:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(status_code=status_code)
    resp_data = await preset_make_json_friendly(result)
    logger.info(f'Successfully found preset for `presetName` = {preset_name}. Returning `preset` data')
    return JSONResponse(content=resp_data, status_code=status_code)


# TODO: add creating and deletion of presets, but only and only after first working version.
#  Because, we don't actually care about presets for now, only 1 will be used.
