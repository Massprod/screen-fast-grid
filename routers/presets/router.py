from bson import ObjectId
from loguru import logger
from fastapi.responses import Response, JSONResponse
from database.presets.cell_object import GridObject
from routers.presets.models import CellType, PresetData
from utility.utilities import async_convert_object_records, get_object_id, time_w_timezone
from database.mongo_connection import mongo_client
from auth.jwt_validation import get_role_verification_dependency
from fastapi import APIRouter, Body, Depends, HTTPException, status, Path
from .crud import (
    add_new_preset,
    get_preset_by_id,
    preset_make_json_friendly,
    get_preset_by_name,
    get_all_presets,
    presets_make_json_friendly
)
from constants import (
    ADMIN_ACCESS_ROLES,
    DB_PMK_NAME,
    CLN_PRESETS,
    BASIC_PAGE_VIEW_ROLES,
    EE_LABORATORY
)


router = APIRouter()


@router.get(
    path='/all',
    description='Get all currently present `preset`s in DB, without their structure',
    response_class=JSONResponse,
    name='Get all presets',
)
async def route_get_all_presets(
        db=Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
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
        db=Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
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
        db=Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
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


@router.post(
    path='/create',
    description='Create preset with provided data. Preset names are unique.',
    name='Create Preset',
)
async def route_post_create_preset(
        req_data: PresetData = Body(...),
        db=Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(ADMIN_ACCESS_ROLES),
):
    data = req_data.model_dump()

    preset_name: str = data['presetName']
    preset_type: str = data['presetType']

    rows: int = data['rows']
    columns: int = data['columns']
    
    row_identifiers: list[str] = data['rowIdentifiers']
    column_identifiers: list[str] = data['columnIdentifiers']
    
    if not row_identifiers:
        row_identifiers = [str(index) for index in range(1, 1 + rows)]
    if not column_identifiers:
        column_identifiers = [str(index) for index in range(1, 1 + columns)]
    # '' - empty row == first row == identifiers header
    preset_rows: list[list[GridObject]] = {'rows': {}}
    empty_columns: dict[str, dict] = {
        identifier: GridObject(identifier=True, identifier_string=identifier) for identifier in column_identifiers
        }
    # '' adding first element as `whitespace` == standard header
    # We want to see empty cell -> identifiers, so we could use rowIdentifiers as 1st cell later.
    # And '' == empty strings can't be used on request `minLength == 1`
    empty_columns[''] = GridObject(whitespace=True)
    preset_rows['rows'][''] = {
        'columnsOrder': [''] + column_identifiers,
        'columns': empty_columns,
    }
    for row_identifier in row_identifiers:
        row_columns = {
            identifier: GridObject(wheelstack=True) for identifier in column_identifiers
        }
        row_columns[''] = GridObject(identifier=True, identifier_string=row_identifier)
        preset_rows['rows'][row_identifier] = {
            'columnsOrder': [''] + column_identifiers,
            'columns': row_columns
            }
    creation_date = await time_w_timezone()
    preset_data = {
        'presetName': preset_name,
        'presetType': preset_type,
        'createdAt': creation_date,
        'rowsOrder': [''] + row_identifiers,
        'rows': preset_rows['rows'],
        'extra': {},
    }
    # OverrideCells
    cell_types: dict[str, dict[str, CellType]] = data['cellTypes']
    for cell_row in cell_types:
        for cell_col in cell_types[cell_row]:
            cell_type = cell_types[cell_row][cell_col]
            if not (cell_row in preset_data['rows'] and cell_col in preset_data['rows'][cell_row]['columns']):
                continue
            target = preset_data['rows'][cell_row]['columns'][cell_col]
            target.reset_object()
            # We only have 2 types `whitespace` `wheelStack`, both booleans.
            if 'wheelStack' == cell_type:
                target.wheelstack = True
            elif 'whitespace' == cell_type:
                target.whitespace = True 
    # Extra
    extra_data = data['extra']
    for el_name, el_data in extra_data.items():
        preset_data['extra'][el_name] = {
            'type': el_data['type'],
            'id': el_data['id'],
            'orders': {},
            'blocked': False,
        }
    # Placements is a messs, and we won't be able to utilize them correctyl without lab at every1.
    extra_data['laboratory'] = {
        'type': EE_LABORATORY,
        'id': 'fLab',
        'orders': {},
        'blocked': False,
    }
    # Convert records
    def convert_grid_object(value: GridObject):
        return value.get_dict()
    
    converters = {
        GridObject: convert_grid_object
    }
    preset_data['rows'] = await async_convert_object_records(preset_data['rows'], converters)
    created_id: ObjectId = await add_new_preset(
        preset_data, db, DB_PMK_NAME, CLN_PRESETS
    )
    return JSONResponse(
        content={
            '_id': str(created_id),
        },
        status_code=200,
    )
