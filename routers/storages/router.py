import asyncio
from bson import ObjectId
from loguru import logger
from typing import Optional
from utility.utilities import get_object_id, async_convert_object_id_and_datetime_to_str
from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_connection import mongo_client
from fastapi.responses import JSONResponse, Response
from fastapi import APIRouter, Depends, HTTPException, status, Query
from constants import (
    DB_PMK_NAME,
    CLN_STORAGES,
    BASIC_PAGE_VIEW_ROLES,
    ADMIN_ACCESS_ROLES,
)
from routers.storages.crud import (
    db_get_storage_by_name,
    db_create_storage,
    db_get_storage_by_object_id,
    db_get_all_storages,
    db_get_storages_with_elements_data,
    db_storage_get_placed_wheelstack,
    db_get_storage_by_element,
)
from auth.jwt_validation import get_role_verification_dependency


router = APIRouter()


@router.post(
    path='/create',
    description='Creation of a storage',
    name='Create Storage',
)
async def route_post_create_storage(
        storage_name: str = Query(...,
                                  description="Name of the storage to create"),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(ADMIN_ACCESS_ROLES),
):
    exist = await db_get_storage_by_name(
        storage_name, False, db, DB_PMK_NAME, CLN_STORAGES,
    )
    if exist:
        raise HTTPException(
            detail=f'Storage with name = {storage_name}. Already Exists.',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    storage_id = await db_create_storage(
        storage_name, db, DB_PMK_NAME, CLN_STORAGES,
    )
    return JSONResponse(
        content={
            'createdId': str(storage_id.inserted_id),
        },
        status_code=status.HTTP_201_CREATED,
    )


@router.get(
    path='/',
    description='Search for a storage by `name` or `ObjectId`.'
                ' `ObjectId` used in priority over `name`',
    name='Search Storage',
)
async def route_get_storage(
        storage_name: str = Query('',
                                  description="Name of the storage to search"),
        storage_id: str = Query('',
                                description="`ObjectId` of the storage to search"),
        include_data: bool = Query(True,
                                   description="Indicator to include data of the `elements` inside of `storage`"),
        expanded_data: bool = Query(False,
                                    description="Expand all elements to include all data, not just `_id`s"),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    if not storage_name and not storage_id:
        logger.warning(f'Attempt to search for a `storage` without correctly provided Query parameters')
        raise HTTPException(
            detail='You must provide either `ObjectId` or `name` to search for a `storage`',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    exist = None
    if expanded_data:
        if storage_id:
            storage_id: ObjectId = get_object_id(storage_id)
        identifiers: list[dict] = [{'_id': storage_id}, {'name': storage_name}]
        data = await db_get_storages_with_elements_data(
            identifiers, db, DB_PMK_NAME, CLN_STORAGES
        )
        if data:
            exist = data[0]
    elif storage_id:
        storage_object_id = await get_object_id(storage_id)
        exist = await db_get_storage_by_object_id(
            storage_object_id, include_data, db, DB_PMK_NAME, CLN_STORAGES
        )
    elif storage_name:
        exist = await db_get_storage_by_name(
            storage_name, include_data, db, DB_PMK_NAME, CLN_STORAGES
        )
    if exist is None:
        raise HTTPException(
            detail=f'Storage not found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    resp_data = await async_convert_object_id_and_datetime_to_str(exist)
    return JSONResponse(
        content=resp_data,
        status_code=status.HTTP_200_OK,
    )


@router.get(
    path='/all',
    description='Get all of the storages currently presented in DB. With data or without.',
    name='Get Storages',
)
async def route_get_created_storages(
        include_data: bool = Query(False,
                                   description='Indicator to include data of the `storages`'),
        expanded_data: bool = Query(False, 
                                    description='Expand all elements to include all data, not just `_id`s'),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    resp_data = []
    if not expanded_data:
        resp_data = await db_get_all_storages(include_data, db, DB_PMK_NAME, CLN_STORAGES)
    else:
        resp_data = await db_get_storages_with_elements_data(
            [], db, DB_PMK_NAME, CLN_STORAGES
        )
    convert_tasks = []
    for index in range(len(resp_data)):
        convert_task = async_convert_object_id_and_datetime_to_str(resp_data[index])
        convert_tasks.append(convert_task)
    convert_results = await asyncio.gather(*convert_tasks)
    return JSONResponse(
        content=convert_results,
        status_code=status.HTTP_200_OK,
    )


@router.get(
    path='/find_in',
    description='Check element presence in storage by `ObjectId` of the element',
    name='Check Element',
)
async def route_get_stored_element(
    storage_name: Optional[str] = Query(None,
                                      description='Storage `name` to filter on'),
    storage_id: Optional[str] = Query(None,
                                      description='Storage `ObjectId` to filter on'),
    element_id: str = Query(...,
                            description='Stored element `ObjectId` to filter on'),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
    token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    if not storage_id and not storage_name:
        logger.warning(f'Attempt to search in `storage` without correctly provided Query parameters')
        raise HTTPException(
            detail='You must provide either `ObjectId` or `name` to search in `storage`',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    if storage_id:
        storage_id = await get_object_id(storage_id)
    element_object_id = await get_object_id(element_id)
    resp_data = await db_storage_get_placed_wheelstack(
        storage_id, storage_name, element_object_id, db, DB_PMK_NAME, CLN_STORAGES
    )
    if not resp_data:
        raise HTTPException(
            detail='Storage doesnt contain provided element',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    return Response(status_code=status.HTTP_200_OK)


@router.get(
    path='/find',
    description='Search storage by element `ObjectId` stored in it',
    name='Find Storage',
)
async def route_get_storage_by_element(
    element_id: str = Query(...,
                            description='Stored element `ObjectId` to filter on'),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
    token_data: dict = get_role_verification_dependency(BASIC_PAGE_VIEW_ROLES),
):
    element_object_id: ObjectId = await get_object_id(element_id)
    storage_data = await db_get_storage_by_element(
        element_object_id, db, DB_PMK_NAME, CLN_STORAGES
    )
    if not storage_data:
        raise HTTPException(
            detail='NotFound',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    resp_body = {
        'storage': storage_data,
    }
    return JSONResponse(
        content=resp_body,
        status_code=status.HTTP_200_OK,
    )
