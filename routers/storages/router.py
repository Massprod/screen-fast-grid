from bson import ObjectId
from loguru import logger
from utility.utilities import get_object_id
from constants import DB_PMK_NAME, CLN_STORAGES
from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_connection import mongo_client
from fastapi.responses import JSONResponse, Response
from fastapi import APIRouter, Depends, HTTPException, status, Body, Query
from routers.storages.crud import (db_get_storage_by_name,
                                   db_create_storage,
                                   db_get_storage_by_object_id,
                                   db_storage_make_json_friendly, db_get_all_storages
                                   )


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
async def route_get_created_storage(
        storage_name: str = Query(None,
                                  description="Name of the storage to search"),
        storage_id: str = Query(None,
                                description="`ObjectId` of the storage to search"),
        include_data: bool = Query(True,
                                   description="Indicator to include data of the `elements` inside of `storage`"),
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    if not storage_name and not storage_id:
        logger.warning(f'Attempt to search for a `storage` without correctly provided Query parameters')
        raise HTTPException(
            detail='You must provide either `ObjectId` or `name` to search for a `storage`',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    exist = None
    if storage_id:
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
    resp_data = await db_storage_make_json_friendly(exist)
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
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    resp_data = await db_get_all_storages(include_data, db, DB_PMK_NAME, CLN_STORAGES)
    for index in range(len(resp_data)):
        resp_data[index] = await db_storage_make_json_friendly(resp_data[index])
    return JSONResponse(
        content=resp_data,
        status_code=status.HTTP_200_OK,
    )
