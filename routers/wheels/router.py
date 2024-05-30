from .crud import db_insert_wheel, db_find_wheel, make_json_friendly, db_update_wheel, db_delete_wheel
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, Response
from database.mongo_connection import get_db
from .models.models import CreateWheelRequest
from motor.motor_asyncio import AsyncIOMotorClient


router = APIRouter()


@router.put('/')
async def update_wheel(
        wheel: CreateWheelRequest,
        db: AsyncIOMotorClient = Depends(get_db),
):
    wheel_data = wheel.dict()
    result = await db_update_wheel(db, wheel_data)
    if result.matched_count == 0:
        return Response(status_code=404)
    elif result.modified_count == 0:
        return Response(status_code=304)
    return Response(status_code=200)


@router.post('/')
async def create_wheel(
        wheel: CreateWheelRequest,
        db: AsyncIOMotorClient = Depends(get_db),
):
    wheel_data = wheel.dict()
    wheel_id = wheel_data['wheelId']
    result = await db_find_wheel(db, wheel_id)
    if result is not None:
        return HTTPException(detail=f'Wheel with provided `wheelId`={wheel_id}, already exist',
                             status_code=302)
    result = await db_insert_wheel(db, wheel_data)
    wheel_data['_id'] = result.inserted_id
    return JSONResponse(content=await make_json_friendly(wheel_data), status_code=201)


@router.delete('/{wheel_id}')
async def delete_wheel(
        wheel_id: str,
        db: AsyncIOMotorClient = Depends(get_db),
):
    result = await db_delete_wheel(db, wheel_id)
    if 0 == result.deleted_count:
        return HTTPException(status_code=404,
                             detail=f'Wheel with provided `wheelId`={wheel_id}, not found')
    return Response(status_code=200)
