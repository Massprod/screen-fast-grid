from fastapi import APIRouter, Depends, status, Query, Path, HTTPException, Response
from fastapi.responses import JSONResponse
from database.mongo_connection import mongo_client
from .crud import get_platform, put_wheelstack_in_platform
from bson import ObjectId
from bson.errors import InvalidId
from routers.wheelstacks.crud import db_find_wheelstack
from routers.base_platform.crud import platform_make_json_friendly


router = APIRouter()


@router.get(
    path='/',
    description='get current base platform state',
)
async def get_current_platform(
        db=Depends(mongo_client.depend_client)
):
    res = await get_platform(db)
    status_code = status.HTTP_200_OK
    cor_res = await platform_make_json_friendly(res)
    return JSONResponse(content=cor_res, status_code=status_code)


@router.put(
    path='/{wheelstack_object_id}',
    description='wheelstack object id ',
)
async def move_wheelstack_in_the_platform(
        row: str = Query(...),
        column: str = Query(...),
        wheelstack_object_id: str = Path(...),
        db=Depends(mongo_client.depend_client),
):
    status_code: int = status.HTTP_200_OK
    try:
        object_id = ObjectId(wheelstack_object_id)
    except InvalidId as e:
        status_code = status.HTTP_400_BAD_REQUEST
        raise HTTPException(detail=str(e), status_code=status_code)
    exist = await db_find_wheelstack(db, object_id)
    if exist is None:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail='Wheelstack doesnt exist', status_code=status_code)
    if exist['blocked']:
        status_code = status.HTTP_403_FORBIDDEN
        raise HTTPException(detail='Wheelstack blocked', status_code=status_code)

    res = await put_wheelstack_in_platform(
        db, row, column, object_id,
    )
    if 0 == res.matched_count:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail='Placement doesnt exist', status_code=status_code)
    if 0 == res.modified_count:
        status_code = status.HTTP_304_NOT_MODIFIED
        raise HTTPException(detail='Not modified', status_code=status_code)
    return Response(status_code=status_code)
