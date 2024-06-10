from database.mongo_connection import mongo_client
from fastapi.responses import JSONResponse, Response
from fastapi import APIRouter, Depends, HTTPException, status, Path, Body, Query
from .models.models import GridModelResponse
from .crud import get_grid, grid_make_json_friendly, put_wheelstack_in_grid
from bson import ObjectId
from bson.errors import InvalidId
from ..wheelstacks.crud import db_find_wheelstack


router = APIRouter()


@router.get(
    path='/',
    description='get current grid state',
)
async def get_current_grid(
        db=Depends(mongo_client.depend_client)
):
    res = await get_grid(db)
    status_code = status.HTTP_200_OK
    cor_res = await grid_make_json_friendly(res)
    return JSONResponse(content=cor_res, status_code=status_code)


# Validate everything, we need to store max ROWs and COLs somewhere and validate them.
# Extra check for existing wheelstack etc...
@router.put(
    path='/{wheelstack_object_id}',
    description='wheelstack object id ',
)
async def move_wheelstack_in_the_grid(
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
    res = await put_wheelstack_in_grid(db, row, column, object_id)
    if 0 == res.matched_count:
        status_code = status.HTTP_404_NOT_FOUND
        raise HTTPException(detail='Placement doesnt exist', status_code=status_code)
    if 0 == res.modified_count:
        status_code = status.HTTP_304_NOT_MODIFIED
        raise HTTPException(detail='Not modified', status_code=status_code)
    return Response(status_code=status_code)
