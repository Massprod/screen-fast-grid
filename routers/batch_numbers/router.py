from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_connection import mongo_client
from fastapi.responses import JSONResponse, Response
from fastapi import APIRouter, Depends, HTTPException, status, Path, Query

from utility.utilities import get_correct_datetime
from .crud import (db_find_batch_number,
                   db_find_all_batch_numbers,
                   db_change_lab_status,
                   batch_number_record_make_json_friendly, db_find_all_batch_numbers_in_period
                   )
from constants import DB_PMK_NAME, CLN_BATCH_NUMBERS


router = APIRouter()


@router.get(
    path='/all',
    name='Get All',
    description='Return all of the `batch_numbers` filtered by Query parameters',
)
async def route_get_all_batch_numbers(
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        laboratory_passed: bool = Query(None,
                                        description='Filter to only passed or not passed'),
        days_delta: int = Query(None,
                                description='Filter to `from days_delta to now`'),
):
    res = await db_find_all_batch_numbers(
        laboratory_passed, days_delta, db, DB_PMK_NAME, CLN_BATCH_NUMBERS
    )
    resp_data = {}
    for record in res:
        resp_data[record['batchNumber']] = await batch_number_record_make_json_friendly(record)
    return JSONResponse(
        content=resp_data,
        status_code=status.HTTP_200_OK,
    )


@router.get(
    path='/period',
    name='Get In Period',
    description='Return all of the `batch_number` in the provided period range.'
                ' Range is from first_day -> target_day, inclusive.'
                ' Format: YYYY-mm-dd',
)
async def route_get_all_batch_numbers_in_period(
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        period_start: str = Query(...,
                                  description='Start of the period in format: `YYYY-mm-dd`'),
        period_end: str = Query(...,
                                description='End of the period in format: `YYYY-mm-dd`')
):
    correct_start = await get_correct_datetime(period_start)
    # TODO: Move this to Model, but later.
    if not correct_start:
        raise HTTPException(
            detail='Incorrect `period_start` format. Correct: `YYYY-mm-dd`',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    correct_end = await get_correct_datetime(period_end)
    if not correct_end:
        raise HTTPException(
            detail='Incorrect `period_end` format. Correct: `YYYY-mm-dd`',
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    resp = await db_find_all_batch_numbers_in_period(
        correct_start, correct_end, db, DB_PMK_NAME, CLN_BATCH_NUMBERS
    )
    cor_resp = [await batch_number_record_make_json_friendly(record) for record in resp]
    return JSONResponse(
        content=cor_resp,
        status_code=status.HTTP_200_OK,
    )


@router.get(
    path='/batch_number/{batch_number}',
    name='Get Batch Number',
    description='Returns data about chosen `batch_number`',
)
async def route_get_batch_number(
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        batch_number: str = Path(...,
                                 description='Target `batchNumber`')
):
    res = await db_find_batch_number(batch_number, db, DB_PMK_NAME, CLN_BATCH_NUMBERS)
    if res is None:
        raise HTTPException(
            detail=f'`batchNumber` = {batch_number}. Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    cor_res = await batch_number_record_make_json_friendly(res)
    return JSONResponse(
        content=cor_res,
        status_code=status.HTTP_200_OK,
    )


@router.patch(
    path='/update_laboratory_status/{batch_number}',
    name='Update Lab Status',
    description='Updating Laboratory status',
)
async def route_patch_batch_number_lab_status(
        db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
        laboratory_passed: bool = Query(...,
                                        description='Current Lab status of the `batchNumber`'),
        batch_number: str = Path(...,
                                 description='`batchNumber` to update')
):
    res = await db_change_lab_status(
        batch_number, laboratory_passed, db, DB_PMK_NAME, CLN_BATCH_NUMBERS
    )
    if 0 == res.matched_count:
        raise HTTPException(
            detail=f'`batchNumber` = {batch_number}. Not Found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    return Response(status_code=status.HTTP_200_OK)
