import re
from loguru import logger
from datetime import timedelta, datetime
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from database.mongo_connection import mongo_client
from fastapi.responses import JSONResponse, Response
from auth.jwt_validation import get_role_verification_dependency
from utility.utilities import async_convert_object_id_and_datetime_to_str, get_correct_datetime, get_db_collection
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from fastapi import APIRouter, Depends, HTTPException, status, Path, Query
from constants import DB_PMK_NAME, CLN_BATCH_NUMBERS, LAB_PAGE_VIEW_ROLES, LAB_PAGE_ACTION_ROLES
from .crud import (
    db_find_batch_number,
    db_find_all_batch_numbers,
    db_change_lab_status,
    batch_number_record_make_json_friendly,
    db_find_all_batch_numbers_in_period,
)


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
        token_data: dict = get_role_verification_dependency(LAB_PAGE_VIEW_ROLES),
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
                                description='End of the period in format: `YYYY-mm-dd`'),
        token_data: dict = get_role_verification_dependency(LAB_PAGE_VIEW_ROLES),
):
    correct_start = await get_correct_datetime(period_start)
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
        token_data: dict = get_role_verification_dependency(LAB_PAGE_VIEW_ROLES),
        batch_number: str = Path(...,
                                 description='Target `batchNumber`'),
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
        token_data: dict = get_role_verification_dependency(LAB_PAGE_ACTION_ROLES),
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


# region dataGather => DataTables 

# https://datatables.net/manual/server-side

# region mainTable
# region dataFiltering
def build_try_dates(search_value: str) -> dict:
    """
    Build a MongoDB date filter to handle various date formats including custom ones like '19.11.2024 - 10:36:20'.

    Args:
        search_value (str): The user input string.

    Returns:
        dict: A MongoDB filter for the `laboratoryTestDate` field.
    """
    try:
        # Check for the specific custom format first
        try:
            parsed_date = datetime.strptime(search_value, "%d.%m.%Y - %H:%M:%S")
            start_date = parsed_date.replace(microsecond=0)
            end_date = start_date + timedelta(seconds=1)
            return start_date, end_date
        except ValueError:
            # Fallback to dateutil parsing for flexible formats
            parsed_date = parse(search_value, fuzzy=True, default=datetime.min)

        # Determine the level of granularity and build the filter accordingly
        if parsed_date.year != 1 and parsed_date.month == 1 and parsed_date.day == 1:
            # Only Year provided (e.g., "2024")
            start_date = datetime(parsed_date.year, 1, 1)
            end_date = start_date + relativedelta(years=1)
            return start_date, end_date

        if parsed_date.day == 1 and parsed_date.hour == 0:
            # Year and Month provided (e.g., "Nov 2024" or "11.2024")
            start_date = datetime(parsed_date.year, parsed_date.month, 1)
            end_date = start_date + relativedelta(months=1)
            return start_date, end_date

        if parsed_date.hour == 0 and parsed_date.minute == 0:
            # Full Date (Day granularity, e.g., "19.11.2024")
            start_date = parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
            return start_date, end_date

        if parsed_date.minute == 0 and parsed_date.second == 0:
            # Date + Hour granularity (e.g., "19.11.2024 - 10:00")
            start_date = parsed_date.replace(minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(hours=1)
            return start_date, end_date

        if parsed_date.second == 0:
            # Full Date + Time (Minute granularity, e.g., "19.11.2024 - 10:36")
            start_date = parsed_date.replace(second=0, microsecond=0)
            end_date = start_date + timedelta(minutes=1)
            return start_date, end_date

        # Exact Timestamp (e.g., "19.11.2024 - 10:36:20")
        start_date = parsed_date.replace(microsecond=0)
        end_date = start_date + timedelta(seconds=1)
        return start_date, end_date

    except ValueError:
        # If parsing fails, return an empty filter
        return None, None


def create_filter(search_value: str) -> dict:
    """
    Create a full MongoDB filter query based on the input.
    """
    start_date, end_date = build_try_dates(search_value)
    search_filter = {}
    search_filter['$or'] = [
        {"batchNumber": {"$regex": search_value, "$options": "i"}},
    ]
    if start_date and end_date:
        search_filter['$or'].append(
            {'createdAt': {'$gte': start_date, '$lte': end_date}}
        )
        search_filter['$or'].append(
            {'laboratoryTestDate': {'$gte': start_date, '$lte': end_date}}
        )
    # Return combined search filter
    return search_filter


def preprocess_search_value(search_value: str) -> str:
    """
    Preprocess the search value to remove or escape special characters
    that can cause issues in MongoDB regex queries.
    
    Args:
        search_value (str): The raw search value input from the user.
    
    Returns:
        str: The sanitized search value.
    """
    # Escape special regex characters to ensure safety in MongoDB regex
    sanitized_value = re.sub(r"[.*+?^${}()|[\]\\]", r"", search_value)
    return sanitized_value
# endregion dataFiltering


# CloseTied endPoint == just creating it to show correct data in the `main` table.
@router.get("/tables_data/batch_main")
async def get_batch_main_data(
    startDate: str = Query(''),
    endDate: str = Query(''),
    createdAtPeriod: bool = Query(True),
    draw: int = Query(...),
    start: int = Query(0),
    length: int = Query(10),
    search_value: str = Query("", alias="search[value]"),
    order_column: int = Query(0, alias="order[0][column]"),
    order_dir: str = Query("asc", alias="order[0][dir]"),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    # No idea why even correct string with format => "%d.%m.%Y - %H:%M:%S"
    # Can't be correctyl transformed to date, but w.e leaving filter on date from search, for now.
    cor_search_value = preprocess_search_value(search_value)
    filter_query: dict[str, dict] = create_filter(cor_search_value)
    # Always the same query, we either get date or not.
    # But, we can filter data for some period, with `startDate`, `endDate`.
    if startDate or endDate:
        try:
            if startDate:
                start_date = datetime.strptime(startDate, "%Y-%m-%d")
            else:
                start_date = datetime.min
            if endDate:
                end_date = datetime.strptime(endDate, "%Y-%m-%d") + timedelta(days=1)
            else:
                end_date = datetime.max
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid period dates format: {e}")
        date_field = "createdAt" if createdAtPeriod else "laboratoryTestDate"
        filter_query[date_field] = {"$gte": start_date, "$lte": end_date}
    # We don't need wheels for the `main` table.
    projection = {
        'wheels': 0,
    }
    batches_collection: AsyncIOMotorCollection = await get_db_collection(db, DB_PMK_NAME, CLN_BATCH_NUMBERS)
    total_records = await batches_collection.count_documents({})
    filtered_records = await batches_collection.count_documents(filter_query)
    # Default column of the `main` table.
    column_map: list[str] = ['batchNumber', 'createdAt', 'laboratoryTestDate', 'laboratoryPassed']
    sort_column: str = column_map[order_column]
    sort_direction: int = 1 if order_dir == 'asc' else -1
    records = await batches_collection.find(filter_query, projection) \
                     .sort(sort_column, sort_direction) \
                     .skip(start)  \
                     .limit(length) \
                     .to_list(length=length)
    records_data = await async_convert_object_id_and_datetime_to_str(records)
    resp_content = {
        'draw': draw,
        'recordsTotal': total_records,
        'recordsFiltered': filtered_records,
        'data': records_data,
    }
    return JSONResponse(
        content=resp_content,
        status_code=status.HTTP_200_OK
    )
# endregion mainTable
# endregion
