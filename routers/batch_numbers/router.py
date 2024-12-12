import re
import asyncio
from bson import ObjectId
from loguru import logger
from dateutil.parser import parse
from difflib import get_close_matches
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from database.mongo_connection import mongo_client
from fastapi.responses import JSONResponse, Response
from auth.jwt_validation import get_role_verification_dependency
from fastapi import APIRouter, Body, Depends, HTTPException, status, Path, Query
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from routers.orders.orders_creation import orders_create_move_to_laboratory
from utility.utilities import (
    get_object_id,
    time_w_timezone,
    get_db_collection,
    get_correct_datetime,
    async_convert_object_id_and_datetime_to_str,
)
from constants import (
    ADMIN_ACCESS_ROLES,
    CLN_WHEELS,
    DB_PMK_NAME,
    CLN_BATCH_NUMBERS,
    LAB_PAGE_VIEW_ROLES,
    LAB_PAGE_ACTION_ROLES,
    WHEELS_STATUS_MAP,
    WH_GRID,
    WH_STORAGE,
    WH_LABORATORY,
)
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

# region dataFiltering
async def build_try_dates(search_value: str) -> dict:
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
            try:
                parsed_date = parse(search_value, fuzzy=True, default=datetime.min)
            except Exception:
                return None, None
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


async def main_table_create_filter(search_value: str) -> dict:
    """
    Create a full MongoDB filter query based on the input.
    """
    start_date, end_date = await build_try_dates(search_value)
    search_filter = {}
    search_filter['$or'] = [
        {"batchNumber": {"$regex": search_value, "$options": "i"}},
        {"wheels.wheelId": {"$regex": search_value, "$options": "i"}}
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


async def test_wheels_table_create_filter(search_value: str) -> dict:
    start_date, end_date = await build_try_dates(search_value)
    search_filter = {}
    doc_prefix: str = 'wheels'
    search_filter['$or'] = [
        {f'{doc_prefix}.wheelId': {'$regex': search_value, '$options': 'i'}},
        {f'{doc_prefix}.confirmedBy': {'$regex': search_value, '$options': 'i'}}
    ]
    if start_date and end_date:
        search_filter["$or"].append(
            {'{doc_prefix}.arrivalDate': {'$gte': start_date, '$lte': end_date}}
        )
        search_filter['$or'].append(
            {'{doc_prefix}.testDate': {'$gte': start_date, '$lte': end_date}}
        )
    return search_filter


async def translate_status(search_value: str) -> str:
    match: list[str] = get_close_matches(search_value, WHEELS_STATUS_MAP.keys(), n=1, cutoff=0.3)
    if match:
        return WHEELS_STATUS_MAP[match[0]]
    return ''


async def all_wheels_table_create_filter(search_value: str) -> dict:
    prepare_tasks = []
    prepare_tasks.append(
        build_try_dates(search_value)
    )
    prepare_tasks.append(
        translate_status(search_value)
    )
    prepare_results = await asyncio.gather(*prepare_tasks)
    start_date, end_date = prepare_results[0]
    status_translate = prepare_results[1]
    search_filter = {}
    search_filter['$or'] = [
        { 'wheelId': { '$regex': search_value, '$options': 'i' } },
        { 'receiptDate': { '$gte': start_date, '$lte': end_date } },
    ]
    if status_translate:
        search_filter['$or'].append(
            { 'status': { '$regex': status_translate, '$options': 'i' } },
        )
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
    sanitized_value = sanitized_value.strip()
    return sanitized_value


async def get_period(start: str, end: str) -> tuple[datetime, datetime]:
    start_date: datetime
    end_date: datetime
    try:
        if start:
            start_date = datetime.strptime(start, "%Y-%m-%d")
        else:
            start_date = datetime.min
        if end:
            end_date = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
        else:
            end_date = datetime.max
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid period dates format: {e}")
    return start_date, end_date
# endregion dataFiltering

# region mainTable
# CloselyTied with endPoint == just creating it to show correct data in the `main` table.
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
    filter_tasks = []
    filter_tasks.append(
        main_table_create_filter(cor_search_value)
    )
    if startDate or endDate:
        # Always the same query, we either get date or not.
        # But, we can filter data for some period, with `startDate`, `endDate`.
        filter_tasks.append(
            get_period(startDate, endDate)
        )
    filter_results = await asyncio.gather(*filter_tasks)
    filter_query: dict[str, dict] = filter_results[0]
    if startDate or endDate:
        start_date, end_date = filter_results[1]
        date_field = "createdAt" if createdAtPeriod else "laboratoryTestDate"
        filter_query[date_field] = {"$gte": start_date, "$lte": end_date}
    # We don't need wheels for the `main` table.
    projection = {
        'wheels': 0,
    }
    batches_collection: AsyncIOMotorCollection = await get_db_collection(db, DB_PMK_NAME, CLN_BATCH_NUMBERS)
    db_search_tasks = []
    db_search_tasks.append(
        batches_collection.count_documents({})
    )
    db_search_tasks.append(
        batches_collection.count_documents(filter_query)
    )
    # Default column of the `main` table.
    column_map: list[str] = ['batchNumber', 'createdAt', 'laboratoryTestDate', 'laboratoryPassed']
    sort_column: str = column_map[order_column]
    sort_direction: int = 1 if order_dir == 'asc' else -1
    db_search_tasks.append(
        batches_collection.find(filter_query, projection) \
                          .sort(sort_column, sort_direction) \
                          .skip(start)  \
                          .limit(length) \
                          .to_list(length=length)
    )
    db_search_results = await asyncio.gather(*db_search_tasks)
    total_records: int = db_search_results[0]
    filtered_records: int = db_search_results[1]
    records = db_search_results[2]
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

# region testWheelsTable
@router.get("/tables_data/test_wheels")
async def get_test_wheels_data(
    startDate: str = Query(''),
    endDate: str = Query(''),
    batchNumber: str = Query(...),
    arrivalDatePeriod: bool = Query(True),
    draw: int = Query(...),
    start: int = Query(0),
    length: int = Query(10),
    search_value: str = Query("", alias="search[value]"),
    order_column: int = Query(0, alias="order[0][column]"),
    order_dir: str = Query("asc", alias="order[0][dir]"),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    doc_prefix: str = 'wheels'
    cor_search_value = preprocess_search_value(search_value)
    filter_tasks = []
    filter_tasks.append(
        test_wheels_table_create_filter(cor_search_value)
    )
    if startDate or endDate:
        filter_tasks.append(
            get_period(startDate, endDate)
        )
    filter_task_results = await asyncio.gather(*filter_tasks)
    search_query = filter_task_results[0]
    if startDate or endDate:
        start_date, end_date = filter_task_results[1]
        date_field = 'arrivalDate' if arrivalDatePeriod else 'testDate'
        search_query[doc_prefix + '.' + date_field] = {'$gte': start_date, '$lte': end_date}
    # region basicAggregation
    basic_aggregation = []
    # filterDocs to certain `batchNumber`
    batch_match = {
        '$match' : {
            'batchNumber': batchNumber,
        }
    }
    basic_aggregation.append(batch_match)
    # removeUnwanted fields
    project_wheels = {
        '$project': {
            '_id': 0,
            'wheels': 1,
        }
    }
    basic_aggregation.append(project_wheels)
    # countAllWheels for batch
    count_wheels = {
        "$addFields": {
            "totalRecords": {
                "$size": {
                    "$ifNull": ['$wheels', []]
                } 
            }
        }
    }
    basic_aggregation.append(count_wheels)
    # unwind `wheels` to multiple docs
    unwind_wheels = {
        '$unwind': {
            'path': "$wheels",
            'includeArrayIndex': 'string',
            'preserveNullAndEmptyArrays': False
        },
    }
    basic_aggregation.append(unwind_wheels)
    # search in correct documents
    match_cor_data = {
        '$match': search_query
    }
    basic_aggregation.append(match_cor_data)
    # count filtered
    count_filtered_data = {
        '$setWindowFields': {
            'output': {
                'recordsFiltered': {'$count': {}}
            }
        }
    }
    basic_aggregation.append(count_filtered_data)
    # endregion basicAggregation
    column_map: list[str] = [
        'wheelId', 'arrivalDate', 'testDate', 'result'
    ]
    batches_collection: AsyncIOMotorCollection = await get_db_collection(db, DB_PMK_NAME, CLN_BATCH_NUMBERS)
    sort_column: str = column_map[order_column]
    sort_direction: int = 1 if order_dir == 'asc' else -1
    # region extraAggregation
    sort_stage = {
        '$sort': {
            f'wheels.{sort_column}': sort_direction
        }
    }
    basic_aggregation.append(sort_stage)
    skip_stage = {
        '$skip': start
    }
    basic_aggregation.append(skip_stage)
    limit_stage = {
        '$limit': length
    }
    basic_aggregation.append(limit_stage)
    # endregion extraAggregation
    query_result: list[dict] = await batches_collection.aggregate(basic_aggregation).to_list(length=length)
    total_records: int = 0
    filtered_records: int = 0
    if query_result:
        total_records = query_result[0].get('totalRecords', 0)
        filtered_records = query_result[0].get('recordsFiltered', 0)
    records: list[dict] = [
        record['wheels'] for record in query_result
    ]
    
    
    resp_content = {
        'draw': draw,
        'recordsTotal': total_records,
        'recordsFiltered': filtered_records,
        'data': await async_convert_object_id_and_datetime_to_str(records),
    }
    return JSONResponse(
        content=resp_content,
        status_code=status.HTTP_200_OK,
    )
# endregion testWheelsTable

# region allWheelsTable
@router.get('/tables_data/all_wheels')
async def get_test_wheels_data(
    startDate: str = Query(''),
    endDate: str = Query(''),
    draw: int = Query(...),
    start: int = Query(0),
    length: int = Query(10),
    search_value: str = Query("", alias="search[value]"),
    order_column: int = Query(0, alias="order[0][column]"),
    order_dir: str = Query("asc", alias="order[0][dir]"),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
):
    cor_search_value = preprocess_search_value(search_value)
    filter_tasks = []
    filter_tasks.append(
        all_wheels_table_create_filter(cor_search_value)
    )
    filter_tasks.append(
        get_period(startDate, endDate)
    )
    filter_task_results = await asyncio.gather(*filter_tasks)
    filter_query = filter_task_results[0]
    if startDate or endDate:
        start_date, end_date = filter_task_results[1]
        filter_query['receiptDate'] = {'$gte': start_date, '$lte': end_date}
    projection = {
        'batchNumber': 0,
        'transferData': 0,
        'sqlData': 0, 
    }
    wheels_collection = await get_db_collection(db, DB_PMK_NAME, CLN_WHEELS)
    table_columns: list[str] = ['wheelId', 'receiptDate', 'status']
    sort_column: str = table_columns[order_column]
    sort_direction: int = 1 if order_dir == 'asc' else -1
    db_search_tasks = []
    db_search_tasks.append(
        wheels_collection.count_documents({})
    )
    db_search_tasks.append(
        wheels_collection.count_documents(filter_query)
    )
    # region basicAggregation
    agreggation_stages = []
    # Filter wheels on input
    wheels_match = {
        '$match': filter_query
    }
    agreggation_stages.append(wheels_match)
    wheels_projection = {
        '$project': projection
    }
    agreggation_stages.append(wheels_projection)
    # Add data about wheelstack -> otherwise we don't know about placement
    wheelstack_lookup = {
        '$lookup': {
            'from': 'wheelStacks',
            'localField': 'wheelStack.wheelStackId',
            'foreignField': '_id',
            'as': 'wheelStack.wheelstackData'
        }
    }
    agreggation_stages.append(wheelstack_lookup)
    # Add data about wheelstack_placement == wheel_placement, and we only care about name :)
    grid_lookup = {
        '$lookup': {
            'from': 'basePlatform',
            'localField': 'wheelStack.wheelstackData.placement.placementId',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        'name': 1,
                    }
                }
            ],
            'as': 'wheelStack.placementName.basePlatform'
        },
    }
    agreggation_stages.append(grid_lookup)
    base_platform_lookup = {
        '$lookup': {
            'from': 'grid',
            'localField': 'wheelStack.wheelstackData.placement.placementId',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        'name': 1
                    }
                }
            ],
            'as': 'wheelStack.placementName.grid'
        }
    }
    agreggation_stages.append(base_platform_lookup)
    sort_records = {
        '$sort': {
            sort_column: sort_direction
        }
    }
    agreggation_stages.append(sort_records)
    skip_records = {
        '$skip': start
    }
    agreggation_stages.append(skip_records)
    limit_records = {
        '$limit': length
    }
    agreggation_stages.append(limit_records)
    # endregion basicAggregation
    aggregation_task = wheels_collection.aggregate(agreggation_stages).to_list(length=length)
    db_search_tasks.append(aggregation_task)
    db_search_results = await asyncio.gather(*db_search_tasks)
    total_records: int = db_search_results[0]
    filtered_records: int = db_search_results[1]
    records = db_search_results[2]
    records_data = await async_convert_object_id_and_datetime_to_str(records)
    resp_content = {
        'draw': draw,
        'recordsTotal': total_records,
        'recordsFiltered': filtered_records,
        'data': records_data
    }
    return JSONResponse(
        content=resp_content,
        status_code=status.HTTP_200_OK,
    )
# endregion allWheelsTable


@router.post(
    path='/update_status',
    name='Update batch status',
    description='Updates last tested wheel data + updates `batchNumber` status.',
)
async def route_post_lab_result(
    batchNumber: str = Body(...),
    wheelObjectId: str = Body(...),
    testResult: bool = Body(...),
    forceUpdate: bool = Query(False,
                              description='Override previous result and date.'),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
    token_data: dict = get_role_verification_dependency(LAB_PAGE_ACTION_ROLES | ADMIN_ACCESS_ROLES),
):
    batch_collection: AsyncIOMotorCollection = await get_db_collection(db, DB_PMK_NAME, CLN_BATCH_NUMBERS)
    wheel_object_id = await get_object_id(wheelObjectId)
    update_date = await time_w_timezone()
    confirmed_by = token_data.get('sub', 'Anonymous')
    query = {
        'batchNumber': batchNumber,
        'wheels._id': wheel_object_id,
    }
    update_str = 'wheels.$[element]'
    update = {
        '$set': {
            'laboratoryPassed': testResult,
            'laboratoryTestDate': update_date,
            f'{update_str}.result': testResult,
            f'{update_str}.testDate': update_date,
            f'{update_str}.confirmedBy': confirmed_by,
        }
    }
    array_filters = [
        {'element._id': wheel_object_id, 'element.testDate': None},
    ]
    if forceUpdate:
        array_filters[0]['element.result'] = None
    result = await batch_collection.update_one(
        query, update, array_filters=array_filters
    )
    if 0 == result.matched_count:
        raise HTTPException(
            detail='Wheel not found, or already tested',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    updated_data = {
        'batchNumber': batchNumber,
        'laboratoryTestDate': update_date,
        'laboratoryPassed': testResult,
    }
    cor_updated_data = await async_convert_object_id_and_datetime_to_str(updated_data)
    return JSONResponse(
        content=cor_updated_data,
        status_code=status.HTTP_200_OK,
    )


@router.post(
    path='/request_wheel',
    name='Request Wheel Transfer',
    description='Request wheel to the laboratory, creates move order',
)
async def route_post_request_wheel(
    wheelObjectId: str = Query(...),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client),
    token_data: dict = get_role_verification_dependency(LAB_PAGE_ACTION_ROLES | ADMIN_ACCESS_ROLES),
):
    requested_by: str = token_data['sub']
    wheel_object_id: ObjectId = await get_object_id(wheelObjectId)
    wheel_collection = await get_db_collection(db, DB_PMK_NAME, CLN_WHEELS)
    aggregation_stages = []
    match_stage = {
        '$match': {
            '_id': wheel_object_id,
        },
    }
    aggregation_stages.append(match_stage)
    proj_stage = {
        '$project': {
            'transferData': 0,
            'sqlData': 0,
        }
    }
    aggregation_stages.append(proj_stage)
    wheelstack_stage = {
        '$lookup': {
            'from': 'wheelStacks',
            'localField': 'wheelStack.wheelStackId',
            'foreignField': '_id',
            'as': 'wheelstackData',
        },
    }
    aggregation_stages.append(wheelstack_stage)
    placement_stage = {
        '$lookup': {
            'from': 'grid',
            'localField': 'wheelstackData.placement.placementId',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        'name': 1,
                        'extra': 1,
                    }
                }
            ],
            'as': 'placementData.grid',
        }
    }
    aggregation_stages.append(placement_stage)
    wheels_data = await wheel_collection.aggregate(aggregation_stages).to_list(length=None)
    if not wheels_data:
        raise HTTPException(
            detail='Wheel not found',
            status_code=status.HTTP_404_NOT_FOUND,
        )
    logger.error(wheels_data)
    target_wheel_data = wheels_data[0]
    wheelstack_data = target_wheel_data['wheelstackData'][0]
    if not wheelstack_data:
        raise HTTPException(
            detail='Wheels in not placed in wheelstack',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    if wheelstack_data['blocked']:
        raise HTTPException(
            detail='wheelstack blocked',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    placement_data: str = wheelstack_data['placement']
    placement_type: str = placement_data['type']
    if placement_type != WH_GRID:
        raise HTTPException(
            detail='Request available only from the `grid`',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    placement_id: ObjectId = placement_data['placementId']
    placement_row: str = wheelstack_data['rowPlacement']
    placement_col: str = wheelstack_data['colPlacement']
    grid_data = target_wheel_data['placementData']['grid'][0]
    extra_element = grid_data['extra']
    if WH_LABORATORY not in extra_element:
        raise HTTPException(
            detail=f'Placement {grid_data['name']} doesnt have assigned `laboratory` element',
            status_code=status.HTTP_403_FORBIDDEN,
        )
    order_data = {
        'orderName': 'LabMoveRequest',
        'orderDescription': f'Request from lab personal -> {requested_by}',
        'source': {
            'placementType': 'grid',
            'placementId': placement_id,
            'rowPlacement': placement_row,
            'columnPlacement': placement_col,
        },
        'destination': {
            'placementType': 'grid',
            'placementId': placement_id,
            'elementName': 'laboratory',
        },
        'chosenWheel': wheel_object_id
    }
    created_order = await orders_create_move_to_laboratory(db, order_data)
    return JSONResponse(
        content={'orderId': str(created_order)},
        status_code=status.HTTP_200_OK,
    )
# endregion
