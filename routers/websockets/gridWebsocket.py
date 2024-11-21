from datetime import datetime
import itertools
import json
import asyncio
from bson import ObjectId
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient
from routers.base_platform.crud import get_platform_by_object_id
from routers.batch_numbers.crud import db_find_batch_numbers_many, db_find_batch_numbers_w_unplaced
from database.mongo_connection import mongo_client
from auth.jwt_validation import websocket_verify_multi_roles_token
from fastapi import (
    APIRouter,
    WebSocket,
    Query,
    WebSocketException,
    status,
    Depends,
    WebSocketDisconnect
)
from constants import (
    BASIC_PAGE_ACTION_ROLES,
    BASIC_PAGE_VIEW_ROLES,
    CLN_ACTIVE_ORDERS,
    CLN_BASE_PLATFORM,
    CLN_BATCH_NUMBERS,
    CLN_CANCELED_ORDERS,
    CLN_COMPLETED_ORDERS,
    CLN_GRID,
    CLN_STORAGES,
    CLN_WHEELS,
    CLN_WHEELSTACKS,
    DB_PMK_NAME,
    PT_BASE_PLATFORM,
    PT_GRID,
)
from routers.grid.crud import get_grid_by_object_id
from routers.grid.data_gather import convert_and_store_threadpool
from routers.history.history_actions import background_history_record
from routers.orders.crud import db_get_orders_by_id_many
from routers.storages.crud import db_get_storages_with_elements_data
from routers.wheels.crud import db_find_many_wheels_by_id, db_find_wheels_free_fields
from routers.wheelstacks.crud import db_history_get_placement_wheelstacks
from routers.wheelstacks.router import create_new_wheelstack_action
from utility.utilities import (
    async_convert_object_id_and_datetime_to_str,
    get_object_id,
    handle_http_exceptions_for_websocket
)


router = APIRouter()


active_grid_page_connections: dict[WebSocket, bool] = {}


async def create_json_req_resp(type, task, data, handler = ''):
    req_resp = {
        'type': type,
        'filter': {
            'task': task
        },
        'data': data,
        'handler': handler,
    }
    return json.dumps(req_resp)


async def placement_update_action(
        db: AsyncIOMotorClient, settings: dict,
) -> dict:
    init_tasks = []
    ignored_dates: list[datetime] = []
    if settings['lastChange']:
        ignored_dates.append(settings['lastChange'])
    if PT_GRID == settings['placementType']:
        init_tasks.append(
            get_grid_by_object_id(
                settings['placementId'], db, DB_PMK_NAME, CLN_GRID, ignored_dates
            )
        )
    elif PT_BASE_PLATFORM == settings['placementType']:
        init_tasks.append(
            get_platform_by_object_id(
                settings['placementId'], db, DB_PMK_NAME, CLN_BASE_PLATFORM, ignored_dates
            )
        )
    if settings['includeWheelstacks']:
        init_tasks.append(
            db_history_get_placement_wheelstacks(
                settings['placementId'], settings['placementType'],
                db, DB_PMK_NAME, CLN_WHEELSTACKS,  [settings['placementType']], True
            )
        )
    init_results = await asyncio.gather(*init_tasks)
    placement_data = init_results[0]
    if not placement_data:
        return {}
    placement_data['wheelstacksData'] = {}
    placement_data['wheels'] = {}
    # Not gathering `wheel`s without `wheelstacks`...
    if not settings['includeWheelstacks']:
        return placement_data
    placement_wheelstacks = init_results[1]
    if placement_wheelstacks is None: 
        return placement_data
    placement_wheels = []
    conversion_tasks = []
    conversion_tasks.append(
        convert_and_store_threadpool(placement_wheelstacks)
    )
    if settings['includeWheels']:
        for wheelstack in placement_wheelstacks:
            placement_wheels.extend(wheelstack['wheels'])
        if placement_wheels:
            wheels_data: list[dict] = await db_find_many_wheels_by_id(
                placement_wheels, db, DB_PMK_NAME, CLN_WHEELS
            )
            conversion_tasks.append(
                convert_and_store_threadpool(wheels_data)
            )
    conversion_results = await asyncio.gather(*conversion_tasks)
    placement_data['wheelstacksData'] = conversion_results[0]
    placement_data['wheelsData'] = conversion_results[-1]
    placement_data['placementType'] = settings['placementType']
    return placement_data


async def filter_req_data(req_data: dict, db: AsyncIOMotorClient):
    req_resp: dict[str, str | dict]
    req_type: str = req_data['type']
    req_task: str = req_data['filter']['task']
    req_data_filter: dict = req_data['filter']['dataFilter']
    req_handler: str = req_data.get('handler', '')
    # TODO: we can create filter with `dict`, but the problem is args.
    #  We can specify default args for tasks, but what about extra?
    #  Some DB requests using req args as first ones...later
    #  Move this all to correct place, later.
    # region GATHER
    if 'gather' == req_type:
        # region batchNumbersWUnplaced
        if 'batchNumbersWUnplaced' == req_task:
            data = await db_find_batch_numbers_w_unplaced(
                db, DB_PMK_NAME, CLN_BATCH_NUMBERS,
            )
            cor_data = await async_convert_object_id_and_datetime_to_str(data)
            req_resp = await create_json_req_resp(
                'dataUpdate', 'batchNumbersWUnplaced', cor_data
            )
        # endregion batchNumbersWUnplaced
        # region wheelsUnplaced
        elif 'wheelsUnplaced' == req_task:
            req_batch_number = req_data_filter.get('batchNumber', '')
            req_status = req_data_filter.get('status', '')
            query_fields = {
                'batchNumber': req_batch_number,
                'status': req_status,
            }
            data = await db_find_wheels_free_fields(
                db, DB_PMK_NAME, CLN_WHEELS, query_fields
            )
            cor_data = await async_convert_object_id_and_datetime_to_str(data)
            resp_data = {
                'wheels': cor_data,
                'batchNumber': req_batch_number,
            }
            req_resp = await create_json_req_resp(
                'dataUpdate', 'wheelsUnplaced', resp_data, req_handler
            )
        # endregion wheelsUnplaced
        # region tempoStorage
        elif 'expandedStorage' == req_task:
            storage_name: str = req_data_filter['name']
            last_change = req_data_filter.get('lastChange', None)
            ignore_date: datetime | None = datetime.fromisoformat(req_data_filter['lastChange']) if last_change else None
            identifiers: list[dict] = [
                {'name': storage_name}
            ]
            data = await db_get_storages_with_elements_data(
                identifiers, db, DB_PMK_NAME, CLN_STORAGES, None, ignore_date
            )
            cor_data = None
            if data:
                cor_data = await async_convert_object_id_and_datetime_to_str(data[0])  # using this for multi gather == array
            req_resp = await create_json_req_resp(
                'dataUpdate', 'expandedStorage', cor_data
            )
        # endregion tempoStorage
        # region placementData
        elif 'placementUpdate' == req_task:
            placement_id: ObjectId = await get_object_id(req_data_filter['placementId'])
            placement_name: str = req_data_filter['placementName']
            placement_type: str = req_data_filter['placementType']
            last_change = req_data_filter.get('lastChange', None)
            last_change: datetime = datetime.fromisoformat(req_data_filter['lastChange']) if last_change else None
            include_wheelstacks: bool = req_data_filter.get('includeWheelstacks', False)
            include_wheels: bool = req_data_filter.get('includeWheels', False)
            action_settings = {
                'placementId': placement_id,
                'placementName': placement_name,
                'placementType': placement_type,
                'lastChange': last_change,
                'includeWheelstacks': include_wheelstacks,
                'includeWheels': include_wheels,
            }
            placement_data: dict = await placement_update_action(
                db, action_settings
            )
            cor_placement_data: dict = await async_convert_object_id_and_datetime_to_str(placement_data)
            req_resp = await create_json_req_resp(
                'dataUpdate', 'placementUpdate', cor_placement_data
            )
        # endregion placementData
        # region batchesData
        elif 'batchesData' == req_task:
            batch_numbers: list[str] = req_data_filter['batchNumbers']
            batches_data: list[dict] = await db_find_batch_numbers_many(
                batch_numbers, db, DB_PMK_NAME, CLN_BATCH_NUMBERS
            )
            cor_batches_data: list[dict] = await async_convert_object_id_and_datetime_to_str(batches_data)
            req_resp = await create_json_req_resp(
                'dataUpdate', 'batchesUpdate', cor_batches_data
            )
        # endregion batchesData
        # region ordersData
        elif 'ordersData' == req_task:
            orders: list[str] = req_data_filter['orders']
            convert_tasks = []
            for order_id in orders:
                convert_tasks.append(
                    get_object_id(order_id)
                )
            convert_results = await asyncio.gather(*convert_tasks)
            # This idea with 3 collection for 3 orderTypes...
            # We need to rebuild :)
            # For now, just check all we choose in message.
            check_collections: list[str] = {
                CLN_ACTIVE_ORDERS: req_data_filter.get('activeOrders', False),
                CLN_COMPLETED_ORDERS: req_data_filter.get('completedOrders', False),
                CLN_CANCELED_ORDERS: req_data_filter.get('canceledOrders', False),
            }
            gather_tasks = []
            for collection, include in check_collections.items():
                if include:
                    gather_tasks.append(
                        db_get_orders_by_id_many(
                            convert_results, db, DB_PMK_NAME, collection 
                        )
                    )
            gather_results = await asyncio.gather(*gather_tasks)
            # flatten + parse into `list` | without creating a new list with all lists nested - generator == chain()
            orders_data = list(itertools.chain(*gather_results))
            cor_orders_data: list[dict] = await async_convert_object_id_and_datetime_to_str(orders_data)
            req_resp = await create_json_req_resp(
                'dataUpdate', 'ordersUpdate', cor_orders_data
            )
        # endregion ordersData
    # endregion GATHER
    # + create +
    elif 'create' == req_type:
        # + wheelstackCreation + <- with BG(asyncio.task) placement record
        if 'wheelstackCreation' == req_task:
            wheelstack_data = req_data_filter['wheelstackData']
            data = await create_new_wheelstack_action(
                db, wheelstack_data,
            )
            # + HISTORY RECORD +
            created_wheelstack_data = data['usedData']
            placement_id: ObjectId = created_wheelstack_data['placement']['placementId']
            placement_type: str = created_wheelstack_data['placement']['type']
            asyncio.create_task(
                background_history_record(placement_id, placement_type, db)
            )
            # - HISTORY RECORD -
            cor_data = await async_convert_object_id_and_datetime_to_str(data)
            req_resp = await create_json_req_resp(
                'create', 'wheelstackCreation', cor_data
            )
        # - wheelstackCreation -
    # - create -
    return req_resp


@router.websocket("/grid_page")
async def websocket_endpoint(
    websocket: WebSocket,
    auth_token: str = Query(..., description='Authorization token'),
    db: AsyncIOMotorClient = Depends(mongo_client.depend_client)
):
    await websocket.accept()
    active_grid_page_connections[websocket] = True
    # TODO: Only validating it once. For now.
    #  Ideally we should check it for every request?
    # But it will slow us and is there a real reason?
    try:
        token_data = await websocket_verify_multi_roles_token(
            BASIC_PAGE_ACTION_ROLES | BASIC_PAGE_VIEW_ROLES,
            auth_token,
        )
    except WebSocketException as exception:
        await websocket.close(code=exception.code)
        logger.error(f'WebSocket connection cloded, reason: {exception.reason}')
        del active_grid_page_connections[websocket]
        return
    
    try:
        while True:
            try:
                req_data = await websocket.receive_text()
                
                cor_req_data = json.loads(req_data)
                result = await handle_http_exceptions_for_websocket(
                    filter_req_data, cor_req_data, db
                )
                await websocket.send_text(result)

            except asyncio.TimeoutError:
                logger.warning("WebSocket connection timed out")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                break
            except WebSocketException as exception:
                if exception.code == status.WS_1011_INTERNAL_ERROR:
                    logger.error(f"WebSocket connection closed: {exception.reason}")
                    await websocket.close(code=exception.code)
                    del active_grid_page_connections[websocket]
                    break
                logger.error(
                    f'Websocket exception: {exception}'
                )
                exception_data: dict = {
                    'type': 'error',
                    'code': exception.code,
                    'message': exception.reason,
                }
                await websocket.send_json(exception_data)
            except Exception as exception:
                exception_data: dict = {
                    'type': 'error',
                    'code': status.WS_1010_MANDATORY_EXT,
                    'message': exception,
                }
                await websocket.send_json(exception_data)
                break

    except WebSocketDisconnect:
        logger.info('Client disconnected')
    finally:
        await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
        if websocket in active_grid_page_connections:
            del active_grid_page_connections[websocket]
            logger.info("WebSocket connection removed from active connections")
