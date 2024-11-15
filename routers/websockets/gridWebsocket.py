import json
import asyncio
from bson import ObjectId
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient
from routers.batch_numbers.crud import db_find_batch_numbers_w_unplaced
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
    CLN_BATCH_NUMBERS,
    CLN_WHEELS,
    DB_PMK_NAME,
)
from routers.history.history_actions import background_history_record
from routers.wheels.crud import db_find_wheels_free_fields
from routers.wheelstacks.router import create_new_wheelstack_action
from utility.utilities import async_convert_object_id_and_datetime_to_str, get_object_id, handle_http_exceptions_for_websocket


router = APIRouter()


active_grid_page_connections: dict[WebSocket, bool] = {}


async def create_json_req_resp(type, task, data):
    req_resp = {
        'type': type,
        'filter': {
            'task': task
        },
        'data': data
    }
    return json.dumps(req_resp)


async def filter_req_data(req_data, db: AsyncIOMotorClient):
    req_resp: str
    req_type: str = req_data['type']
    req_task: str = req_data['filter']['task']
    req_data_filter: str = req_data['filter']['dataFilter']
    # TODO: we can create filter with `dict`, but the problem is args.
    #  We can specify default args for tasks, but what about extra?
    #  Some DB requests using req args as first ones...later
    # + gather +
    if 'gather' == req_type:
        # + batchNumbersWUnplaced +
        if 'batchNumbersWUnplaced' == req_task:
            data = await db_find_batch_numbers_w_unplaced(
                db, DB_PMK_NAME, CLN_BATCH_NUMBERS,
            )
            cor_data = await async_convert_object_id_and_datetime_to_str(data)
            req_resp = await create_json_req_resp(
                'dataUpdate', 'batchNumbersWUnplaced', cor_data
            )
            return req_resp
        # - batchNumbersWUnplaced -
        # + wheelsUnplaced +
        elif 'wheelsUnplaced' == req_task:
            data = await db_find_wheels_free_fields(
                db, DB_PMK_NAME, CLN_WHEELS, req_data_filter 
            )
            cor_data = await async_convert_object_id_and_datetime_to_str(data)
            req_resp = await create_json_req_resp(
                'dataUpdate', 'wheelsUnplaced', cor_data
            )
            return req_resp
        # - wheelsUnplaced -
    # - gather -
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
            return req_resp
        # - wheelstackCreation -
    # - create -


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
