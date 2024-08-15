from loguru import logger
from uuid import uuid4
from fastapi import FastAPI, Request
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from database.mongo_connection import mongo_client
from routers.grid.router import router as grid_router
from routers.wheels.router import router as wheel_router
from routers.orders.router import router as orders_router
from routers.storages.router import router as storages_router
from routers.presets.router import router as presets_router
from routers.base_platform.router import router as platform_router
from routers.wheelstacks.router import router as wheelstack_router
from routers.presets.crud import add_new_preset, get_preset_by_name
from routers.batch_numbers.router import router as batch_numbers_router
from database.collections.collections import create_basic_collections
from constants import PRES_PMK_GRID, PRES_PMK_PLATFORM, DB_PMK_NAME, CLN_PRESETS
from database.presets.presets import create_pmk_grid_preset, create_pmk_platform_preset


# TODO: We need to change records CREATION for some cases.
#  Because, if we're getting multiple requests when we
#  need to find something, and if it doesnt exist we create it.
#  With multiple requests at the same time, we're going to get DUPLICATE_ERROR,
#  and transactions are going to fail => request fail.
load_dotenv('.env')


logger.add(
    f'logs/logs.log',
    rotation='50 MB',
    retention='14 days',
    compression='zip',
    backtrace=True,
    diagnose=True,
)

# TODO: If we're going to satisfy with our solution, and have more time.
#  Refactor orders and actually everything, because there's a LOT of copies.
#  Which we can cull, and create a single method for SOURCE|DEST checks etc.
#  For now, we're just leaving it like this, no time to bother.


@asynccontextmanager
async def lifespan(app: FastAPI):
    await prepare_db()
    yield
    await close_db()

# TODO: remove debug, after completion.
app = FastAPI(
    title='Back Screen',
    version='0.0.1',
    description='Back part of the screen app',
    lifespan=lifespan,
    debug=True,
)
# TODO: Change middleware after we actually complete project.
#  we should change `origins` to the server addresses we want to allow connections.
origins = [
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "http://localhost:5500",
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(presets_router, prefix='/presets', tags=['Preset'])
app.include_router(grid_router, prefix='/grid', tags=['Grid'])
app.include_router(platform_router, prefix='/platform', tags=['Platform'])
app.include_router(batch_numbers_router, prefix='/batch_number', tags=['Batch'])
app.include_router(wheel_router, prefix='/wheels', tags=['Wheels'])
app.include_router(wheelstack_router, prefix='/wheelstacks', tags=['WheelStack'])
app.include_router(orders_router, prefix='/orders', tags=['Orders'])
app.include_router(storages_router, prefix='/storages', tags=['Storages'])


# Deprecated, changed to `lifespan`
# @app.on_event('startup')
async def prepare_db():
    # Basic setup:
    # Creating all of collections we need and assigning their schemas.
    # After that we need to create `grid` and `basePlatform`
    #  and use their `objectId`s on Front.
    db = mongo_client.get_client()
    # SCHEMAS +++ (Creating all basic collections)
    logger.info('Started creation of basic DB collections')
    await create_basic_collections(db)
    logger.info('Ended creation of basic DB collections')
    # --- SCHEMAS
    # PRESETS +++ (Creating all basic presets)
    exist = await get_preset_by_name(PRES_PMK_GRID, db, DB_PMK_NAME, CLN_PRESETS)
    if exist is None:
        logger.info(f"Creating preset data, for `presetName` =  {PRES_PMK_GRID}")
        pmk_grid_preset = await create_pmk_grid_preset()
        logger.info(f'Completed creation of data for `presetName` = {PRES_PMK_GRID}')
        await add_new_preset(pmk_grid_preset, db, DB_PMK_NAME, CLN_PRESETS)
    exist = await get_preset_by_name(PRES_PMK_PLATFORM, db, DB_PMK_NAME, CLN_PRESETS)
    if exist is None:
        logger.info(f"Creating preset data, for `presetName` =  {PRES_PMK_PLATFORM}")
        pmk_platform_preset = await create_pmk_platform_preset()
        logger.info(f"Completed creation of data for `presetName` = {PRES_PMK_PLATFORM}")
        await add_new_preset(pmk_platform_preset, db, DB_PMK_NAME, CLN_PRESETS)
    # --- PRESETS


@app.middleware("http")
async def requests_logging(
        request: Request, call_next
):
    # Check for an existing request ID in the headers
    request_id = request.headers.get("X-Request-ID", str(uuid4()))  # Use existing or generate a new one
    # Add the request ID to the logger context or explicitly log it
    logger.info(f"Incoming request: {request.method} {request.url} | Request ID: {request_id}")
    # Proceed with the request
    response = await call_next(request)
    # Log response completion
    logger.info(
        f"Completed request: {request.method} {request.url} |"
        f" Status: {response.status_code} |"
        f" Request ID: {request_id}"
    )
    # Optionally, add the request ID to the response headers
    response.headers["X-Request-ID"] = request_id
    return response


async def close_db():
    mongo_client.close_client()
