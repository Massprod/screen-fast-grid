import os
from uuid import uuid4
from loguru import logger
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from database.mongo_connection import mongo_client
from fastapi.middleware.cors import CORSMiddleware
from routers.base_platform.crud import get_platform_by_name, create_platform
from routers.grid.crud import collect_wheelstack_cells, get_grid_by_name, create_grid
from routers.grid.router import router as grid_router
from routers.wheels.router import router as wheel_router
from routers.orders.router import router as orders_router
from routers.history.router import router as history_router
from routers.presets.router import router as presets_router
from routers.storages.router import router as storages_router
from routers.base_platform.router import router as platform_router
from routers.wheelstacks.router import router as wheelstack_router
from routers.presets.crud import add_new_preset, get_preset_by_name
from database.collections.collections import create_basic_collections
from routers.batch_numbers.router import router as batch_numbers_router
from database.presets.presets import create_pmk_grid_preset, create_pmk_platform_preset
from constants import PRES_PMK_GRID, PRES_PMK_PLATFORM, DB_PMK_NAME, CLN_PRESETS, CLN_BASE_PLATFORM, CLN_GRID

# TODO: We need to change records CREATION for some cases.
#  Because, if we're getting multiple requests when we
#  need to find something, and if it doesnt exist we create it.
#  With multiple requests at the same time, we're going to get DUPLICATE_ERROR,
#  and transactions are going to fail => request fail.
load_dotenv('.env')


log_dir = 'logs/'
os.makedirs(log_dir, exist_ok=True)

logger.add(
    os.path.join(log_dir, 'log.log'),
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
app.include_router(history_router, prefix='/history', tags=['History'])


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
    grid_preset = await get_preset_by_name(PRES_PMK_GRID, db, DB_PMK_NAME, CLN_PRESETS)
    if grid_preset is None:
        logger.info(f"Creating preset data, for `presetName` =  {PRES_PMK_GRID}")
        pmk_grid_preset = await create_pmk_grid_preset()
        logger.info(f'Completed creation of data for `presetName` = {PRES_PMK_GRID}')
        await add_new_preset(pmk_grid_preset, db, DB_PMK_NAME, CLN_PRESETS)
    platform_preset = await get_preset_by_name(PRES_PMK_PLATFORM, db, DB_PMK_NAME, CLN_PRESETS)
    if platform_preset is None:
        logger.info(f"Creating preset data, for `presetName` =  {PRES_PMK_PLATFORM}")
        pmk_platform_preset = await create_pmk_platform_preset()
        logger.info(f"Completed creation of data for `presetName` = {PRES_PMK_PLATFORM}")
        await add_new_preset(pmk_platform_preset, db, DB_PMK_NAME, CLN_PRESETS)
    # --- PRESETS
    # TODO: think about changing it!
    # BAD SOLUTION == creating basic `grid` & `basePlatform` for PMK.
    create_pmk_preset = os.getenv('CREATE_PMK_PRESETS', '')
    if create_pmk_preset:
        # + basePlatform +
        pmk_platform_name = os.getenv('PMK_PLATFORM_NAME', 'pmkBase1')
        platform_exists = await get_platform_by_name(
            pmk_platform_name, db, DB_PMK_NAME, CLN_BASE_PLATFORM
        )
        if not platform_exists:
            logger.info(
                f'Creating basic `basePlatform` placement => {pmk_platform_name}'
            )
            cor_platform_data = await collect_wheelstack_cells(platform_preset)
            cor_platform_data['name'] = pmk_platform_name
            res = await create_platform(
                cor_platform_data, db, DB_PMK_NAME, CLN_BASE_PLATFORM
            )
            if not res:
                logger.error(
                    f'Failed to create basic `basePlatform` placement => {pmk_platform_name}'
                )
            else:
                logger.info(
                    f'Created basic `basePlatform` placement => {pmk_platform_name}'
                )
        # - basePlatform -
        # + grid +
        pmk_grid_name = os.getenv('PMK_GRID_NAME', 'pmkGrid1')
        grid_exists = await get_grid_by_name(
            pmk_grid_name, db, DB_PMK_NAME, CLN_GRID
        )
        if not grid_exists:
            logger.info(
                f'Creating basic `grid` placement => {pmk_grid_name}'
            )
            cor_grid_data = await collect_wheelstack_cells(grid_preset)
            cor_grid_data['name'] = pmk_grid_name
            res = await create_grid(
                cor_grid_data, db, DB_PMK_NAME, CLN_GRID
            )
            if not res:
                logger.error(
                    f'Failed to create basic `grid` placement => {pmk_grid_name}'
                )
            else:
                logger.info(
                    f'Created basic `grid` placement => {pmk_grid_name}'
                )
        # - grid -


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
