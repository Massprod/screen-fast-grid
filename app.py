from loguru import logger
from fastapi import FastAPI
from dotenv import load_dotenv
from database.mongo_connection import mongo_client
from database.presets.presets import create_pmk_grid_preset, create_pmk_platform_preset
from database.collections.collections import create_basic_collections
from routers.wheels.router import router as wheel_router
from routers.wheelstacks.router import router as wheelstack_router
from routers.grid.router import router as grid_router
from routers.base_platform.router import router as platform_router
# from routers.orders.router import router as orders_router
from routers.presets.router import router as presets_router
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from constants import PRES_PMK_GRID, PRES_PMK_PLATFORM, DB_PMK_NAME, CLN_PRESETS
from routers.presets.crud import add_new_preset, get_preset_by_name


load_dotenv('.env')


logger.add(
    f'logs/logs.log',
    rotation='50 MB',
    retention='14 days',
    compression='zip',
    backtrace=True,
    diagnose=True,
)


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
    "http://127.0.0.1:5500",
    "http://localhost:5500",
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allow specific origins, from which we allow connections.
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Allow all headers
)

app.include_router(presets_router, prefix='/presets', tags=['Preset'])
app.include_router(grid_router, prefix='/grid', tags=['Grid'])
app.include_router(platform_router, prefix='/platform', tags=['Platform'])
app.include_router(wheel_router, prefix='/wheels', tags=['Wheels'])
app.include_router(wheelstack_router, prefix='/wheelstacks', tags=['WheelStack'])
# app.include_router(orders_router, prefix='/orders', tags=['orders'])


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


# Deprecated, changed to `lifespan`
# @app.on_event('shutdown')
async def close_db():
    mongo_client.close_client()
