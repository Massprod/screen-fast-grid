from fastapi import FastAPI
from dotenv import load_dotenv
# from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_connection import mongo_client
from database.grid.grid_constructor import GridConstructor
from loguru import logger
from routers.wheels.router import router as wheel_router
from routers.wheelstacks.router import router as wheelstack_router
from routers.grid.router import router as grid_router
from routers.base_platform.router import router as platform_router
from routers.orders.router import router as orders_router
from fastapi.middleware.cors import CORSMiddleware


load_dotenv('.env')


logger.add(
    f'logs/logs.log',
    rotation='50 MB',
    retention='14 days',
    compression='zip',
    backtrace=True,
    diagnose=True,
)


app = FastAPI(
    title='Back Screen',
    version='0.0.1',
    description='Back part of the screen app',
)
origins = [
    "http://127.0.0.1:5500",
    "http://localhost:5500",
]

# Add CORS middleware to your FastAPI app
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allow specific origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Allow all headers
)

app.include_router(wheel_router, prefix='/wheels', tags=['wheel'])
app.include_router(wheelstack_router, prefix='/wheelstacks', tags=['WheelStack'])
app.include_router(grid_router, prefix='/grid', tags=['grid'])
app.include_router(platform_router, prefix='/platform', tags=['platform'])
app.include_router(orders_router, prefix='/orders', tags=['orders'])


@app.on_event('startup')
async def create_basic_grid():
    # Creating all `grid` DB, and it's essential collections.
    # All of these collections are going to be used for populating.
    # So, we need an empty version of it from the beginning.
    # db: AsyncIOMotorClient = await get_mongo_db_client()
    empty_grid_conf = GridConstructor()
    empty_grid_conf.set_pmk_preset()
    empty_grid_conf.set_grid()
    await empty_grid_conf.set_collections_schemas(mongo_client.get_client())
    await empty_grid_conf.initiate_empty_grid_db(mongo_client.get_client())
    # Change after we see the complete idea.
    base_preset_rows: int = 2
    base_preset_rows_data = {}
    for row in range(1, base_preset_rows + 1):
        base_preset_rows_data[row] = {
            'columns': 4,
            'white_spaces': []
        }
    await empty_grid_conf.create_base_placement_db(mongo_client.get_client(), base_preset_rows, base_preset_rows_data)


@app.on_event('shutdown')
async def close_db():
    mongo_client.close_client()
