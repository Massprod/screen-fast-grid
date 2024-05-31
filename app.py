from fastapi import FastAPI
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_connection import get_mongo_db_client
from database.grid.grid_constructor import GridConstructor
from loguru import logger
from routers.wheels.router import router as wheel_router


load_dotenv('.env')

logger.add(
    f'logs/logs.log',
    rotation='50 MB',
    retention='14 days',
    compression='zip',
    backtrace=True,
    diagnose=True,
)

app = FastAPI()
app.include_router(wheel_router, prefix='/wheels', tags=['wheel'])


@app.on_event('startup')
async def create_basic_grid():
    # Creating all `grid` DB, and it's essential collections.
    # All of these collections are going to be used for populating.
    # So, we need an empty version of it from the beginning.
    db: AsyncIOMotorClient = await get_mongo_db_client()
    empty_grid_conf = GridConstructor()
    empty_grid_conf.set_pmk_preset()
    empty_grid_conf.set_grid()
    await empty_grid_conf.set_collections_schemas(db)
    await empty_grid_conf.initiate_empty_grid_db(db)
    # Change after we see the complete idea.
    base_preset_rows: int = 2
    base_preset_rows_data = {}
    for row in range(1, base_preset_rows + 1):
        base_preset_rows_data[row] = {
            'columns': 4,
            'white_spaces': []
        }
    await empty_grid_conf.create_base_placement_db(db, base_preset_rows, base_preset_rows_data)
