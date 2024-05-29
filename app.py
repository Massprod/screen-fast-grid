from fastapi import FastAPI
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_connection import get_mongo_db_client
from database.grid.grid_constructor import GridConstructor

load_dotenv('.env')

app = FastAPI()


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
