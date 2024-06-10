from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId


async def grid_make_json_friendly(grid_data: dict) -> dict:
    grid_data['_id'] = str(grid_data['_id'])
    grid_data['createdAt'] = grid_data['createdAt'].isoformat()
    grid_data['lastChange'] = grid_data['lastChange'].isoformat()
    for row in grid_data['rows']:
        for column in grid_data['rows'][row]['columns']:
            if grid_data['rows'][row]['columns'][column]['wheelStack'] is None:
                continue
            grid_data['rows'][row]['columns'][column]['wheelStack'] = str(grid_data['rows'][row]['columns'][column]['wheelStack'])
    return grid_data


async def get_grid(
        db: AsyncIOMotorClient,
        db_name: str = 'pmkScreen',
        db_collection: str = 'grid',
):
    collection = db[db_name][db_collection]
    grid = await collection.find_one({'preset': 'pmkGrid'})
    return grid


async def put_wheelstack_in_grid(
        db: AsyncIOMotorClient,
        row: str,
        column: str,
        wheelstack_object_id: ObjectId,
        db_name: str = 'pmkScreen',
        db_collection: str = 'grid',
):
    collection = db[db_name][db_collection]
    query_string: str = f'rows.{row}.columns.{column}.wheelStack'
    document_filter = {query_string: {'$exists': True}}
    update_data = {'$set': {query_string: wheelstack_object_id}}
    result = await collection.update_one(document_filter, update_data)
    return result
