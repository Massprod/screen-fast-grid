from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient


async def platform_make_json_friendly(dict_to_convert):
    dict_to_convert['_id'] = str(dict_to_convert['_id'])
    dict_to_convert['createdAt'] = dict_to_convert['createdAt'].isoformat()
    dict_to_convert['lastChange'] = dict_to_convert['lastChange'].isoformat()
    for row in dict_to_convert['rows']:
        for col in dict_to_convert['rows'][row]['columns']:
            dict_to_convert['rows'][row]['columns'][col]['wheelStack'] = str(dict_to_convert['rows'][row]['columns'][col]['wheelStack'])


async def get_platform(
        db: AsyncIOMotorClient,
        db_name: str = 'pmkScreen',
        db_collection: str = 'basePlacement',
):
    collection = db[db_name][db_collection]
    platform = await collection.find_one({'preset': 'pmkBase'})
    return platform


async def put_wheelstack_in_platform(
        db: AsyncIOMotorClient,
        row: str,
        column: str,
        wheelstack_object_id: ObjectId | None,
        db_name: str = 'pmkScreen',
        db_collection: str = 'basePlacement',
):
    collection = db[db_name][db_collection]
    query_string: str = f'rows.{row}.columns.{column}.wheelStack'
    document_filter = {query_string: {'$exists': True}}
    update_data = {'$set': {query_string: wheelstack_object_id}}
    result = await collection.update_one(document_filter, update_data)
    return result
