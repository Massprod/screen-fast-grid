from motor.motor_asyncio import AsyncIOMotorClient
from os import getenv
from dotenv import load_dotenv
from bson import ObjectId


def item_helper(item) -> dict:
    for key in item:
        print(type(item[key]))
    return {
        "_id": str(item["_id"]),
        "name": item["name"],
        'description': item['description']
    }


async def add_item(item_data: dict, db) -> dict:
    db_collection = db['items']
    if db_collection is None:
        db_collection = await db.create_collection('items')
    item = await db_collection.insert_one(item_data)
    new_item = await db_collection.find_one({"_id": item.inserted_id})
    return item_helper(new_item)


async def retrieve_item(id: str, db) -> dict:
    db_collection = db['items']
    if db_collection is None:
        db_collection = await db.create_collection('items')
    item = await db_collection.find_one({"_id": ObjectId(id)})
    if item:
        return item_helper(item)
    return None
