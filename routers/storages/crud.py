from bson import ObjectId
from loguru import logger
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from utility.utilities import get_db_collection, time_w_timezone, log_db_record, log_db_error_record


async def db_storage_make_json_friendly(storage_data: dict) -> dict:
    storage_data['_id'] = str(storage_data['_id'])
    storage_data['createdAt'] = storage_data['createdAt'].isoformat()
    storage_data['lastChange'] = storage_data['lastChange'].isoformat()
    if 'elements' in storage_data:
        for batch in storage_data['elements']:
            for element_id in storage_data['elements'][batch]:
                storage_data['elements'][batch][element_id] = str(storage_data['elements'][batch][element_id])
    return storage_data


async def db_create_storage(
        storage_name: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(f'Attempt to create a `storage` document with name = {storage_name}' + db_info)
    collection = await get_db_collection(db, db_name, db_collection)
    creation_time = await time_w_timezone()
    storage_data = {
        'name': storage_name,
        'createdAt': creation_time,
        'lastChange': creation_time,
        'elements': [],
    }
    try:
        result = await collection.insert_one(storage_data)
        logger.info(f'Successfully created `storage` document with name = {storage_name}' + db_info)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(f'Error while creating `storage` document with name = {storage_name}' + db_info + error_extra)
        raise HTTPException(
            detail=f'Error while creating `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_storage_by_name(
        storage_name: str,
        include_data: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(f'Attempt to search a `storage` document with name = {storage_name}' + db_info)
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        'name': storage_name,
    }
    projection = {
        'name': True,
        'createdAt': True,
        'lastChange': True,
    }
    if include_data:
        projection['elements'] = True
    try:
        result = await collection.find_one(query, projection)
        if result is None:
            logger.info(f"Unsuccessful search for `storage` document with name = {storage_name}" + db_info)
        else:
            logger.info(f'Successful search for `storage` document with name = {storage_name}' + db_info)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while searching for `storage` document with name = {storage_name}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while searching for `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_storage_by_object_id(
        storage_object_id: ObjectId,
        include_data: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(f'Attempt to search a `storage` document with `objectId` = {storage_object_id}' + db_info)
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '_id': storage_object_id,
    }
    projection = {
        '_id': True,
        'name': True,
        'createdAt': True,
        'lastChange': True,
    }
    if include_data:
        projection['elements'] = True
    try:
        result = await collection.find_one(query, projection)
        if result is None:
            logger.info(f"Unsuccessful search for `storage` document with name = {storage_object_id}" + db_info)
        else:
            logger.info(f'Successful search for `storage` document with name = {storage_object_id}' + db_info)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while searching for `storage` document with `objectId` = {storage_object_id}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while searching for `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_storage_place_wheelstack(
        storage_object_id: ObjectId,
        storage_name: str,
        wheelstack_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
        record_change: bool = True
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to add a new `wheelstack` = {wheelstack_id} into'
        f' `storage` with `objectId` = {storage_object_id} ' + db_info
    )
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '$or': [
            {'_id': storage_object_id},
            {'name': storage_name},
        ]
    }
    update = {
        '$addToSet': {
            f'elements': wheelstack_id,
        }
    }
    if record_change:
        update['$set'] = {
            'lastChange': await time_w_timezone()
        }
    try:
        result = await collection.update_one(query, update, session=session)
        log_mes = f'add operation for `storage` document with id = {storage_object_id} | name = {storage_name}' + db_info
        if 0 == result.modified_count:
            logger.info(f'Unsuccessful {log_mes}')
        else:
            logger.info(f'Successful {log_mes}')
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while adding new element into `storage`'
            f' document with `objectId` = {storage_object_id}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while adding new element into `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_storage_get_placed_wheelstack(
        storage_object_id: ObjectId,
        storage_name: str,
        wheelstack_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '$or': [
            {'_id': storage_object_id},
            {'name': storage_name},
        ],
        f'elements': wheelstack_object_id,
    }
    try:
        result = await collection.find_one(query)
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while searching element in `storage`'
            f' document with `objectId` = {storage_object_id}' + error_extra
        )
        raise HTTPException(
            detail=f'Error while adding new element into `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_storage_delete_placed_wheelstack(
        storage_object_id: ObjectId,
        storage_name: str,
        wheelstack_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession,
        record_change: bool = True,
):
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '$or': [
            {'_id': storage_object_id},
            {'name': storage_name},
        ],
        'elements': wheelstack_object_id,
    }
    update = {
        '$pull': {
            'elements': wheelstack_object_id
        },
    }
    if record_change:
        update['$set'] = {
            'lastChange': await time_w_timezone()
        }
    try:
        result = await collection.update_one(query, update, session=session)
        return result
    except PyMongoError as error:
        raise HTTPException(
            detail=f'Error while deleting placed `wheelstack`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


async def db_get_all_storages(
        include_data: bool,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    db_info = await log_db_record(db_name, db_collection)
    collection = await get_db_collection(db, db_name, db_collection)
    logger.info(
        f'Attempt to get all `storage` documents' + db_info
    )
    query = {}
    projection = {
        '_id': True,
        'name': True,
        'createdAt': True,
        'lastChange': True,
    }
    if include_data:
        projection['elements'] = True
    try:
        result = await collection.find(query, projection).to_list(length=None)
        logger.info(
            f'Successfully gathered all `storage` documents' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while gathering all `storage` documents' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while gathering all `storage` documents',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_storage_by_element(
        element_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to find `storage` by element `ObjectId` => {element_object_id}'
    )
    query = {
        'elements': element_object_id,
    }
    try:
        result = await collection.find_one(query)
        logger.info(
            f'Successfully found `storage` with provided element inside'
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while searching for a `storage`'
        )
        raise HTTPException(
            detail='Error while searching data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def db_get_storage_name_id(
        storage_id: ObjectId,
        storage_name: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        include_data: bool = False,
        session: AsyncIOMotorClientSession = None,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to search a `storage` document with: id = {storage_id} | name = {storage_name}' + db_info
    )
    collection = await get_db_collection(db, db_name, db_collection)
    query = {
        '$or': [
            {'_id': storage_id},
            {'name': storage_name},
        ]
    }
    projection = {}
    if not include_data:
        projection = {
            'elements': 0
        }
    try:
        result = await collection.find_one(query, projection, session=session)
        if result is None:
            logger.info(
                f"Unsuccessful search for `storage` document with: id = {storage_id} | name = {storage_name}" + db_info
            )
        else:
            logger.info(
                f'Successful search for `storage` document with: id = {storage_id} | name = {storage_name}' + db_info
            )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while searching for `storage` document with: id = {storage_id} | name = {storage_name}' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while searching for `storage`',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    

async def db_get_storages_with_elements_data(
        storage_identifiers: list[dict],  # {'_id': ...}, {'name': ...}
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
        session: AsyncIOMotorClientSession = None,
):
    db_info = await log_db_record(db_name, db_collection)
    logger.info(
        f'Attempt to gather all data of `storage`' + db_info
    )
    collection = await get_db_collection(db, db_name, db_collection)
    aggregate_queries = []
    # no identifier == search all
    if storage_identifiers:
        # Main `storage` document filter
        _match = {
            "$match": {
                "$or": storage_identifiers
            }
        }
        aggregate_queries.append(_match)
    # Replace `elements` with corresponding `wheelStack` collection documents
    _lookup_wheelstacks = {
        "$lookup": {
            "from": "wheelStacks",
            "localField": "elements",
            "foreignField": "_id",
            "as": "elements"
        }
    }
    aggregate_queries.append(_lookup_wheelstacks)
    _lookup_wheels = {
        "$lookup": {
            "from": "wheels",
                "localField": "elements.wheels",
                "foreignField": "_id",
                "as": "allWheels",
                "pipeline": [
                    {
                    "$project": {
                        "sqlData": 0,
                        "transferData": 0
                    }
                    }
                ]
        }
    }
    aggregate_queries.append(_lookup_wheels)
    # Find `wheel`s data and replace their `_id`s in corresponding `wheelstack`s
    _add_field_wheels = {
        "$addFields": {
            "elements": {
                "$map": {
                "input": "$elements",
                "as": "element",
                "in": {
                    "$mergeObjects": [
                    "$$element",
                    {
                        "wheels": {
                        "$map": {
                            "input": "$$element.wheels",  # Iterate over the IDs in `element.wheels`
                            "as": "wheelId",
                            "in": {
                                "$arrayElemAt": [
                                    "$allWheels",  # Find the corresponding wheel document in `allWheels`
                                    { "$indexOfArray": ["$allWheels._id", "$$wheelId"] }
                                ]
                            }
                        }
                        }
                    }
                    ]
                }
                }
            }
        }
    }
    aggregate_queries.append(_add_field_wheels)
    try:
        result = await collection.aggregate(aggregate_queries, session=session).to_list(length=None)
        logger.info(
            f'Successfully gathered all elements data for `storage`s' + db_info
        )
        return result
    except PyMongoError as error:
        error_extra: str = await log_db_error_record(error)
        logger.error(
            f'Error while gathering all elements data for `storage`s' + db_info + error_extra
        )
        raise HTTPException(
            detail=f'Error while gathering `storage` data',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
