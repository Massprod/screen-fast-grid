from bson import ObjectId
from loguru import logger
from fastapi import status
from fastapi.exceptions import HTTPException
from utility.utilities import get_db_collection, log_db_record
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError


async def preset_make_json_friendly(preset_data: dict) -> dict:
    preset_data['_id'] = str(preset_data['_id'])
    preset_data['createdAt'] = preset_data['createdAt'].isoformat()
    return preset_data


async def presets_make_json_friendly(presets_data: dict) -> dict:
    for preset in presets_data:
        preset['_id'] = str(preset['_id'])
        preset['createdAt'] = preset['createdAt'].isoformat()
    return presets_data


async def get_all_presets(
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    try:
        presets = await collection.find(
            {},
            {
                'rowsOrder': 0,
                'rows': 0,
                'extra': 0,
            }
        ).to_list(length=None)
        return presets
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_preset_by_id(
        preset_object_id: ObjectId,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_log_data: str = await log_db_record(db_name, db_collection)
    logger.info(
        f'Searching for preset with `objectId` = `{str(preset_object_id)}`' + db_log_data
    )
    try:
        preset = await collection.find_one({'_id': preset_object_id})
        if preset is None:
            logger.info(
                f'Preset with `objectId` = `{str(preset_object_id)}`, not Found' + db_log_data
            )
        else:
            logger.info(
                f'Preset with `objectId` = `{str(preset_object_id)}`, Found' + db_log_data
            )
        return preset
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def get_preset_by_name(
        preset_name: str,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str
):
    collection = await get_db_collection(db, db_name, db_collection)
    db_log_data: str = await log_db_record(db_name, db_collection)
    logger.info(
        f'Searching for preset with `presetName` = {preset_name}' + db_log_data
    )
    try:
        preset = await collection.find_one({'presetName': preset_name})
        if preset is None:
            logger.info(
                f'Preset with `presetName` = `{preset_name}`, not Found.' + db_log_data
            )
        else:
            logger.info(
                f'Preset with `presetName` = `{preset_name}`, Found.' + db_log_data
            )
        return preset
    except PyMongoError as error:
        logger.error(f'Error while searching in DB: {error}')
        raise HTTPException(
            detail=f'Error while searching in DB',
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


async def add_new_preset(
        preset_data: dict,
        db: AsyncIOMotorClient,
        db_name: str,
        db_collection: str,
) -> ObjectId:
    if 'presetName' not in preset_data:
        raise HTTPException(
            detail='`presetName` is not present in provided `preset_data`',
            status_code=status.HTTP_400_BAD_REQUEST
        )
    preset_name = preset_data['presetName']
    db_log_data: str = await log_db_record(db_name, db_collection)
    logger.info(
        f'Creating a new `preset` record in `{db_collection}` collection,'
        f' with `presetName` = `{preset_name}`' + db_log_data
    )
    exist = await get_preset_by_name(preset_name, db, db_name, db_collection)
    collection = await get_db_collection(db, db_name, db_collection)
    if exist is None:
        logger.info(f'Adding `{preset_name}` preset into collection: {db_collection}' + db_log_data)
        try:
            res = await collection.insert_one(preset_data)
            logger.info(
                f'Preset `{preset_name}` successfully added with `objectId` = {res.inserted_id}' + db_log_data
            )
            return res.inserted_id
        except PyMongoError as error:
            logger.error(f'Error while inserting preset `{preset_name}` = {error}' + db_log_data)
            raise HTTPException(
                detail=f'Error while inserting preset: {error}',
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
    else:
        logger.warning(f'Preset = `{preset_name}` already exist' + db_log_data)
        return exist['_id']
