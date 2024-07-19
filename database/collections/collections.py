import json
from loguru import logger
import os
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import CollectionInvalid
from constants import DB_PMK_NAME, FLD_BASIC_SCHEMAS


async def load_json_schema(file_path: str) -> dict:
    logger.debug(f'Loading JSON schema from {file_path}')
    with open(file_path, 'r') as file:
        return json.load(file)


async def create_basic_collections(db: AsyncIOMotorClient,
                                   folder_path: str = '',
                                   ) -> None:
    if not os.path.isdir(folder_path):
        folder_path = FLD_BASIC_SCHEMAS
    if not os.path.isdir(folder_path):
        logger.error(f'Schemas folder not found: {folder_path}')
        try:
            os.makedirs(folder_path)
            logger.info(f'Schemas folder created: {folder_path}')
        except OSError as e:
            logger.error(f'Error creating schemas folder: {folder_path} - {e}')
            raise FileNotFoundError(f'Error creating schemas folder: {folder_path} - {e}')
    logger.debug(f'Setting collections schemas from folder: {folder_path}')
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            collection_name: str = filename.split('.')[0]
            schema: dict = await load_json_schema(os.path.join(folder_path, filename))
            indexes: dict = {}
            if 'indexes' in schema:
                indexes: dict = schema.pop('indexes')
            try:
                await db[DB_PMK_NAME].create_collection(collection_name, validator={'$jsonSchema': schema})
                logger.info(f'Created collection {collection_name} in DB: {DB_PMK_NAME}')
            except CollectionInvalid as col_err:
                logger.warning(f'Collection {collection_name} already exists: {col_err}')
            except Exception as error:
                logger.error(f'Error creating collection {collection_name}: {error}')
                continue
            for index in indexes:
                keys = index['keys']
                options = index.get('options', {})
                try:
                    await db[DB_PMK_NAME][collection_name].create_index(list(keys.items()), **options)
                    logger.info(f'Created index on {keys} in collection: {collection_name}')
                except Exception as error:
                    logger.error(f'Error creating index on {keys} in collection: {collection_name}: {error}')
