from os import getenv
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger


async def create_connection_string(login: str = '', password: str = '', server: str = '') -> str:
    """
    Creates a MongoDB connection string using provided credentials or environment variables.

    Args:
        login (str): MongoDB login username. Defaults to an empty string.
        password (str): MongoDB login password. Defaults to an empty string.
        server (str): MongoDB server address. Defaults to an empty string.

    Returns:
        str: The MongoDB connection string.

    Raises:
        ValueError: If any of the required credentials are missing.
    """
    try:
        login = login or getenv('login')
        password = password or getenv('password')
        server = server or getenv('server')
        if not all([login, password, server]):
            raise ValueError('Missing required database credentials')
        con_string = f'mongodb://{login}:{password}@{server}'
        return con_string
    except Exception as e:
        logger.info(f"Error creating connection string: {e}")
        raise


async def get_mongo_db_client(connection_string: str = '') -> AsyncIOMotorClient:
    """
    Connects to the specified MongoDB database using the provided connection string.

    Args:
        connection_string (str): The MongoDB connection string. Defaults to an empty string.

    Returns:
        AsyncIOMotorDatabase: The connected MongoDB database instance.

    Raises:
        Exception: If the connection to MongoDB fails.
    """
    try:
        if not connection_string:
            connection_string = await create_connection_string()
        db_client = AsyncIOMotorClient(connection_string)
        # Test the connection
        await db_client.admin.command('ping')
        return db_client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


async def get_db():
    test = await get_mongo_db_client()
    try:
        yield test
    finally:
        test.close()
