from os import getenv
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger
from dotenv import load_dotenv


def create_connection_string(login: str = '', password: str = '', server: str = '') -> str:
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


class MongoDBClient:

    def __init__(self):
        self.client: AsyncIOMotorClient | None = None

    def set_mongo_db_client(self, connection_string: str = ''):
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
                connection_string = create_connection_string()
            self.client = AsyncIOMotorClient(connection_string, maxPoolSize=20000)
            # Test the connection
            self.client.admin.command('ping')
            logger.info('MongoDB connection created.')
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def close_client(self):
        if self.client:
            self.client.close()
            logger.info("MongoDB client closed")

    def get_client(self):
        return self.client

    def depend_client(self):
        try:
            yield self.client
        finally:
            pass


load_dotenv('.env')


con_string = create_connection_string()
mongo_client = MongoDBClient()
mongo_client.set_mongo_db_client(con_string)
