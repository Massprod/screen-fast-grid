from os import getenv
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger
from dotenv import load_dotenv


def create_connection_string(
        login: str = '',
        password: str = '',
        server: str = '',
        server_port: str = '',
        replica_name: str = '',
        database_name: str = '',
        auth_database: str = '',
        direct_connection: bool = True,
) -> str:
    """
    Creates a MongoDB connection string using provided credentials or environment variables.

    Args:
        login (str): MongoDB login username. Defaults to an empty string.
        password (str): MongoDB login password. Defaults to an empty string.
        server (str): MongoDB server address. Defaults to an empty string.
        server_port (str): MongoDB server port to use. Defaults to an empty string.
        replica_name (str): MongoDB replica name. Default to an empty string.
        database_name (str): MongoDb database name. Default to an empty string.
        auth_database (str): MongoDB database used for authentication. Default to an empty string.
        direct_connection: (bool): Type of connection to use. Default to `true`.
    Returns:
        str: The MongoDB connection string.

    Raises:
        ValueError: If any of the required credentials are missing.
    """
    try:
        login = login or getenv('API_MONGO_LOGIN')
        password = password or getenv('API_MONGO_PWD')
        server = server or getenv('MONGO_SERVER')
        server_port = server_port or getenv('MONGO_SERVER_INSIDE_PORT')
        replica_name = replica_name or getenv('MONGO_REPLICA_NAME')
        if not all([login, password, server, replica_name]):
            raise ValueError('Missing required database credentials')
        database_name = database_name or getenv('API_MONGO_DB_NAME')
        auth_database = auth_database or getenv('API_MONGO_AUTH_DATABASE')
        con_string = f'mongodb://{login}:{password}@{server}:{server_port}/{database_name}'
        options = []
        if auth_database:
            options.append(f'authSource={auth_database}')
        if replica_name:
            options.append(f'replicaSet={replica_name}')
        if direct_connection:
            options.append(f'directConnection={str(direct_connection).lower()}')
        if options:
            con_string += '?' + '&'.join(options)
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
