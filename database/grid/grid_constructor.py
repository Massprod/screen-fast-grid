import os
import json
import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger
from pymongo.errors import CollectionInvalid


class GridConstructor:

    def __init__(self,
                 rows: int = 1,
                 rows_data: dict[int, dict[str, list[tuple[int, int]] | int]] = None,
                 db_name: str = 'pmkScreen'):
        """
        Initializes the GridConstructor with the number of rows and the row data.

        Args:
            rows (int): The number of rows.
            rows_data (dict[int, dict[str, list[tuple[int, int]] | int]]):
                A dictionary where each key is a row number and each value is another dictionary
                containing "white_spaces" (list of tuples indicating start and end indices of empty spaces)
                and "columns" (int indicating the number of columns).
        """
        if rows <= 0:
            raise ValueError('`rows` cant be < 0')
        # Bad input, need's to be changed.
        # But, because now I'm just trying to get a first working version without anything else.
        # We're limited in time, and it's better
        #  to rush with macaroni than create a cool Classes without actually making it in time.
        # And actually, there's no task to make it Changeable.
        # It's like 1 time creation process.
        self.rows: int = rows
        self.rows_data: dict[int, dict[str, list[tuple[int, int]] | int]] = rows_data
        self.created_rows: list[str] = []
        self.created_rows_columns: dict[str, list[str]] = {}
        self.default_schemas_path: str = 'database/grid/grid_schemas'
        self.db_name: str = db_name
        self.default_db_preset: str = 'pmkGrid'

    @staticmethod
    def row_identifier(row_number: int) -> str:
        """
        Convert a positive integer to its corresponding alphabetic column name.
        Args:
            row_number (int): The row number to convert.

        Returns:
            str: The corresponding alphabetic column name.
        """
        result = []
        while row_number > 0:
            row_number, remainder = divmod(row_number - 1, 26)
            result.append(chr(65 + remainder))
        return ''.join(result[::-1])

    @staticmethod
    def load_json_schema(file_path: str) -> dict:
        logger.debug(f'Loading JSON schema from {file_path}')
        with open(file_path, 'r') as file:
            return json.load(file)

    def set_grid(self) -> None:
        """
        Create rows with columns and specified columns as white spaces.
        Iterates through the rows, generates column identifiers, and marks specified columns as empty.
        """
        logger.debug('Setting up the grid')
        for row in range(1, self.rows + 1):
            row_identifier: str = self.row_identifier(row)
            row_columns: list[str] = [str(col + 1) for col in range(self.rows_data[row]['columns'])]
            if self.rows_data and row in self.rows_data:
                white_spaces: list[tuple[int, int]] = self.rows_data[row]['white_spaces']
                for start, end in white_spaces:
                    if start < 0:
                        continue
                    for index in range(start, end + 1):
                        if index < len(row_columns):
                            row_columns[index] += '_W'
                            continue
                        break
            self.created_rows.append(row_identifier)
            self.created_rows_columns[row_identifier] = row_columns
        logger.info(f'Grid set with rows: {self.created_rows}')

    def set_pmk_preset(self) -> None:
        """
        Setting standard Grid state of PMK, for creation.
        """
        # All ranges inclusive.
        # 1 -> 31 == 6 rows
        # 32 -> 58 == 9 rows
        # 9 rows at max, first 30 cols should be empty for the first 3 rows.
        # 1 -> 3 rows == 1 -> 30 cols empty
        # 4 -> 9 rows == 1 -> 58 cols, 0 empty.
        logger.debug('Setting PMK preset')
        self.rows: int = 9
        # { row number: { 'white_spaces': [(start, end)], 'columns' } }
        self.rows_data: dict[int, dict[str, list[tuple[int, int]] | int]] = {}
        for row in range(1, 4):
            self.rows_data[row] = {
                'white_spaces': [(0, 30)],  # marking by Indexes, not column identifiers.
                'columns': 58,
            }
        for row in range(4, 10):
            self.rows_data[row] = {
                'white_spaces': [],
                'columns': 58,
            }
        logger.info('PMK preset settings are set')

    async def set_collections_schemas(self,
                                      db: AsyncIOMotorClient,
                                      folder_path: str = '',
                                      ) -> None:
        if not os.path.isdir(folder_path):
            folder_path = self.default_schemas_path
        if not os.path.isdir(folder_path):
            logger.error(f'Schema folder not found: {folder_path}')
            raise FileNotFoundError(f'Schema folder not found: {folder_path}')
        logger.debug(f'Setting collections schemas from folder: {folder_path}')
        for filename in os.listdir(folder_path):
            if filename.endswith('.json'):
                collection_name: str = filename.split('.')[0]
                schema: dict = self.load_json_schema(os.path.join(folder_path, filename))
                indexes: dict = {}
                if 'indexes' in schema:
                    indexes: dict = schema.pop('indexes')
                try:
                    await db[self.db_name].create_collection(collection_name, validator={'$jsonSchema': schema})
                    logger.info(f'Created collection {collection_name} in DB: {self.db_name}')
                except CollectionInvalid as col_err:
                    logger.warning(f'Collection {collection_name} already exists: {col_err}')
                except Exception as error:
                    logger.error(f'Error creating collection {collection_name}: {error}')
                    continue
                for index in indexes:
                    keys = index['keys']
                    options = index.get('options', {})
                    try:
                        await db[self.db_name][collection_name].create_index(list(keys.items()), **options)
                        logger.info(f'Created index on {keys} in collection: {collection_name}')
                    except Exception as error:
                        logger.error(f'Error creating index on {keys} in collection: {collection_name}: {error}')

    async def initiate_empty_grid_db(self, db: AsyncIOMotorClient, collection_name: str = 'grid') -> None:
        logger.debug(f'Initializing empty grid DB in collection: {collection_name}')
        whole_rec = {
            'preset': self.default_db_preset,
            'createdAt': datetime.datetime.now(),
            'lastChange': datetime.datetime.now(),
            'rowsOrder': self.created_rows,
            'rows': {},
        }
        for row in self.created_rows:
            row_columns = self.created_rows_columns[row]
            rec = {
                row: {
                    'columnsOrder': row_columns,
                    'columns': {},
                },
            }
            for identifier in row_columns:
                rec[row]['columns'][identifier.removesuffix('_W')] = {
                    'wheelStack': None,
                    'whiteSpace': True if (len(identifier) > 2 and identifier[-2:] == '_W') else False
                }
            whole_rec['rows'].update(rec)
        exist = await db[self.db_name][collection_name].find_one({'preset': self.default_db_preset})
        if exist is None:
            await db[self.db_name][collection_name].insert_one(whole_rec)
            logger.info(f'Initialized empty grid DB in collection: {collection_name}')

    async def create_base_placement_db(self, db: AsyncIOMotorClient, rows: int,
                                       rows_data: dict[int, dict[str, list[tuple[int, int]] | int]],
                                       collection_name: str = 'basePlacement',
                                       preset: str = 'pmkBase') -> None:
        logger.debug('Setting up the base placement')
        created_rows: list[str] = []
        created_rows_columns: dict[str, list[str]] = {}
        for row in range(1, rows + 1):
            row_columns: list[str] = [str(col + 1) for col in range(rows_data[row]['columns'])]
            if rows_data and row in rows_data:
                white_spaces: list[tuple[int, int]] = rows_data[row]['white_spaces']
                for start, end in white_spaces:
                    if start < 0:
                        continue
                    for index in range(start, end + 1):
                        if index < len(row_columns):
                            print(white_spaces)
                            row_columns[index] += '_W'
                            continue
                        break
            row_name: str = str(row)
            created_rows.append(row_name)
            created_rows_columns[row_name] = row_columns
        logger.info(f'Base placement set with rows: {created_rows}')

        logger.debug(f'Initializing empty grid DB in collection: {collection_name}')
        whole_rec = {
            'preset': preset,
            'createdAt': datetime.datetime.now(),
            'lastChange': datetime.datetime.now(),
            'rowsOrder': created_rows,
            'rows': {},
        }
        for row in created_rows:
            row_columns = created_rows_columns[row]
            rec = {
                row: {
                    'columnsOrder': row_columns,
                    'columns': {},
                },
            }
            for identifier in row_columns:
                rec[row]['columns'][identifier.removesuffix('_W')] = {
                    'wheelStack': None,
                    'whiteSpace': True if len(identifier) > 2 and identifier[-2:] == '_W' else False
                }
            whole_rec['rows'].update(rec)
        exist = await db[self.db_name][collection_name].find_one({'preset': preset})
        if exist is None:
            await db[self.db_name][collection_name].insert_one(whole_rec)
            logger.info(f'Initialized empty grid DB in collection: {collection_name}')
