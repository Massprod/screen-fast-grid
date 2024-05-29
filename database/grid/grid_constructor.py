import os
import json
import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger
from database.mongo_connection import get_mongo_db_client
import asyncio
import dotenv


# dotenv.load_dotenv('../../.env')


class GridConstructor:

    def __init__(self,
                 rows: int,
                 rows_data: dict[int, dict[str, list[tuple[int, int]] | int]],
                 db_name: str = 'pmk_grid'):
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
            raise ValueError('`rows` cant be <= 0')
        self.rows: int = rows
        self.rows_data = rows_data
        self.created_rows: list[str] = []
        self.created_rows_columns: dict[str, list[str]] = {}
        self.default_schemas_path: str = 'grid_schemas'
        self.db_name: str = db_name

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
    def load_json_schema(file_path: str) -> str:
        with open(file_path, 'r') as file:
            return json.load(file)

    def set_grid(self) -> None:
        """
        Create rows with columns and specified columns as white spaces.
        Iterates through the rows, generates column identifiers, and marks specified columns as empty.
        """
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
                            row_columns[index] += 'W'
                            continue
                        break
            self.created_rows.append(row_identifier)
            self.created_rows_columns[row_identifier] = row_columns

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

    async def set_collections_schemas(self,
                                      db: AsyncIOMotorClient,
                                      folder_path: str = '',
                                      ) -> None:
        if not os.path.isdir(folder_path):
            folder_path = self.default_schemas_path
            if not os.path.isdir(folder_path):
                os.mkdir(folder_path)
        for filename in os.listdir(folder_path):
            if filename.endswith('.json'):
                collection_name: str = filename.split('.')[0]
                schema: str = self.load_json_schema(os.path.join(folder_path, filename))
                try:
                    await db[self.db_name].create_collection(collection_name, validator={'$jsonSchema': schema})
                except Exception as er:
                    continue

    async def initiate_empty_grid_db(self, db: AsyncIOMotorClient, collection_name: str = 'grid') -> None:
        whole_rec = {
            'preset': 'pmk_grid',
            'createdAt': datetime.datetime.now(),
            'lastChange': datetime.datetime.now(),
            'rows': {},
        }
        for row in self.created_rows:
            row_columns = self.created_rows_columns[row]
            rec = {row: {}}
            rec[row]['columns'] = {}
            for identifier in row_columns:
                rec[row]['columns'][identifier[0]] = {
                    'wheelStack': None,
                    'wheels': None,
                    'whiteSpace': False if len(identifier) > 1 and identifier[-1] != 'W' else True
                }
            whole_rec['rows'].update(rec)
        await db[self.db_name][collection_name].insert_one(whole_rec)


# test_rows: int = 6
# test_rows_data = {}
# for row in range(test_rows):
#     test_rows_data[row] = {
#         'white_spaces': [],
#         'columns': 5,
#     }
#
#
# async def test1(test_r, test_r_d):
#     db = await get_mongo_db_client()
#     test = GridConstructor(5, {})
#     test.set_pmk_preset()
#     test.set_grid()
#     await test.set_collections_schemas(db)
#     await test.initiate_grid_db(db)
#     await db['pmk_grid']['wheels'].insert_one(
#         {
#             'wheelId': '1234',
#             'batchNumber': '123',
#             'wheelDiameter': 900,
#             'receiptDate': datetime.datetime.now(),
#             'status': 'testStatus',
#         }
#     )
#     db.close()
# asyncio.run(test1(test_rows, test_rows_data))
