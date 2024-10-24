import asyncio
from concurrent.futures import ThreadPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient
from constants import DB_PMK_NAME, CLN_WHEELSTACKS
from routers.wheelstacks.crud import db_find_wheelstack_by_object_id


async def placement_gather_wheelstacks(
        placement_state: dict, db: AsyncIOMotorClient
):
    wheelstacks_data: dict = {}
    rows = placement_state['rows']
    for row in rows:
        for col in rows[row]['columns']:
            cell_data = rows[row]['columns'][col]
            wheelstack_id = cell_data['wheelStack']
            if wheelstack_id is None:
                continue
            wheelstack_data = await db_find_wheelstack_by_object_id(
                wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
            )
            wheelstacks_data[str(wheelstack_data['_id'])] = wheelstack_data
    return wheelstacks_data


def convert_sync(data, key = '_id'):
    return {str(data[key]): data}


async def convert_and_store_threadpool(data_list, key='_id'):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        # Run the synchronous function using the ThreadPoolExecutor
        converted_data_list = await asyncio.gather(
            *[loop.run_in_executor(executor, convert_sync, data, key) for data in data_list]
        )

    converted_data = {}
    for item in converted_data_list:
        converted_data.update(item)

    return converted_data
