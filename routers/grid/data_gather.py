from motor.motor_asyncio import AsyncIOMotorClient
from constants import DB_PMK_NAME, CLN_WHEELSTACKS
from routers.wheelstacks.crud import db_find_wheelstack_by_object_id


async def grid_gather_wheelstacks(
        grid_state: dict, db: AsyncIOMotorClient
):
    wheelstacks_data: dict = {}
    rows = grid_state['rows']
    for row in rows:
        for col in row['columns']:
            cell_data = rows[row]['columns'][col]
            wheelstack_id = cell_data['wheelStack']
            if wheelstack_id is None:
                continue
            wheelstack_data = await db_find_wheelstack_by_object_id(
                wheelstack_id, db, DB_PMK_NAME, CLN_WHEELSTACKS
            )
            wheelstacks_data[wheelstack_data['_id']] = wheelstack_data
    return wheelstacks_data
