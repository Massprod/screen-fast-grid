import json
import asyncio
import requests
from bson import ObjectId
from utility.utilities import time_w_timezone
from constants import PS_BASE_PLATFORM
from random import choice, randint
from string import ascii_letters, digits

all_symbols: str = ascii_letters + digits
api_base_url: str = "http://localhost:8000"
platform_id: str = '66bed1cf28eac1d5922ba238'


async def create_wheel(wheel_id: str, wheel_batch: str, wheel_diam: int) -> str:
    receipt_date: str = (await time_w_timezone()).isoformat()
    status: str = PS_BASE_PLATFORM
    post_url: str = api_base_url + '/wheels/'
    body_data: dict = {
        'wheelId': wheel_id,
        'batchNumber': wheel_batch,
        'wheelDiameter': wheel_diam,
        'receiptDate': receipt_date,
        'status': status,
    }
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, requests.post, post_url, json.dumps(body_data))
    print('wheelResp', resp, '\n', resp.json(), '\n')
    if resp.status_code != 200:
        return ''
    data = resp.json()
    return data['_id']


used_pis_ids: set[str] = set()
used_wheel_ids: set[str] = set()
used_batch_numbers: set[str] = set()


async def create_stack(_row: str, _col: str, original_pis_id: str, placement_id: str, batch_num: str):
    created_wheels: list[str] = []
    new_wheel_diam: int = 1000
    for _ in range(randint(1, 6)):
        new_wheel_id: str = ''.join([choice(all_symbols) for _ in range(12)])
        while new_wheel_id in used_wheel_ids:
            new_wheel_id = ''.join([choice(all_symbols) for _ in range(12)])
        created_wheels.append(await create_wheel(new_wheel_id, batch_num, new_wheel_diam))
    wheelstack_data = {
        'batchNumber': batch_num,
        'blocked': False,
        'rowPlacement': _row,
        'colPlacement': _col,
        'maxSize': 6,
        'originalPisId': original_pis_id,
        'placementId': placement_id,
        'placementType': PS_BASE_PLATFORM,
        'status': PS_BASE_PLATFORM,
        'wheels': created_wheels,
    }
    post_url: str = api_base_url + '/wheelstacks/'
    loop = asyncio.get_event_loop()
    print('requestData', wheelstack_data)
    resp = await loop.run_in_executor(None, requests.post, post_url, json.dumps(wheelstack_data))
    print('stackResp', resp, '\n', resp.json(), '\n')
    return resp


async def main():
    rows: list[str] = ['A', 'B']
    cols: list[str] = ['1', '2', '3', '4']

    # batch_number: str = 'MWkITB4ylxEa4Ez2gDy4JWVd'
    tasks = []
    for row in rows:
        batch_number = ''.join([choice(all_symbols) for _ in range(24)])
        while batch_number in used_batch_numbers:
            batch_number = ''.join([choice(all_symbols) for _ in range(24)])
        for col in cols:
            original_pis: str = ''.join([choice(all_symbols) for _ in range(24)])
            while original_pis in used_pis_ids:
                original_pis = ''.join([choice(all_symbols) for _ in range(24)])
            used_pis_ids.add(original_pis)
            task = create_stack(row, col, original_pis, platform_id, batch_number)
            tasks.append(task)

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
