import functools
import json
import asyncio
import requests
from bson import ObjectId
from utility.utilities import time_w_timezone
from random import choice, randint
from string import ascii_letters, digits

PS_BASE_PLATFORM: str = 'basePlatform'
global LOGIN, PASS
LOGIN = 'Admin'
PASS = 'Admin12345@'
AUTH_BASE_URL = 'http://localhost:8080'

all_symbols: str = ascii_letters + digits
api_base_url: str = "http://localhost:8000"
platform_id: str = '66c863729e3e5c4ba1e38a53'


async def create_wheel(wheel_id: str, wheel_batch: str, wheel_diam: int, token: str) -> str:
    headers: dict = {
        'Content-type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
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
    resp = await loop.run_in_executor(
        None,
        functools.partial(requests.post, post_url, data=json.dumps(body_data), headers=headers)
    )
    print(f'Creating `wheel`\n{'-' * 30}')
    print('wheelResp', resp, '\n', resp.json(), '\n')
    print(f'{'-' * 30}\n')
    if resp.status_code != 200:
        return ''
    data = resp.json()
    return data['_id']


used_pis_ids: set[str] = set()
used_wheel_ids: set[str] = set()
used_batch_numbers: set[str] = set()


async def create_stack(_row: str, _col: str, original_pis_id: str,
                       placement_id: str, batch_num: str, token: str):
    headers: dict = {
        'Content-type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    created_wheels: list[str] = []
    new_wheel_diam: int = 1000
    for _ in range(randint(1, 6)):
        new_wheel_id: str = ''.join([choice(all_symbols) for _ in range(12)])
        while new_wheel_id in used_wheel_ids:
            new_wheel_id = ''.join([choice(all_symbols) for _ in range(12)])
        created_wheels.append(await create_wheel(new_wheel_id, batch_num, new_wheel_diam, token))
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
    print(f'Creating `wheelstack`:\n {'-' * 30}')
    print('requestData', wheelstack_data)
    resp = await loop.run_in_executor(
        None,
        functools.partial(requests.post, post_url, data=json.dumps(wheelstack_data), headers=headers)
    )
    print('stackResp', resp, '\n', resp.json(), '\n')
    print(f'{'^' * 30}\n')
    return resp


async def main(use_batch: str):
    rows: list[str] = ['A', 'B']
    cols: list[str] = ['1', '2', '3', '4']
    tokenResp = requests.post(
        f'{AUTH_BASE_URL}/users/login',
        data={
            'username': LOGIN,
            'password': PASS,
        },
        headers={
            'Content-type': 'application/x-www-form-urlencoded',
        }
    )
    if not tokenResp.ok:
        print('Incorrect credentials')
        return
    respData = tokenResp.json()
    token = respData['access_token']
    batch_number: str = use_batch
    tasks = []
    for row in rows:
        if not use_batch:
            batch_number = ''.join([choice(all_symbols) for _ in range(24)])
            while batch_number in used_batch_numbers:
                batch_number = ''.join([choice(all_symbols) for _ in range(24)])
        for col in cols:
            original_pis: str = ''.join([choice(all_symbols) for _ in range(24)])
            while original_pis in used_pis_ids:
                original_pis = ''.join([choice(all_symbols) for _ in range(24)])
            used_pis_ids.add(original_pis)
            task = create_stack(row, col, original_pis, platform_id, batch_number, token)
            tasks.append(task)

    await asyncio.gather(*tasks)


batch = ''
# batch = 'Zo2ovkUbNDNT2EGcBpBTp7El'
if __name__ == '__main__':
    asyncio.run(main(batch))
