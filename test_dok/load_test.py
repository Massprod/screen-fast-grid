import aiohttp
import asyncio
from string import ascii_letters, digits
from random import choices, choice, randint
from datetime import datetime, timezone


wheel_creation_url: str = 'http://127.0.0.1:8000/wheels/'
wheelstack_creation_url: str = 'http://127.0.0.1:8000/wheelstacks/'
wheelstack_place_grid_url: str = 'http://127.0.0.1:8000/grid/'


async def get_curtime():
    now = datetime.now(timezone.utc)
    iso = now.isoformat()
    return iso


class WheelStackGen:
    def __init__(self, id_length: int = 14, batch_wheel_limit: int = 20, max_wheelstack_size: int = 6):
        self.used_wheelstack_ids: dict[str, bool] = {'': True}
        self.used_wheel_ids: dict[str, bool] = {'': True}
        self.used_batches: dict[str, bool] = {'' : True}
        self.all_allowed: str = ascii_letters + digits
        self.base_length: int = id_length
        self.batch_used: int = 0
        self.batch_wheel_limit: int = batch_wheel_limit
        self.current_batch: str = ''
        self.wheel_diameters: list[int] = [num for num in range(100, 1001, 100)]
        self.max_wheelstack_size: int = max_wheelstack_size
        self.created_db_wheelstack_ids: list[str] = []

    async def create_batch(self, length: int) -> str:
        if not self.current_batch or self.batch_wheel_limit <= self.batch_used:
            while self.current_batch in self.used_batches:
                self.current_batch = ''.join(choices(self.all_allowed, k=length))
                if self.current_batch not in self.used_batches:
                    self.used_batches[self.current_batch] = True
                    self.batch_used = 0
                    break
        self.batch_used += 1
        return self.current_batch

    async def create_wheelstack_id(self, length: int) -> str:
        wheelstack_id: str = ''
        while wheelstack_id in self.used_wheelstack_ids:
            wheelstack_id = ''.join(choices(self.all_allowed, k=length))
            if wheelstack_id not in self.used_wheelstack_ids:
                self.used_wheelstack_ids[wheelstack_id] = True
                return wheelstack_id

    async def create_wheel_id(self, length: int) -> str:
        wheel_id: str = ''
        while wheel_id in self.used_wheel_ids:
            wheel_id = ''.join(choices(self.all_allowed, k=length))
            if wheel_id not in self.used_wheel_ids:
                self.used_wheel_ids[wheel_id] = True
                return wheel_id

    async def create_wheelstack(self, wheels: list[str], row_placement: str = '', col_placement: str = ''):
        wheelstack_data: dict[str, str | bool | int] = {
            'batchNumber': self.current_batch,
            'blocked': False,
            'colPlacement': col_placement,
            'maxSize': self.max_wheelstack_size,
            'originalPisId': await self.create_wheelstack_id(self.base_length),
            'placement': 'base',
            'status': 'inActive',
            'rowPlacement': row_placement,
            'wheels': wheels,
        }
        return wheelstack_data

    async def create_wheel(self) -> dict[str, str]:
        wheel_data: dict[str, str] = {
            'wheelId': await self.create_wheel_id(self.base_length),
            'batchNumber': await self.create_batch(self.base_length),
            'wheelDiameter': choice(self.wheel_diameters),
            'receiptDate': await get_curtime(),
            'status': 'grid',
        }
        return wheel_data


async def post(url: str, data: dict):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=data) as response:
                response.raise_for_status()
                response_data = await response.json()
                return response_data
        except aiohttp.ClientError as e:
            print(f"POST request failed: {e}")


async def put(url: str):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.put(url) as response:
                response.raise_for_status()
                return response
        except aiohttp.ClientError as e:
            print(f'PUT request failed: {e}')


async def imitate_wheels_wheelstack(generator):
    while True:
        wheels_left: int = generator.batch_wheel_limit - generator.batch_used
        await generator.create_batch(12)
        if not wheels_left:
            await asyncio.sleep(1)
            continue
        wheels: list[str] = []
        for _ in range(min(randint(1, wheels_left), generator.max_wheelstack_size)):
            wheel = await generator.create_wheel()
            await post(wheel_creation_url, wheel)
            wheels.append(wheel['wheelId'])
        wheelstack = await generator.create_wheelstack(wheels)
        # print(wheelstack)
        resp_data = await post(wheelstack_creation_url, wheelstack)
        print('Response\n', resp_data)
        print('------------------\n')
        generator.created_db_wheelstack_ids.append(resp_data['data']['_id'])
        await asyncio.sleep(1)


async def imitate_shuffle(generator, available_placements: dict[str, list[str]]):
    all_rows = list(available_placements.keys())
    while True:
        row = choice(all_rows)
        col = choice(available_placements[row])
        print('Created wheelstacks\n', generator.created_db_wheelstack_ids)
        print('------------------\n')
        if not generator.created_db_wheelstack_ids:
            await asyncio.sleep(1)
            continue
        object_id = choice(generator.created_db_wheelstack_ids)
        put_url: str = f'{wheelstack_place_grid_url}{object_id}?row={row}&column={col}'
        await put(put_url)
        await asyncio.sleep(1)


async def main():
    placements: dict[str, list[str]] = {}
    first_rows = ['A', 'B', 'C']
    for row in first_rows:
        placements[row] = [str(col) for col in range(32, 59)]
    other_rows = ['D', 'E', 'F', 'G', 'H', 'I']
    for row in other_rows:
        placements[row] = [str(col) for col in range(1, 59)]
    gen = WheelStackGen()
    task1 = asyncio.create_task(imitate_wheels_wheelstack(gen))
    task2 = asyncio.create_task(imitate_shuffle(gen, placements))
    await asyncio.gather(task1, task2)

asyncio.run(main())
