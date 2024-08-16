from loguru import logger
from utility.utilities import time_w_timezone
from database.presets.cell_object import GridObject
from constants import PRES_PMK_GRID, PRES_TYPE_GRID, PRES_PMK_PLATFORM, PRES_TYPE_PLATFORM, EE_HAND_CRANE


# I was trying to create something universal, but it's actually going to
#  take more time and resources, and there's no actual reason for this.
# Because we're not using any other presets for now,
#  and all we care is creating a DB collection from which we can build a `grid` + `basePlatform`.
async def create_pmk_grid_preset() -> dict:
    preset: dict = {
        'presetName': PRES_PMK_GRID,
        'presetType': PRES_TYPE_GRID,
        'createdAt': await time_w_timezone(),
    }
    # 0 - row all column Identifiers
    # 0 - column all row Identifiers
    # Everything else is w.e we build.
    row_identifiers: list[str] = ['0', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']
    grid = []
    zero_row = [GridObject(whitespace=True) for _ in range(32)]
    zero_row += [GridObject(identifier=True, identifier_string=str(col)) for col in range(32, 59)]
    grid.append(zero_row)
    for row_id in range(1, 3):
        row = [GridObject(whitespace=True) for _ in range(31)]
        row += [GridObject(identifier=True, identifier_string=row_identifiers[row_id])]
        row += [GridObject(wheelstack=True) for _ in range(32, 59)]
        grid.append(row)
    row = [GridObject(whitespace=True)]
    row += [GridObject(identifier=True, identifier_string=str(col)) for col in range(1, 31)]
    row += [GridObject(identifier=True, identifier_string='31\\C')]
    row += [GridObject(wheelstack=True) for _ in range(32, 59)]
    grid.append(row)
    for row_id in range(4, len(row_identifiers)):
        row = [GridObject(identifier=True, identifier_string=row_identifiers[row_id])]
        row += [GridObject(wheelstack=True) for _ in range(1, 59)]
        grid.append(row)
    preset['rowsOrder'] = row_identifiers
    preset['rows'] = {}
    for row in range(len(row_identifiers)):
        preset['rows'][row_identifiers[row]] = {
            'columnsOrder': None,
            'columns': {},
        }
        preset['rows'][row_identifiers[row]]['columnsOrder'] = [str(index) for index in range(len(grid[row]))]
        cur_order = preset['rows'][row_identifiers[row]]['columnsOrder']
        cur_columns = preset['rows'][row_identifiers[row]]['columns']
        for col in cur_order:
            cur_object = grid[row][int(col)]
            cur_columns[col] = {
                'wheelStack': cur_object.wheelstack,
                'whitespace': cur_object.whitespace,
                'identifier': cur_object.identifier,
                'identifierString': cur_object.identifier_string,
            }
    # Extra elements.
    # Creating them as a separate elements
    preset['extra'] = {}
    for _ in range(1, 4):
        preset['extra'][f'vic{_}'] = {
            'type': EE_HAND_CRANE,
            'id': f'crane_vic{_}',
            'orders': {},
            'blocked': False,
        }
    preset['extra']['laboratory'] = {
        'type': 'laboratory',
        'id': 'laboratory',
        'orders': {},
        'blocked': False,
    }
    return preset


async def create_pmk_platform_preset() -> dict:
    preset: dict = {
        'presetName': PRES_PMK_PLATFORM,
        'presetType': PRES_TYPE_PLATFORM,
        'createdAt': await time_w_timezone(),
    }
    row_identifiers: list[str] = ['0', 'A', 'B']
    base_platform = []
    zero_row = [GridObject(whitespace=True)]
    zero_row += [GridObject(identifier=True, identifier_string=str(col)) for col in range(1, 5)]
    base_platform.append(zero_row)
    for row_id in range(1, len(row_identifiers)):
        row = [GridObject(identifier=True, identifier_string=row_identifiers[row_id])]
        row += [GridObject(wheelstack=True) for _ in range(1, 5)]
        base_platform.append(row)
    preset['rowsOrder'] = row_identifiers
    preset['rows'] = {}
    for row in range(len(row_identifiers)):
        preset['rows'][row_identifiers[row]] = {
            'columnsOrder': None,
            'columns': {},
        }
        preset['rows'][row_identifiers[row]]['columnsOrder'] = [str(index) for index in range(len(base_platform[row]))]
        cur_order = preset['rows'][row_identifiers[row]]['columnsOrder']
        cur_columns = preset['rows'][row_identifiers[row]]['columns']
        for col in cur_order:
            cur_object = base_platform[row][int(col)]
            cur_columns[col] = {
                'wheelStack': cur_object.wheelstack,
                'whitespace': cur_object.whitespace,
                'identifier': cur_object.identifier,
                'identifierString': cur_object.identifier_string,
            }
    # No extra elements for now.
    preset['extra'] = {}
    return preset
