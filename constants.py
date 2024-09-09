from os import getenv
from pathlib import Path

# Order options ENUM.
ORDER_MOVE_WHOLE_STACK: str = 'moveWholeStack'
ORDER_MOVE_TOP_WHEEL: str = 'moveTopWheel'
ORDER_MOVE_TO_LABORATORY: str = 'moveToLaboratory'
ORDER_MERGE_WHEEL_STACKS: str = 'mergeWheelstacks'
ORDER_MOVE_TO_PROCESSING: str = 'moveToProcessing'
ORDER_MOVE_TO_REJECTED: str = 'moveToRejected'
ORDER_MOVE_TO_STORAGE: str = 'moveToStorage'

# Order statuses ENUM.
ORDER_STATUS_COMPLETED: str = 'completed'
ORDER_STATUS_CANCELED: str = 'canceled'
ORDER_STATUS_PENDING: str = 'pending'

# BASIC EXTRA MOVES
BASIC_EXTRA_MOVES: set[str] = {
    ORDER_MOVE_TO_LABORATORY,
    ORDER_MOVE_TO_REJECTED,
    ORDER_MOVE_TO_PROCESSING,
}

# DB names
DB_PMK_NAME: str = 'pmkScreen'

# DB Collections
CLN_ACTIVE_ORDERS: str = 'activeOrders'
CLN_BASE_PLATFORM: str = 'basePlatform'
CLN_CANCELED_ORDERS: str = 'canceledOrders'
CLN_COMPLETED_ORDERS: str = 'completedOrders'
CLN_GRID: str = 'grid'
CLN_WHEELSTACKS: str = 'wheelStacks'
CLN_WHEELS: str = 'wheels'
CLN_PRESETS: str = 'presets'
CLN_BATCH_NUMBERS: str = 'batchNumbers'
CLN_STORAGES: str = 'storages'
# PRESETS
PRES_PMK_GRID: str = 'pmkGrid'
PRES_PMK_PLATFORM: str = 'pmkBasePlatform'

# PRESET TYPES
PRES_TYPE_GRID: str = 'grid'
PRES_TYPE_PLATFORM: str = 'basePlatform'

# EXTRA ELEMENT TYPES
EE_GRID_ROW_NAME: str = 'extra'
EE_HAND_CRANE: str = 'handCrane'
EE_LABORATORY: str = 'laboratory'

# CREATION_REJECT_MESSAGES
MSG_TESTS_NOT_DONE = "TESTS_NOT_DONE"
MSG_TESTS_FAILED = "TESTS_FAILED"

# FOLDERS
FLD_BASIC_SCHEMAS: str = 'database/collections/schemas'

# PLACEMENT STATUSES
PS_LABORATORY: str = 'laboratory'
PS_SHIPPED: str = 'shipped'
PS_GRID: str = 'grid'
PS_BASE_PLATFORM: str = 'basePlatform'
PS_REJECTED: str = 'rejected'
PS_STORAGE: str = 'storage'

# WHEELSTACKS LIMIT
WS_MIN_WHEELS: int = 0
WS_MAX_WHEELS: int = 6

# WHEELS LIMIT
WL_MIN_DIAM: int = 50
WL_MAX_DIAM: int = 10000

# JWT INFO
PUBLIC_KEY_PATH = Path('auth/public_key.pem')

with open(PUBLIC_KEY_PATH, 'r') as key_file:
    PUBLIC_KEY = key_file.read()

ALGORITHM = getenv('jwt_algorithm')
ISSUER = getenv('jwt_issuer')

ADMIN_ROLE = 'admin'
MANAGER_ROLE = 'manager'
LAB_PERSONAL_ROLE = 'labPersonal'


ADMIN_ACCESS_ROLES: set[str] = {
    ADMIN_ROLE
}
BASIC_PAGE_VIEW_ROLES: set[str] = {
    ADMIN_ROLE, MANAGER_ROLE
}
BASIC_PAGE_ACTION_ROLES: set[str] = {
    ADMIN_ROLE, MANAGER_ROLE
}
LAB_PAGE_VIEW_ROLES: set[str] = {
    ADMIN_ROLE, MANAGER_ROLE, LAB_PERSONAL_ROLE
}
LAB_PAGE_ACTION_ROLES: set[str] = {
    LAB_PERSONAL_ROLE
}
