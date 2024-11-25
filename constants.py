from os import getenv
from pathlib import Path
from loguru import logger
from fastapi import status


# Placement types
PT_GRID = 'grid'
PT_BASE_PLATFORM = 'basePlatform'
PT_STORAGE = 'storage'

# Order options ENUM.
ORDER_MOVE_WHOLE_STACK: str = 'moveWholeStack'
ORDER_MOVE_TOP_WHEEL: str = 'moveTopWheel'
ORDER_MOVE_TO_LABORATORY: str = 'moveToLaboratory'
ORDER_MERGE_WHEELSTACKS: str = 'mergeWheelStacks'
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
DB_PMK_NAME: str = getenv('API_MONGO_DB_NAME', 'pmkScreen')

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
CLN_PLACEMENT_HISTORY: str = 'placementHistory'
# PRESETS
PRES_PMK_GRID: str = 'pmkGrid'
PRES_PMK_PLATFORM: str = 'pmkBasePlatform'
# Placement collections
PLACEMENT_COLLECTIONS: dict[str, str] = {
    PT_GRID: CLN_GRID,
    PT_BASE_PLATFORM: CLN_BASE_PLATFORM,
    PT_STORAGE: CLN_STORAGES,
}

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

# WHEEL STATUSES 
WH_LABORATORY = 'laboratory'
WH_SHIPPED = 'shipped'
WH_GRID = 'grid'
WH_PLATFORM = 'basePlatform'
WH_REJECTED = 'rejected'
WH_STORAGE = 'storage'
WH_UNPLACED = 'unplaced'

# PLACEMENT STATUSES
PS_LABORATORY: str = 'laboratory'
PS_SHIPPED: str = 'shipped'
PS_GRID: str = 'grid'
PS_BASE_PLATFORM: str = 'basePlatform'
PS_REJECTED: str = 'rejected'
PS_STORAGE: str = 'storage'
PS_DECONSTRUCTED: str = 'deconstructed'
OUT_STATUSES: list[str] = [PS_LABORATORY, PS_SHIPPED, PS_REJECTED]

# WHEELSTACKS LIMIT
WS_MIN_WHEELS: int = 0
WS_MAX_WHEELS: int = 6

# WHEELS LIMIT
# WL_MIN_DIAM: int = 500
# WL_MAX_DIAM: int = 100_000

# JWT INFO
PUBLIC_KEY = None
PUBLIC_KEY_FILE_NAME = getenv('JWT_PUBLIC_KEY_NAME', 'public_key.pem')
PUBLIC_KEY_PATH = Path(f'auth/{PUBLIC_KEY_FILE_NAME}')
if PUBLIC_KEY_PATH.exists():
    try:
        with open(PUBLIC_KEY_PATH, 'r') as key_file:
            PUBLIC_KEY = key_file.read()
            logger.info(f"Public key successfully loaded from {PUBLIC_KEY_PATH}")
    except Exception as e:
        logger.error(f"Failed to read public key from {PUBLIC_KEY_PATH}: {e}")
else:
    logger.error(f"Public key file not found at {PUBLIC_KEY_PATH}")

ALGORITHM = getenv('jwt_algorithm')
ISSUER = getenv('jwt_issuer')

ADMIN_ROLE = 'admin'
MANAGER_ROLE = 'manager'
OPERATOR_ROLE = 'operator'
LAB_PERSONAL_ROLE = 'labPersonal'
CELERY_WORKER_ROLE = 'celeryWorker'


ADMIN_ACCESS_ROLES: set[str] = {
    ADMIN_ROLE
}
BASIC_PAGE_VIEW_ROLES: set[str] = {
    ADMIN_ROLE, MANAGER_ROLE, OPERATOR_ROLE
}
BASIC_PAGE_ACTION_ROLES: set[str] = {
    ADMIN_ROLE, MANAGER_ROLE
}
LAB_PAGE_VIEW_ROLES: set[str] = {
    ADMIN_ROLE, MANAGER_ROLE, LAB_PERSONAL_ROLE, OPERATOR_ROLE
}
LAB_PAGE_ACTION_ROLES: set[str] = {
    LAB_PERSONAL_ROLE
}
CELERY_ACTION_ROLES: set[str] = {
    CELERY_WORKER_ROLE
}

# WS CODES translate
WS_CODES: dict[int, int] = {
    status.HTTP_400_BAD_REQUEST: status.WS_1003_UNSUPPORTED_DATA,  # Bad Request -> Unsupported Data
    status.HTTP_401_UNAUTHORIZED: status.WS_1008_POLICY_VIOLATION,  # Unauthorized -> Policy Violation
    status.HTTP_403_FORBIDDEN: status.WS_1008_POLICY_VIOLATION,  # Forbidden -> Policy Violation
    status.HTTP_404_NOT_FOUND: status.WS_1007_INVALID_FRAME_PAYLOAD_DATA,  # Not Found -> Invalid Frame Payload Data
    status.HTTP_409_CONFLICT: status.WS_1008_POLICY_VIOLATION,  # Conflict -> Policy Violation
    status.HTTP_500_INTERNAL_SERVER_ERROR: status.WS_1011_INTERNAL_ERROR,  # Internal Server Error -> Internal Error
}

# preset cell types
PRESET_WHEELSTACK = 'wheelStack'
PRESET_WHITESPACE = 'whitespace'
PRESET_IDENTIFIER = 'identifier'
PRESET_IDENTIFIER_STRING = 'identifierString'
