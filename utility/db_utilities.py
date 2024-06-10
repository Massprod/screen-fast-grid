from bson import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException, status
from datetime import datetime, timezone


async def get_preset(
        collection_name: str
):
    presets = {
        'basePlacement': 'pmkBase',
        'grid': 'pmkGrid',
    }
    return presets.get(collection_name, None)


async def get_object_id(
        object_id: str
):
    try:
        object_id = ObjectId(object_id)
        return object_id
    except InvalidId as e:
        status_code = status.HTTP_400_BAD_REQUEST
        raise HTTPException(detail=str(e), status_code=status_code)


async def time_w_timezone() -> datetime:
    return datetime.now(timezone.utc)
