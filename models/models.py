from pydantic import BaseModel, Field
from typing import Optional
from bson import ObjectId


class MongoBaseModel(BaseModel):
    id: Optional[str] = Field(None, alias="_id", title="ID", description="The unique identifier of the item")

    class Config:
        json_encoders = {
            ObjectId: str
        }


class CreateItemRequest(BaseModel):
    name: str = Field(..., title="Name", description="Name of the item")
    description: Optional[str] = Field(None, title="Description", description="Description of the item (optional)")


class ItemResponse(MongoBaseModel):
    name: str = Field(..., title="Name", description="Name of the item")
    description: Optional[str] = Field(None, title="Description", description="Description of the item (optional)")
