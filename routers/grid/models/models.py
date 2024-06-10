from bson import ObjectId
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Dict, Optional


class Column(BaseModel):
    wheelStack: Optional[str] = Field(None, description="Must be an objectId or null")
    whiteSpace: bool = Field(..., description="Mark of the whitespace on Grid, show or not <div>")


class Row(BaseModel):
    columnsOrder: list = Field(..., description="Column identifiers in correct order they should be used")
    columns: Dict[str, Column] = Field(..., description="Columns dictionary")


class GridModelResponse(BaseModel):
    preset: str = Field(..., description="Must be a string and is required")
    createdAt: datetime = Field(..., description="Must be a date and is required")
    lastChange: datetime = Field(..., description="Must be a date and is required")
    rowsOrder: list = Field(..., description="Row identifiers in correct order they should be used")
    rows: Dict[str, Row] = Field(..., description="Rows dictionary")

    class Config:
        json_schema_extra = {
            "example": {
                "preset": "pmkGrid",
                "createdAt": "2024-06-02T19:13:39.213Z",
                "lastChange": "2024-06-02T19:13:39.213Z",
                "rowsOrder": ["A", "B", "C", "D", "E", "F", "G", "H", "I"],
                "rows": {
                    "A": {
                        "columnsOrder": [
                            "1_W", "2_W", "3_W", "4_W", "5_W", "6_W", "7_W", "8_W", "9_W", "10_W", "11_W", "12_W",
                            "13_W", "14_W", "15_W", "16_W", "17_W", "18_W", "19_W", "20_W", "21_W", "22_W", "23_W",
                            "24_W", "25_W", "26_W", "27_W", "28_W", "29_W", "30_W", "31_W", "32", "33", "34", "35",
                            "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
                            "51", "52", "53", "54", "55", "56", "57", "58"
                        ],
                        "columns": {
                            "1": {"wheelStack": None, "whiteSpace": True},
                            "2": {"wheelStack": None, "whiteSpace": True},
                            "3": {"wheelStack": None, "whiteSpace": True},
                            "4": {"wheelStack": None, "whiteSpace": True},
                            "5": {"wheelStack": None, "whiteSpace": True},
                            "6": {"wheelStack": None, "whiteSpace": True},
                            "7": {"wheelStack": None, "whiteSpace": True},
                            "8": {"wheelStack": None, "whiteSpace": True},
                            "9": {"wheelStack": None, "whiteSpace": True},
                            "10": {"wheelStack": None, "whiteSpace": True},
                            "11": {"wheelStack": None, "whiteSpace": True},
                            "12": {"wheelStack": None, "whiteSpace": True},
                            "13": {"wheelStack": None, "whiteSpace": True},
                            "14": {"wheelStack": None, "whiteSpace": True},
                            "15": {"wheelStack": None, "whiteSpace": True},
                            "16": {"wheelStack": None, "whiteSpace": True},
                            "17": {"wheelStack": None, "whiteSpace": True},
                            "18": {"wheelStack": None, "whiteSpace": True},
                            "19": {"wheelStack": None, "whiteSpace": True},
                            "20": {"wheelStack": None, "whiteSpace": True},
                            "21": {"wheelStack": None, "whiteSpace": True},
                            "22": {"wheelStack": None, "whiteSpace": True},
                            "23": {"wheelStack": None, "whiteSpace": True},
                            "24": {"wheelStack": None, "whiteSpace": True},
                            "25": {"wheelStack": None, "whiteSpace": True},
                            "26": {"wheelStack": None, "whiteSpace": True},
                            "27": {"wheelStack": None, "whiteSpace": True},
                            "28": {"wheelStack": None, "whiteSpace": True},
                            "29": {"wheelStack": None, "whiteSpace": True},
                            "30": {"wheelStack": None, "whiteSpace": True},
                            "31": {"wheelStack": None, "whiteSpace": True},
                            "32": {"wheelStack": None, "whiteSpace": False},
                            "33": {"wheelStack": None, "whiteSpace": False},
                            "34": {"wheelStack": None, "whiteSpace": False},
                            "35": {"wheelStack": None, "whiteSpace": False},
                            "36": {"wheelStack": None, "whiteSpace": False},
                            "37": {"wheelStack": None, "whiteSpace": False},
                            "38": {"wheelStack": None, "whiteSpace": False},
                            "39": {"wheelStack": None, "whiteSpace": False},
                            "40": {"wheelStack": None, "whiteSpace": False},
                            "41": {"wheelStack": None, "whiteSpace": False},
                            "42": {"wheelStack": None, "whiteSpace": False},
                            "43": {"wheelStack": None, "whiteSpace": False},
                            "44": {"wheelStack": None, "whiteSpace": False},
                            "45": {"wheelStack": None, "whiteSpace": False},
                            "46": {"wheelStack": None, "whiteSpace": False},
                            "47": {"wheelStack": None, "whiteSpace": False},
                            "48": {"wheelStack": None, "whiteSpace": False},
                            "49": {"wheelStack": None, "whiteSpace": False},
                            "50": {"wheelStack": None, "whiteSpace": False},
                            "51": {"wheelStack": None, "whiteSpace": False},
                            "52": {"wheelStack": None, "whiteSpace": False},
                            "53": {"wheelStack": None, "whiteSpace": False},
                            "54": {"wheelStack": None, "whiteSpace": False},
                            "55": {"wheelStack": None, "whiteSpace": False},
                            "56": {"wheelStack": None, "whiteSpace": False},
                            "57": {"wheelStack": None, "whiteSpace": False},
                            "58": {"wheelStack": None, "whiteSpace": False}
                        }
                    }
                }
            }
        }
