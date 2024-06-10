from pydantic import BaseModel
from typing import ClassVar
from fastapi import status as res_status


class WheelsStandardResponse(BaseModel):
    status: str = ""
    message: str = ""
    data: dict = None

    STATUS_MAP: ClassVar[dict[int, str]] = {
        res_status.HTTP_200_OK: "SUCCESS",
        res_status.HTTP_201_CREATED: "CREATED",
        res_status.HTTP_202_ACCEPTED: "ACCEPTED",
        res_status.HTTP_204_NO_CONTENT: "NO_CONTENT",
        res_status.HTTP_302_FOUND: "FOUND",
        res_status.HTTP_304_NOT_MODIFIED: "NOT_MODIFIED",
        res_status.HTTP_400_BAD_REQUEST: "VALIDATION_ERROR",
        res_status.HTTP_401_UNAUTHORIZED: "UNAUTHORIZED",
        res_status.HTTP_403_FORBIDDEN: "FORBIDDEN",
        res_status.HTTP_404_NOT_FOUND: "NOT_FOUND",
        res_status.HTTP_409_CONFLICT: "CONFLICT",
        res_status.HTTP_500_INTERNAL_SERVER_ERROR: "ERROR"
    }

    def set_status(self, status_code: int):
        self.status = self.STATUS_MAP.get(status_code, "UNKNOWN_STATUS")

    def set_found_message(self, wheel_id: str):
        self.message = f'Wheel with ID: {wheel_id} found.'

    def set_not_found_message(self, wheel_id: str):
        self.message = f'Wheel with ID: {wheel_id} not found.'

    def set_create_message(self, wheel_id: str):
        self.message = f'Wheel with ID: {wheel_id} has been created successfully.'

    def set_duplicate_message(self, wheel_id: str):
        self.message = f'Wheel with ID: {wheel_id} already exist in DB.'

    def set_update_message(self, wheel_id: str):
        self.message = f'Wheel with ID: {wheel_id} has been updated successfully.'

    def set_up_to_date_message(self, wheel_id: str):
        self.message = f'Wheel with ID: {wheel_id} already up to date.'

    def set_delete_message(self, wheel_id: str):
        self.message = f'Wheel with ID: {wheel_id} has been deleted successfully.'


find_response_examples = {
    res_status.HTTP_200_OK: {
        "description": "Wheel found",
        "content": {
            "application/json": {
                "example": {
                    "status": "SUCCESS",
                    "message": "Wheel with ID: 12345 found.",
                    "data": {
                        "_id": "6658697150ffd9be65c4364d",
                        "wheelId": "12345",
                        "batchNumber": "54321",
                        "wheelDiameter": 9999,
                        "receiptDate": "2024-05-30T11:56:16.209000",
                        "status": "laboratory",
                        "wheelStack": None
                    }
                }
            }
        }
    },
    res_status.HTTP_404_NOT_FOUND: {
        "description": "Wheel not found",
        "content": {
            "application/json": {
                "example": {
                    "detail": {
                        "status": "NOT_FOUND",
                        "message": "Wheel with ID: 12345 not found.",
                        "data": None
                    }
                }
            }
        }
    },
    res_status.HTTP_500_INTERNAL_SERVER_ERROR: {
        "description": "Database error",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Database search error"
                }
            }
        }
    }
}


update_response_examples = {
    res_status.HTTP_200_OK: {
        "description": "Wheel updated successfully",
        "content": {
            "application/json": {
                "example": {
                    "status": "SUCCESS",
                    "message": "Wheel with ID: 12345 has been updated successfully.",
                    "data": {
                        "wheelId": "12345",
                        "batchNumber": "54321",
                        "wheelDiameter": 9999,
                        "receiptDate": "2024-05-30T11:56:16.209000",
                        "status": "updated",
                        "wheelStack": None
                    }
                }
            }
        }
    },
    res_status.HTTP_404_NOT_FOUND: {
        "description": "Wheel not found",
        "content": {
            "application/json": {
                "example": {
                    "detail": {
                        "status": "NOT_FOUND",
                        "message": "Wheel with ID: 12345 not found.",
                        "data": None
                    }
                }
            }
        }
    },
    res_status.HTTP_304_NOT_MODIFIED: {
        "description": "Wheel not modified",
        "content": {
            "application/json": {
                "example": {
                    "status": "NOT_MODIFIED",
                    "message": "Wheel with ID: 12345 is already up to date.",
                    "data": None
                }
            }
        }
    },
    res_status.HTTP_500_INTERNAL_SERVER_ERROR: {
        "description": "Database error",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Database update error"
                }
            }
        }
    }
}


create_response_examples = {
    res_status.HTTP_201_CREATED: {
        "description": "Wheel created successfully",
        "content": {
            "application/json": {
                "example": {
                    "status": "SUCCESS",
                    "message": "Wheel with ID: W12345 has been created successfully.",
                    "data": {
                        "_id": "6658697150ffd9be65c4364d",
                        "wheelId": "W12345",
                        "batchNumber": "B54321",
                        "wheelDiameter": 650,
                        "receiptDate": "2024-05-30T11:56:16.209000+00:00",
                        "status": "laboratory",
                        "wheelStack": {
                            "wheelStackId": "WS123",
                            "wheelStackPosition": 2
                        }
                    }
                }
            }
        }
    },
    res_status.HTTP_302_FOUND: {
        "description": "Wheel already exists",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Wheel with provided `wheelId`=W12345, already exists",
                }
            }
        }
    },
    res_status.HTTP_500_INTERNAL_SERVER_ERROR: {
        "description": "Database error",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Database insertion error"
                }
            }
        }
    }
}
