from pydantic import BaseModel, Field
from typing import ClassVar, Optional
from fastapi import status as res_status


class WheelsStackStandardResponse(BaseModel):
    status: str = ''
    message: str = ''
    data: Optional[dict] = None

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

    def set_found_message(self, wheelstack_object_id: str):
        self.message = f'Wheelstack with objectId: {wheelstack_object_id} found.'

    def set_not_found_message(self, wheelstack_object_id: str):
        self.message = f'Wheelstack with ID: {wheelstack_object_id} not found.'

    def set_create_message(self, wheelstack_object_id: str):
        self.message = f'Wheelstack with ID: {wheelstack_object_id} has been created successfully.'

    def set_duplicate_message(self, wheelstack_object_id: str):
        self.message = f'Wheelstack with ID: {wheelstack_object_id} already exist in DB.'

    def set_pis_duplicate_message(self, original_pis_id: str):
        self.message = f'Wheelstack with pisId: {original_pis_id} already exist in DB.'

    def set_update_message(self, wheelstack_object_id: str):
        self.message = f'Wheelstack with ID: {wheelstack_object_id} has been updated successfully.'

    def set_up_to_date_message(self, wheelstack_object_id: str):
        self.message = f'Wheelstack with ID: {wheelstack_object_id} already up to date.'

    def set_delete_message(self, wheelstack_object_id: str):
        self.message = f'Wheelstack with ID: {wheelstack_object_id} has been deleted successfully.'
