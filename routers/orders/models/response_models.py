from pydantic import BaseModel
from typing import Optional


class OrderStandardResponse(BaseModel):
    status: int = ""
    message: str = ""
    data: Optional[dict] = None

    def set_status(self, status_code: int):
        self.status = status_code

    def set_create_message(self, order_id: str):
        self.message = f"Order created successfully with ID {order_id}"

    def set_get_all_message(self):
        self.message = 'All active orders presented in DB'
