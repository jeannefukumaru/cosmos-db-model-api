from typing import Optional
from pydantic import BaseModel

class Item(BaseModel):
    wine_id: int
    alcohol: float