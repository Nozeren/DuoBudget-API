from pydantic import BaseModel
from db.database import database 
from typing import Optional
import logging


class BankModel(BaseModel):
    id: Optional[int] = None
    name: str

class Banks():
    def __init__(self):
        self.database = database
    
    async def get_all_banks(self) -> list[BankModel]:
        query = "SELECT * FROM banks"
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            data = [BankModel(id=row['id'], name=row['name']) for row in rows]
            return data 
