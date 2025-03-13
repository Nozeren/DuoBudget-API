from pydantic import BaseModel
from db.database import database 
from typing import Optional
import logging


class BankModel(BaseModel):
    id: Optional[int] = None
    name: str
    country: str

class Banks():
    def __init__(self):
        self.database = database
    
    async def get_all_banks(self) -> list[BankModel]:
        query = "SELECT * FROM banks"
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            data = [BankModel(id=row['id'], name=row['name'], country=row['country']) for row in rows]
            return data 

    async def insert_bank(self, bank:BankModel):
        query = "INSERT INTO banks (name, country) VALUES ($1, $2) RETURNING *"
        async with database.pool.acquire() as conn:
            await conn.execute(query, bank.name, bank.country )

    async def get_bank_by_id(self, id:int):
        query = "SELECT * FROM banks WHERE id = $1"
        async with self.database.pool.acquire() as conn:
            return await conn.fetchrow(query, id)