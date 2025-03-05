from pydantic import BaseModel
from db.database import database 
from typing import Optional
import pandas as pd


class Transactions(BaseModel):
    id: Optional[int] = None
    name: str
    category_id: int

class Transactions():
    def __init__(self):
        self.database = database
    async def get_subcategory_id_by_name(self, subcategory_name:str): 
        query = f"""SELECT id FROM subcategories WHERE position('{subcategory_name}' in name)>0"""
        async with self.database.pool.acquire() as conn:
            return await conn.fetchrow(query)

