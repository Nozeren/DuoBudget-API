from pydantic import BaseModel
from db.database import database
from typing import Optional
import logging


class AccountsTypeModal(BaseModel):
    id: Optional[int] = None
    name: str
    in_budget: bool



class AccountsType():
    def __init__(self):
        self.database = database

    async def get_all_accounts_type(self):
        query = "SELECT * FROM accounts_type"
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            if not rows:
                return 
            data = [AccountsTypeModal(id=row['id'], name=row['name'],in_budget=row['in_budget']) for row in rows]
            return data


