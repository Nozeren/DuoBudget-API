from pydantic import BaseModel
from db.database import database
from typing import Optional
import logging


class AccountsTypeModal(BaseModel):
    id: Optional[int] = None
    name: str


class AccountsType():
    def __init__(self):
        self.database = database

    async def get_all_accounts_type(self):
        query = "SELECT * FROM accounts_type"
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            if not rows:
                return 
            data = [AccountsTypeModal(id=row['id'], name=row['name']) for row in rows]
            return data


