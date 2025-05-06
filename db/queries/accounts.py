from pydantic import BaseModel
from db.database import database
from typing import Optional
import logging


class AccountsModal(BaseModel):
    id: Optional[int] = None
    name: str
    type_id: int
    bank_id: int 
    user_id: int 
    balance: Optional[float] = None



class Accounts():
    def __init__(self):
        self.database = database

    async def get_accouts_by_user_id(self, user_id: int):
        query = "SELECT * FROM accounts WHERE user_id = $1"
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query, user_id)
            if not rows:
                return 
            data = [AccountsModal(id=row['id'], name=row['name'],
                              type_id=row['type_id'], bank_id=row['bank_id'],
                              user_id=row['user_id'], balance=row['balance']) for row in rows]
            return data
    async def get_accounts(self):
        query = "SELECT accounts.id, accounts.name, type_id, user_id, users.name as user_name, bank_id, balance FROM accounts LEFT JOIN users ON users.id = accounts.user_id"
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            if not rows:
                return 
            data = [row for row in rows]
            return data



    async def update_value(self, row_id: int, column, value):
        if isinstance(value, str):
            query = f"UPDATE accounts SET {column} = '{value}' WHERE id = {row_id} RETURNING *"
        elif isinstance(value, int):
            query = f"UPDATE accounts SET {column} = {value} WHERE id = {row_id} RETURNING *"
        async with self.database.pool.acquire() as conn:
            return await conn.execute(query) 

    async def insert_account(self, account:AccountsModal):
        query = "INSERT INTO accounts (name, type_id, bank_id, user_id) VALUES ($1, $2, $3, $4) RETURNING *"
        async with database.pool.acquire() as conn:
            row = await conn.fetch(query, account.name, account.type_id, account.bank_id, account.user_id)
            if not row:
                return None
            return row
        