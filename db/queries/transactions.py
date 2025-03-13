from pydantic import BaseModel, StrictStr
from db.database import database
from typing import Optional, Union
from datetime import datetime


class TransactionsModel(BaseModel):
    id: Optional[int] = None
    posted_date: Union[StrictStr, datetime]
    description: str
    user_id: int
    bank_id: int
    subcategory_id: int
    shared_amount: float
    amount: float


class Transactions():
    def __init__(self):
        self.database = database

    async def get_subcategory_id_by_name(self, subcategory_name: str):
        query = f"""SELECT id FROM subcategories WHERE position('{subcategory_name}' in name)>0"""
        async with self.database.pool.acquire() as conn:
            return await conn.fetchrow(query)

    async def get_all_transactions(self):
        query = f"""SELECT t.id, t.posted_date, t.description, t.user_id, u.name as user_name, subcategory_id, s.name as subcategory_name, shared_amount , bank_id, b.name as bank_name, amount from transactions t
                        left join subcategories as s
                        on t.subcategory_id = s.id
                        left join banks as b
                        on t.bank_id = b.id
                        left join users as u
                        on t.user_id= u.id"""
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            rows = [row for row in rows]
            return rows

    async def update_value(self, row_id: int, column, value):
        if isinstance(value, str):
            query = f"UPDATE transactions SET {column} = '{value}' WHERE id = {row_id} RETURNING *"
        elif isinstance(value, int):
            query = f"UPDATE transactions SET {column} = {value} WHERE id = {row_id} RETURNING *"
        async with self.database.pool.acquire() as conn:
            return await conn.execute(query)

    async def add_row(self, transaction: TransactionsModel):
        query = f"""INSERT INTO transactions(posted_date, description, user_id, bank_id, subcategory_id, shared_amount, amount)
                VALUES('{transaction.posted_date}',$1,$2,$3,$4,$5,$6) RETURNING *;"""
        async with self.database.pool.acquire() as conn:
            result = await conn.fetchrow(query, transaction.description,
                                         transaction.user_id, transaction.bank_id, transaction.subcategory_id,
                                         transaction.shared_amount, transaction.amount)
            if result is not None:
                return TransactionsModel(id=result['id'], posted_date=datetime.strftime(result['posted_date'], '%Y-%m-%d'), description=result['description'], user_id=result['user_id'], bank_id=result['bank_id'],
                                         subcategory_id=result['subcategory_id'], shared_amount=result['shared_amount'],
                                         amount=result['amount'])
            return None

    async def delete_row(self, id: int):
        query = "DELETE FROM transactions WHERE id = $1 RETURNING *;"
        async with database.pool.acquire() as conn:
            result = await conn.fetchrow(query, id)
            if result is not None:
                return TransactionsModel(id=result['id'], posted_date=datetime.strftime(result['posted_date'], '%Y-%m-%d'), description=result['description'], user_id=result['user_id'], bank_id=result['bank_id'],
                                         subcategory_id=result['subcategory_id'], shared_amount=result['shared_amount'],
                                         amount=result['amount'])
            return None
