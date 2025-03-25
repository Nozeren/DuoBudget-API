from pydantic import BaseModel, StrictStr
from db.database import database
from typing import Optional, Union
from datetime import datetime


class TemporaryTransactionsModel(BaseModel):
    id: Optional[int] = None
    posted_date: Union[StrictStr, datetime]
    description: str
    user_id: int
    bank_id: int
    subcategory_id: Union[int, None]
    shared_amount: float
    amount: float


class TemporaryTransactions():
    def __init__(self):
        self.database = database

    async def insert_data(self, data: list, columns: list):
        async with database.pool.acquire() as conn:
            await conn.copy_records_to_table("temporary_transactions", records=data, columns=columns)

    async def remove_duplicate(self):
        query = """DELETE FROM temporary_transactions t
                    USING transactions tr
                    WHERE t.description = tr.description AND
                    t.posted_date = tr.posted_date AND
                    t.user_id = tr.user_id AND
                    t.bank_id = tr.bank_id AND
                    t.amount = tr.amount"""
        async with database.pool.acquire() as conn:
            await conn.execute(query)

    async def add_subcategory(self):
        # Add subcategory if description already in acitivity table
        query = f"""UPDATE temporary_transactions
                    SET subcategory_id = transactions.subcategory_id
                    FROM transactions 
                    WHERE temporary_transactions.description = transactions.description;"""
        async with database.pool.acquire() as conn:
            await conn.execute(query)

    async def get_all_transactions(self):
        query = f"""SELECT t.id, t.posted_date, t.description, t.user_id, u.name as user_name, subcategory_id, s.name as subcategory_name, shared_amount , bank_id, b.name as bank_name, amount from temporary_transactions t
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
            query = f"UPDATE temporary_transactions SET {column} = '{value}' WHERE id = {row_id} RETURNING *"
        elif isinstance(value, int):
            query = f"UPDATE temporary_transactions SET {column} = {value} WHERE id = {row_id} RETURNING *"
        async with self.database.pool.acquire() as conn:
            return await conn.execute(query)

    async def delete_row(self, id: int):
        query = "DELETE FROM temporary_transactions WHERE id = $1 RETURNING *;"
        async with database.pool.acquire() as conn:
            result = await conn.fetchrow(query, id)
            if result is not None:
                return TemporaryTransactionsModel(id=result['id'], posted_date=datetime.strftime(result['posted_date'], '%Y-%m-%d'), description=result['description'], user_id=result['user_id'], bank_id=result['bank_id'],
                                                  subcategory_id=result['subcategory_id'], shared_amount=result['shared_amount'],
                                                  amount=result['amount'])
            return None
