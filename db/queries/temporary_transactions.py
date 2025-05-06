from pydantic import BaseModel, StrictStr
from db.database import database
from typing import Optional, Union
from datetime import datetime


class TemporaryTransactionsModel(BaseModel):
    id: Optional[int] = None
    posted_date: Union[StrictStr, datetime]
    description: str
    user_id: int
    account_id: int
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
                    t.account_id = tr.account_id AND
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

    async def get_all_transactions(self, user_id: int, month: int, year: int):
        query = f"""SELECT t.id, t.posted_date, t.description, t.user_id, u.name as user_name, subcategory_id, s.name as subcategory_name, shared_amount , t.account_id, ac.name as account_name, amount from temporary_transactions t
                        left join subcategories as s
                        on t.subcategory_id = s.id
                        left join accounts as ac
                        on t.account_id = ac.id
                        left join users as u
                        on t.user_id= u.id
                        WHERE t.user_id = $1 
                        AND EXTRACT(YEAR FROM posted_date) = $2
                        AND EXTRACT(MONTH FROM posted_date) = $3 
                        ORDER BY t.posted_date DESC"""
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query, user_id, year, month)
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
                return TemporaryTransactionsModel(id=result['id'], posted_date=datetime.strftime(result['posted_date'], '%Y-%m-%d'), description=result['description'], user_id=result['user_id'], account_id=result['account_id'],
                                                  subcategory_id=result['subcategory_id'], shared_amount=result['shared_amount'],
                                                  amount=result['amount'])
            return None

    async def get_first_last_date(self,user_id):
        max_query = """SELECT EXTRACT(YEAR FROM posted_date) AS year,
                              EXTRACT(MONTH FROM posted_date) AS month 
                    FROM temporary_transactions 
                    WHERE user_id = $1
                    ORDER BY year DESC, month DESC 
                    LIMIT 1; """
        min_query = """SELECT EXTRACT(YEAR FROM posted_date) AS year,
                              EXTRACT(MONTH FROM posted_date) AS month
                    FROM temporary_transactions 
                    WHERE user_id = $1
                    ORDER BY year ASC, month ASC 
                    LIMIT 1; """
        async with self.database.pool.acquire() as conn:
            max_date = await conn.fetchrow(max_query,user_id)
            min_date = await conn.fetchrow(min_query,user_id)
            if max_date and min_date:
                return {'max': {'year': max_date['year'], 'month': max_date['month']},
                        'min': {'year': min_date['year'], 'month': min_date['month']}}
            else:
                return None

