from pydantic import BaseModel, StrictStr
from db.database import database
from typing import Optional, Union
from datetime import datetime


class TransactionsModel(BaseModel):
    id: Optional[int] = None
    posted_date: Union[StrictStr, datetime]
    description: str
    user_id: int
    account_id: int
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

    async def get_all_transactions(self, user_id: int, month: int, year: int):
        query = f"""SELECT t.id, t.posted_date, t.description, t.user_id, u.name as user_name, subcategory_id, s.name as subcategory_name, shared_amount , t.account_id, ac.name as account_name, amount from transactions t
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
            query = f"UPDATE transactions SET {column} = '{value}' WHERE id = {row_id} RETURNING *"
        elif isinstance(value, int):
            query = f"UPDATE transactions SET {column} = {value} WHERE id = {row_id} RETURNING *"
        async with self.database.pool.acquire() as conn:
            return await conn.execute(query)

    async def add_row(self, transaction: TransactionsModel):
        query = f"""INSERT INTO transactions(posted_date, description, user_id, account_id, subcategory_id, shared_amount, amount)
                VALUES('{transaction.posted_date}',$1,$2,$3,$4,$5,$6) RETURNING *;"""
        async with self.database.pool.acquire() as conn:
            result = await conn.fetchrow(query, transaction.description,
                                         transaction.user_id, transaction.account_id, transaction.subcategory_id,
                                         transaction.shared_amount, transaction.amount)
            if result is not None:
                return TransactionsModel(id=result['id'], posted_date=datetime.strftime(result['posted_date'], '%Y-%m-%d'), description=result['description'], user_id=result['user_id'], account_id=result['account_id'],
                                         subcategory_id=result['subcategory_id'], shared_amount=result['shared_amount'],
                                         amount=result['amount'])
            return None

    async def delete_row(self, id: int):
        query = "DELETE FROM transactions WHERE id = $1 RETURNING *;"
        async with database.pool.acquire() as conn:
            result = await conn.fetchrow(query, id)
            if result is not None:
                return TransactionsModel(id=result['id'], posted_date=datetime.strftime(result['posted_date'], '%Y-%m-%d'), description=result['description'], user_id=result['user_id'], account_id=result['account_id'],
                                         subcategory_id=result['subcategory_id'], shared_amount=result['shared_amount'],
                                         amount=result['amount'])
            return None

    async def saveImportedData(self):
        query = """INSERT INTO transactions (posted_date, description, user_id, account_id, subcategory_id, shared_amount, amount)
                        SELECT posted_date, description, user_id, account_id, subcategory_id, shared_amount, amount 
                        FROM temporary_transactions
                        WHERE subcategory_id IS NOT NULL RETURNING *;
                    """
        delete_query = """DELETE FROM temporary_transactions
                        WHERE subcategory_id IS NOT NULL;"""
        async with database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            if rows is not None:
                await conn.execute(delete_query)
                return [TransactionsModel(id=result['id'], posted_date=datetime.strftime(result['posted_date'], '%Y-%m-%d'), description=result['description'], user_id=result['user_id'], account_id=result['account_id'],
                                          subcategory_id=result['subcategory_id'], shared_amount=result['shared_amount'],
                                          amount=result['amount'])for result in rows]
            return None

    async def get_first_last_date(self, user_id):
        max_query = """SELECT EXTRACT(YEAR FROM posted_date) AS year,
                              EXTRACT(MONTH FROM posted_date) AS month 
                    FROM transactions 
                    WHERE user_id = $1
                    ORDER BY year DESC, month DESC 
                    LIMIT 1; """
        min_query = """SELECT EXTRACT(YEAR FROM posted_date) AS year,
                              EXTRACT(MONTH FROM posted_date) AS month
                    FROM transactions 
                    WHERE user_id = $1
                    ORDER BY year ASC, month ASC 
                    LIMIT 1; """
        async with self.database.pool.acquire() as conn:
            max_date = await conn.fetchrow(max_query, user_id)
            min_date = await conn.fetchrow(min_query, user_id)
            if max_date and min_date:
                return {'max': {'year': max_date['year'], 'month': max_date['month']},
                        'min': {'year': min_date['year'], 'month': min_date['month']}}
            else:
                return None

