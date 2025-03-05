from pydantic import BaseModel
from db.database import database 
from typing import Optional
import pandas as pd


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