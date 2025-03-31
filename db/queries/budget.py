from pydantic import BaseModel
from db.database import database
from typing import Optional
import logging


class BudgetModel(BaseModel):
    id: Optional[int] = None
    month: int
    year: int
    user_id: int
    category_id: int
    subcategory_id: int
    activity: int
    available: int


class Budget():
    def __init__(self):
        self.database = database

    async def get_all_budgets(self) :
        query = """SELECT b.id as budgedt_id, s.id as subcategory_id, s.name as subcategory, c.name as category, b.user_id, b.month, b.year, b.assigned, b.activity, b.available  FROM subcategories as s
                    left join budget as b
                    ON s.id = b.subcategory_id
                    left join categories as c
                    on c.id = s.category_id"""
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            if rows is not None:
                data = [row for row in rows]
                return data
            return None
    async def get_first_last_date(self):
        max_query = """SELECT year, month 
                    FROM budget
                    ORDER BY year DESC, month DESC 
                    LIMIT 1; """
        min_query= """SELECT year, month 
                    FROM budget
                    ORDER BY year ASC, month ASC 
                    LIMIT 1; """
        async with self.database.pool.acquire() as conn:
            max_date =  await conn.fetchrow(max_query)
            min_date = await conn.fetchrow(min_query)
            if max_date and min_date:
                return {'max':{'year': max_date['year'], 'month':max_date['month']},
                    'min':{'year': min_date['year'], 'month':min_date['month']}} 
            else:
                return None
