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

    async def update_budgets(self):
        query_transactions_date = """SELECT EXTRACT(YEAR FROM posted_date) AS year, 
                                       EXTRACT(MONTH FROM posted_date) AS month,
                                       user_id
                                       FROM transactions
                                	   GROUP BY year, month, user_id
                                """
        query_activity = """
                            -- Update Activity based on the SUM of transactions amount
                            UPDATE budget
                            SET activity = transaction_query.activity
                            FROM (
                                SELECT subcategory_id, user_id,COALESCE(SUM(amount), 0) AS activity, EXTRACT(MONTH FROM posted_date) AS month, EXTRACT(YEAR FROM posted_date) AS year
                                FROM transactions
                                GROUP BY subcategory_id, posted_date, user_id
                            ) AS transaction_query
                            WHERE budget.subcategory_id = transaction_query.subcategory_id
                            AND budget.user_id = transaction_query.user_id
                            AND budget.month = transaction_query.month 
                            AND budget.year = transaction_query.year RETURNING *;"""
        # Update Subcategories without transactions
        query_none = """-- Update Activity based on Subcategories without transactions
                        UPDATE budget
                        SET activity = 0
                        FROM(
                        SELECT subcategories.id as id FROM subcategories
                        LEFT JOIN transactions
                        ON subcategories.id = transactions.subcategory_id
                        GROUP BY subcategories.id
                        HAVING COALESCE(SUM(amount), 0) = 0
                        ) AS s
                        WHERE budget.subcategory_id = s.id"""
        query_savings= """
                            -- Update Activity based on the SUM of only negative savings amount
                            UPDATE budget
                            SET activity = transaction_query.activity
                            FROM (
                                SELECT subcategory_id, user_id,COALESCE(SUM(amount), 0) AS activity, EXTRACT(MONTH FROM posted_date) AS month, EXTRACT(YEAR FROM posted_date) AS year
                                FROM transactions
                                LEFT JOIN subcategories as s
                                ON s.id = transactions.subcategory_id
                                LEFT JOIN categories as c
                                ON s.category_id = c.id
                                WHERE c.type = 'savings'
                                AND amount < 0
                                GROUP BY subcategory_id, posted_date, user_id, c.type, amount
                            ) AS transaction_query
                            WHERE budget.subcategory_id = transaction_query.subcategory_id
                            AND budget.user_id = transaction_query.user_id
                            AND budget.month = transaction_query.month 
                            AND budget.year = transaction_query.year RETURNING *;"""
        query_previous_month = """ -- Update Available based on rows with available amount <> 0 during the previous month
                                UPDATE budget
                                SET available = COALESCE(sb.available, 0) + budget.assigned - ABS(budget.activity)
                                FROM (
                                	SELECT subcategory_id, month, year, user_id, available FROM budget
                                ) as sb
                                WHERE budget.subcategory_id = sb.subcategory_id 
                                AND sb.user_id = budget.user_id
                                AND (
                                    (sb.month = budget.month - 1 AND sb.year = budget.year)  
                                    OR (budget.month = 1 AND sb.month = 12 AND sb.year = budget.year - 1)
                                    )
                               """
        query_without_previous_month = """-- Update Available based on rows with available amount = 0 during the previous month
                                    UPDATE budget
                                    SET available = budget.assigned - ABS(budget.activity)
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM budget sb
                                        WHERE budget.subcategory_id = sb.subcategory_id
                                        AND budget.user_id = sb.user_id
                                        AND (
                                            (sb.month = budget.month - 1 AND sb.year = budget.year) 
                                            OR (budget.month = 1 AND sb.month = 12 AND sb.year = budget.year - 1)
                                        )
                                    )
                                    ;"""
        async with self.database.pool.acquire() as conn:
            # Add new row to budget for transactions month
            dates = await conn.fetch(query_transactions_date)
            dates = [row for row in dates]
            for row in dates:
                if not await self.get_budget(user_id=row['user_id'], month=row['month'], year=row['year']):
                    await self.add_new_month(user_id=row['user_id'], month=row['month'], year=row['year'])
            # Update
            await conn.execute(query_activity)
            await conn.execute(query_none)
            await conn.execute(query_savings)
            await conn.execute(query_previous_month)
            await conn.execute(query_without_previous_month)

    async def add_new_month(self, user_id: int, month: int, year: int):
        insert_query = """INSERT INTO budget(month, year, user_id, subcategory_id, assigned, activity, available)
                                SELECT $1, $2, $3, subcategories.id, 0, 0, 0 FROM subcategories
                                LEFT JOIN categories
                                ON subcategories.category_id = categories.id
                                WHERE categories.type IN ('debit', 'savings');"""

        async with self.database.pool.acquire() as conn:
            await conn.execute(insert_query, month, year, user_id)

    async def get_all_budgets(self, user_id: int, month: int, year: int):
        query = """SELECT b.id as budgedt_id, s.id as subcategory_id, s.name as subcategory, c.name as category, b.user_id, b.month, b.year, b.assigned, b.activity, b.available  FROM subcategories as s
                    left join budget as b
                    ON s.id = b.subcategory_id
                    left join categories as c
                    on c.id = s.category_id
                    WHERE user_id = $1
                    and month = $2
                    and year = $3
                    and type  IN ('debit', 'savings')
                    ORDER BY s.id"""
        async with self.database.pool.acquire() as conn:
            await self.update_budgets()
            rows = await conn.fetch(query, user_id, month, year)
            if not rows:
                await self.add_new_month(user_id=user_id, month=month, year=year)
                await self.update_budgets()
                rows = await conn.fetch(query, user_id, month, year)
            data = [row for row in rows]
            return data

    async def get_budget(self, user_id: int, month: int, year: int):
        query = """SELECT * FROM budget
                    WHERE user_id = $1
                    AND month = $2
                    AND year = $3"""
        async with self.database.pool.acquire() as conn:
            return await conn.fetchrow(query, user_id, month, year)

    async def get_first_last_date(self):
        max_query = """SELECT year, month 
                    FROM budget
                    ORDER BY year DESC, month DESC 
                    LIMIT 1; """
        min_query = """SELECT year, month 
                    FROM budget
                    ORDER BY year ASC, month ASC 
                    LIMIT 1; """
        async with self.database.pool.acquire() as conn:
            max_date = await conn.fetchrow(max_query)
            min_date = await conn.fetchrow(min_query)
            if max_date and min_date:
                return {'max': {'year': max_date['year'], 'month': max_date['month']},
                        'min': {'year': min_date['year'], 'month': min_date['month']}}
            else:
                return None
