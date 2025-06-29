from pydantic import BaseModel
from db.database import database
from typing import Optional


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

    async def update_empty_subcategories(self):
        # Set activity to subcategories without transactions
        query = """
                            UPDATE budget
                            SET activity = transaction_query.activity
                            FROM (
                                SELECT 
                                    transactions.user_id,
                                    transactions.subcategory_id,
                                    EXTRACT(YEAR FROM posted_date) AS year,
                                    EXTRACT(MONTH FROM posted_date) AS month,
                                    COALESCE(SUM(amount), 0) AS activity
                                FROM transactions
                                JOIN subcategories ON subcategories.id = transactions.subcategory_id
                                JOIN categories ON categories.id = subcategories.category_id
                                JOIN categories_type ON categories.type_id = categories_type.id
                                JOIN accounts ON transactions.account_id = accounts.id
                                JOIN accounts_type ON accounts.type_id = accounts_type.id
                                WHERE categories_type.name IN ('debit', 'savings')
                                AND accounts_type.name IN ('debit')
                                GROUP BY 
                                    transactions.user_id,
                                    transactions.subcategory_id,
                                    EXTRACT(YEAR FROM posted_date),
                                    EXTRACT(MONTH FROM posted_date)
                                HAVING COALESCE(SUM(amount), 0) = 0
                            ) AS transaction_query
                            WHERE budget.subcategory_id = transaction_query.subcategory_id
                              AND budget.user_id = transaction_query.user_id
                              AND budget.month = transaction_query.month
                              AND budget.year = transaction_query.year
                            RETURNING *; 
        """
        # set activity to 0 if subcategory not found in transactions        
        query_two= """UPDATE budget
                        SET activity = 0
                            WHERE subcategory_id NOT IN (
                                SELECT DISTINCT subcategory_id
                                    FROM transactions
                                JOIN accounts ON transactions.account_id = accounts.id
                                JOIN accounts_type ON accounts.type_id = accounts_type.id
                            WHERE subcategory_id IS NOT NULL
                                AND accounts_type.name IN ('debit')
                                );"""
        async with self.database.pool.acquire() as conn:
            await conn.execute(query)
            await conn.execute(query_two)

    async def update_column_available_by_previous_month(self):
        query_previous_month = """ -- Update Available based on rows with available amount <> 0 during the previous month
                                UPDATE budget
                                SET available = CASE
                                                    WHEN budget.activity < 0 
                                                        THEN COALESCE(sb.available, 0) + budget.assigned - ABS(budget.activity)
                                                    ELSE
                                                        COALESCE(sb.available, 0) + budget.assigned + ABS(budget.activity)
                                                    END
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
                                    SET available = CASE
                                                        WHEN budget.activity < 0 
                                                            THEN budget.assigned - ABS(budget.activity)
                                                        ELSE 
                                                            budget.assigned + ABS(budget.activity)
                                                        END
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
            await conn.execute(query_previous_month)
            await conn.execute(query_without_previous_month)

    async def update_assigned(self, row_id: int , value):
        query = f"UPDATE budget SET assigned = {value} WHERE id = {row_id} RETURNING *"
        async with self.database.pool.acquire() as conn:
            return await conn.execute(query)



    async def update_debits(self):
        query = """
                    -- Update Activity based on the SUM of transactions amount
                            UPDATE budget
                            SET activity = transaction_query.activity
                            FROM (
                                SELECT 
                                    transactions.user_id,
                                    transactions.subcategory_id,
                                    EXTRACT(YEAR FROM posted_date) AS year,
                                    EXTRACT(MONTH FROM posted_date) AS month,
                                    COALESCE(SUM(amount), 0) AS activity
                                FROM transactions
                                JOIN subcategories ON subcategories.id = transactions.subcategory_id
                                JOIN categories ON categories.id = subcategories.category_id
                                JOIN categories_type ON categories.type_id = categories_type.id
                                JOIN accounts ON transactions.account_id = accounts.id
                                JOIN accounts_type ON accounts.type_id = accounts_type.id
                                WHERE categories_type.name IN ('debit', 'savings')
                                AND accounts_type.name IN ('debit')
                                GROUP BY 
                                    transactions.user_id,
                                    transactions.subcategory_id,
                                    EXTRACT(YEAR FROM posted_date),
                                    EXTRACT(MONTH FROM posted_date)
                            ) AS transaction_query
                            WHERE budget.subcategory_id = transaction_query.subcategory_id
                              AND budget.user_id = transaction_query.user_id
                              AND budget.month = transaction_query.month
                              AND budget.year = transaction_query.year
                            RETURNING *; 
                    """
        async with self.database.pool.acquire() as conn:
            await conn.execute(query)

    async def update_shared(self):
        query_other_user = """
                    -- Update Activity based on the SUM of transactions amount
                            UPDATE budget
                            SET assigned = ABS(transaction_query.activity)
                            FROM (
                                SELECT 
                                    transactions.user_id,
                                    EXTRACT(YEAR FROM posted_date) AS year,
                                    EXTRACT(MONTH FROM posted_date) AS month,
                                    SUM(shared_amount) AS activity
                                FROM transactions
                                GROUP BY 
                                    transactions.user_id,
                                    EXTRACT(YEAR FROM posted_date),
                                    EXTRACT(MONTH FROM posted_date)
                            ) AS transaction_query,
                            (
                                    SELECT * FROM budget
                                    JOIN subcategories ON subcategories.id = budget.subcategory_id
                                JOIN categories ON categories.id = subcategories.category_id
                                JOIN categories_type ON categories.type_id = categories_type.id
                                WHERE categories_type.name = 'shared'
                            ) AS b
                            WHERE b.subcategory_id = budget.subcategory_id
                              AND budget.user_id != transaction_query.user_id
                              AND budget.month = transaction_query.month
                              AND budget.year = transaction_query.year
                            RETURNING *; 
                    """
        query_subtract = """
                   UPDATE budget
                        SET assigned = CASE
                                            WHEN budget.assigned > t.activity
                                                THEN  budget.assigned - t.activity
                                            ELSE
                                                0
                                       END
                        FROM (
                                SELECT 
                                    transactions.user_id,
                                    EXTRACT(YEAR FROM posted_date) AS year,
                                    EXTRACT(MONTH FROM posted_date) AS month,
                                    ABS(SUM(shared_amount)) AS activity
                                FROM transactions
                                GROUP BY 
                                    transactions.user_id,
                                    EXTRACT(YEAR FROM posted_date),
                                    EXTRACT(MONTH FROM posted_date)
                        ) AS t,
                        (
                                SELECT * FROM budget
                                    JOIN subcategories ON subcategories.id = budget.subcategory_id
                                    JOIN categories ON categories.id = subcategories.category_id
                                    JOIN categories_type ON categories.type_id = categories_type.id
                                    WHERE categories_type.name = 'shared'
                        ) AS b
                        WHERE b.subcategory_id = budget.subcategory_id
                          AND budget.user_id = t.user_id
                          AND budget.month = t.month
                          AND budget.year = t.year
                        RETURNING *; 
        """
        async with self.database.pool.acquire() as conn:
            # Add shared amount from other users and set it as activity
            await conn.execute(query_other_user)
            # Subtract other users shared amount by user shared amount
            await conn.execute(query_subtract)



    async def update_budgets(self):
        query_transactions_date = """SELECT EXTRACT(YEAR FROM posted_date) AS year, 
                                       EXTRACT(MONTH FROM posted_date) AS month,
                                       user_id
                                       FROM transactions
                                       GROUP BY year, month, user_id
                                """
        async with self.database.pool.acquire() as conn:
            # Update
            await self.update_debits()
            await self.update_empty_subcategories()
            await self.update_shared()
            await self.update_column_available_by_previous_month()

    async def add_new_month(self, user_id: int, month: int, year: int):
        insert_query = """INSERT INTO budget(month, year, user_id, subcategory_id, assigned, activity, available)
                            SELECT $1, $2, $3, subcategories.id, 0, 0, 0 FROM subcategories
                            WHERE
                                NOT EXISTS (
                                    SELECT month, year, user_id, subcategory_id, assigned, activity, available FROM budget 
                                    where user_id = $3
                                    and subcategory_id = subcategories.id
                                    and month = $1
                                    and year = $2
                                        )
                            and user_id = $3;
                        """
        async with self.database.pool.acquire() as conn:
            await conn.execute(insert_query, month, year, user_id)

    async def get_all_budgets(self, user_id: int, month: int, year: int):
        query = """SELECT b.id as budget_id, s.id as subcategory_id, s.name as subcategory, c.name as category, c.id as category_id, b.user_id, b.month, b.year, b.assigned, b.activity, b.available  FROM subcategories as s
                    left join budget as b ON s.id = b.subcategory_id
                    left join categories as c on c.id = s.category_id
                    left join categories_type as ct on c.type_id = ct.id
                    WHERE b.user_id = $1
                    and month = $2
                    and year = $3
                    and ct.name NOT IN ('income')
                    ORDER BY s.id"""
        async with self.database.pool.acquire() as conn:
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

    async def get_total_assigned(self, user_id):
       query= '''
                 SELECT
                     (CASE
                     WHEN SUM(ASSIGNED) > SUM(ACTIVITY) THEN SUM(ASSIGNED) - SUM(ACTIVITY)
                     ELSE 0
                     END) as total
                     FROM
                     budget as b
                    left join subcategories as s ON s.id = b.subcategory_id
                    left join categories as c on c.id = s.category_id
                    left join categories_type as ct on c.type_id = ct.id
                     WHERE b.user_id = $1
                    and ct.name NOT IN ('income')
                     GROUP BY b.user_id
       '''
       async with self.database.pool.acquire() as conn:
            return await conn.fetchrow(query, user_id)
