from pydantic import BaseModel
from db.database import database 
from typing import Optional
import pandas as pd
import logging

INITIAL_DATA = {"House": [
    "Rent",
    "Water",
    "Electricity",
    "Internet",
    "SIM-Card",
    "Condominium",
],
    "Family": [
    "Groceries",
    "Healthcare",
    "Gym",
    "Education",
    "Presents",
    "Clothes",
    "Shared-expenses"
],
    "Transport": ["Tickets"],
    "Extra": [
    "Restaurants",
    "Fast-food",
    "Entertainment",
    "Repair",
    "Vacations",
    "Household-Items",
    "Transactions",
],
    "Income": [
    "Salary",
],
    "Savings":[
    "Savings"
    ],
    "Shared":["Bills"]
}


class SubcategoriesModel(BaseModel):
    id: Optional[int] = None
    name: str
    category_id: int
    user_id: int

class Subcategories():
    def __init__(self):
        self.database = database
    async def get_subcategory_id_by_name(self, subcategory_name:str): 
        query = f"""SELECT id FROM subcategories WHERE position('{subcategory_name}' in name)>0"""
        async with self.database.pool.acquire() as conn:
            return await conn.fetchrow(query)

    async def get_all_subcategories_categorized(self, user_id):
        query = f'''SELECT subcategories.id, categories.id as category_id, categories.name as category, subcategories.name as subcategory FROM subcategories
                    LEFT JOIN categories
                    ON category_id = categories.id
                    WHERE user_id = $1;''' 
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query,user_id)
            data = [row for row in rows]
            return data

    async def load_dataframe_to_database(self, df: pd.DataFrame) -> None:
        data = [tuple(row) for row in df.values]
        await self.database.pool.copy_records_to_table('subcategories', records=data, columns=list(df.columns))

    async def load_initial_data(self, user_id):
        data = []
        for category in INITIAL_DATA:
            query = f"""SELECT id FROM categories
                    WHERE name = $1"""
            async with self.database.pool.acquire() as conn:
                category_data = await conn.fetchrow(query, category)
                category_id = category_data["id"]
            for subcategory in INITIAL_DATA[category]:
                data.append((subcategory, category_id, user_id))
        dataframe = pd.DataFrame(data=data, columns=("name", "category_id", "user_id" ))
        await self.load_dataframe_to_database(df=dataframe)


    async def update_name(self, row_id: int, value):
        query = f"UPDATE subcategories SET name = '{value}' WHERE id = {row_id} RETURNING *"
        async with self.database.pool.acquire() as conn:
            return await conn.execute(query) 

    async def delete_subcategory(self, id: int):
        query_budget = "DELETE FROM budget WHERE subcategory_id = $1 RETURNING *"
        query = "DELETE FROM subcategories WHERE id = $1 RETURNING *"
        async with database.pool.acquire() as conn:
            await conn.execute(query_budget, id)
            await conn.execute(query, id)


    async def insert_subcategory(self, subcategory:SubcategoriesModel):
        query = "INSERT INTO subcategories (user_id, name, category_id ) VALUES ($1, $2, $3) RETURNING id"
        async with database.pool.acquire() as conn:
            result = await conn.fetchrow(query, subcategory.user_id, subcategory.name, subcategory.category_id)
        return result["id"]
