from pydantic import BaseModel
from db.database import database 
from typing import Optional
import logging


class SubcategoriesModel(BaseModel):
    id: Optional[int] = None
    name: str
    category_id: int

class Subcategories():
    def __init__(self):
        self.database = database
    async def get_subcategory_id_by_name(self, subcategory_name:str): 
        query = f"""SELECT id FROM subcategories WHERE position('{subcategory_name}' in name)>0"""
        async with self.database.pool.acquire() as conn:
            return await conn.fetchrow(query)

    async def get_all_subcategories_categorized(self):
        query = f'''SELECT subcategories.id, categories.name as category, subcategories.name as subcategory FROM subcategories
                    LEFT JOIN categories
                    ON category_id = categories.id;''' 
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            data = [row for row in rows]
            return data