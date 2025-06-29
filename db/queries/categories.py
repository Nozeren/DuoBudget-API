from pydantic import BaseModel
from db.database import database 


class Categories():
    def __init__(self):
        self.database = database

    async def update_name(self, row_id: int, value):
        query = f"UPDATE categories SET name = '{value}' WHERE id = {row_id} RETURNING *"
        async with self.database.pool.acquire() as conn:
            return await conn.execute(query) 


