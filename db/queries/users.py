from pydantic import BaseModel
from db.database import database 
from typing import Optional
import logging


class UserModel(BaseModel):
    id: Optional[int] = None
    name: str
    color: str  

class Users():
    def __init__(self):
        self.database = database
    
    async def get_all_users(self) -> list[UserModel]:
        query = "SELECT * FROM users"
        async with self.database.pool.acquire() as conn:
            rows = await conn.fetch(query)
            users = [UserModel(id=row['id'], name=row['name'], color=row['color']) for row in rows]
            return users
    
    async def insert_user(self, user:UserModel):
        query = "INSERT INTO users (name, color) VALUES ($1, $2)"
        async with database.pool.acquire() as conn:
            await conn.execute(query, user.name, user.color)