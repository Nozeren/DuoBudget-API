from fastapi import APIRouter
from db.queries.users import UserModel, Users

users_router = APIRouter(prefix="/users")

@users_router.get('/')
async def get_all_users()->list[UserModel]:
    return await Users().get_all_users()

@users_router.post('/')
async def insert_user(user: UserModel):
    return await Users().insert_user(user) 

@users_router.delete('/{id}')
async def delete_user(id:int):
    return await Users().delete_user(id) 
