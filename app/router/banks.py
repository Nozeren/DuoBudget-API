from fastapi import APIRouter
from db.queries.banks import Banks 

banks_router = APIRouter(prefix="/banks")

@banks_router.get('/')
async def get_all_banks():
    return await Banks().get_all_banks()

