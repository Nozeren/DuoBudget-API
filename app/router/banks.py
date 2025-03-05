from fastapi import APIRouter
from db.queries.banks import Banks, BankModel

banks_router = APIRouter(prefix="/banks")

@banks_router.get('/')
async def get_all_banks():
    return await Banks().get_all_banks()

@banks_router.get('/{id}')
async def get_bank(id:int):
    return await Banks().get_bank_by_id(id)


@banks_router.post('/')
async def insert_bank(bank: BankModel):
    return await Banks().insert_bank(bank)
