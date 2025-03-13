from fastapi import APIRouter
from db.queries.transactions import Transactions, TransactionsModel
import json
router = APIRouter(prefix="/transactions")

@router.get('/')
async def get_all_transactions():
    return await Transactions().get_all_transactions()

@router.put('/')
async def update_row_value(response:dict):
    return await Transactions().update_value(row_id=int(response['row_id']), column=response['column'], value=response['value'])

@router.post('/')
async def add_row(transaction:TransactionsModel):
    return await Transactions().add_row(transaction)

