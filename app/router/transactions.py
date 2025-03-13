from fastapi import APIRouter, status, HTTPException
from db.queries.transactions import Transactions, TransactionsModel
import json
router = APIRouter(prefix="/transactions")


@router.get('/')
async def get_all_transactions():
    return await Transactions().get_all_transactions()


@router.put('/')
async def update_row_value(response: dict):
    return await Transactions().update_value(row_id=int(response['row_id']), column=response['column'], value=response['value'])


@router.post('/', status_code=status.HTTP_201_CREATED)
async def add_row(transaction: TransactionsModel) -> TransactionsModel:
    row = await Transactions().add_row(transaction)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"{id} content not found!")
    return row


@router.delete('/{id}', status_code=status.HTTP_200_OK)
async def delete_row(id: int):
    row = await Transactions().delete_row(id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"{id} content not found!")
    return row
