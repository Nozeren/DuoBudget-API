import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, status
from db.queries.temporary_transactions import TemporaryTransactions
import logging
router = APIRouter(prefix="/temporary-transactions")


@router.post('/')
async def upload_transactions(response: dict):
    # The goal here is to add dataset to DB
    data = list(json.loads(response['data']))
    columns = list(json.loads(response['columns']))
    for row in data:
        for index, column in enumerate(columns):
            if column == "posted_date":
                row[index] = datetime.strptime(row[index], "%d/%m/%Y")
            elif column == "user_id":
                row[index] = int(row[index])
            elif column == "bank_id":
                row[index] = int(row[index])
            elif column == "amount":
                row[index] = float(row[index])

    temporary_transactions = TemporaryTransactions()
    await temporary_transactions.insert_data(data=data, columns=columns)
    await temporary_transactions.remove_duplicate()
    await temporary_transactions.add_subcategory()


@router.get('/')
async def get_all_transactions():
    return await TemporaryTransactions().get_all_transactions()


@router.put('/')
async def update_row_value(response: dict):
    return await TemporaryTransactions().update_value(row_id=int(response['row_id']), column=response['column'], value=response['value'])


@router.delete('/{id}', status_code=status.HTTP_200_OK)
async def delete_row(id: int):
    row = await TemporaryTransactions().delete_row(id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"{id} content not found!")
    return row
