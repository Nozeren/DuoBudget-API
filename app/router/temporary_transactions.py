import json
from datetime import datetime
from fastapi import APIRouter
from db.queries.temporary_transactions import TemporaryTransactions 
import logging
router = APIRouter(prefix="/temporary-transactions")


@router.post('/')
async def upload_transactions(response:dict):
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
    await temporary_transactions.insert_data(data=data,columns=columns)
    await temporary_transactions.remove_duplicate()
    await temporary_transactions.add_subcategory()
