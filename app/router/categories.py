from fastapi import APIRouter, status, HTTPException
from db.queries.categories import Categories

router = APIRouter(prefix="/category")



@router.put('/name')
async def update_name(response: dict):
    return await Categories().update_name(row_id=int(response['row_id']), value=response['value'])


