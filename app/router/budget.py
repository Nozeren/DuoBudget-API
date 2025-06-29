from fastapi import APIRouter
from db.queries.budget import Budget, BudgetModel

router = APIRouter(prefix="/budget")


@router.post('/')
async def get_all_budget(data:dict):
    return await Budget().get_all_budgets(data['user_id'], data['month'], data['year'])

@router.get('/getfirstlastdate')
async def get_first_last_date():
    return await Budget().get_first_last_date()

@router.put('/assigned')
async def update_assigned(response: dict):
    return await Budget().update_assigned(row_id=int(response['row_id']), value=response['value'])

@router.get('/totalassigned/{user_id}')
async def get_total_assigned(user_id:int):
    return await Budget().get_total_assigned(user_id=user_id)


