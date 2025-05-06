from fastapi import APIRouter
from db.queries.budget import Budget, BudgetModel

router = APIRouter(prefix="/budget")


@router.post('/')
async def get_all_budget(data:dict):
    return await Budget().get_all_budgets(data['user_id'], data['month'], data['year'])

@router.get('/getfirstlastdate')
async def get_first_last_date():
    return await Budget().get_first_last_date()