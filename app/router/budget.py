from fastapi import APIRouter
from db.queries.budget import Budget, BudgetModel

router = APIRouter(prefix="/budget")


@router.get('/')
async def get_all_budget():
    return await Budget().get_all_budgets()

@router.get('/getfirstlastdate')
async def get_first_last_date():
    return await Budget().get_first_last_date()