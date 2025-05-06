from fastapi import APIRouter
from db.queries.accounts_type import AccountsTypeModal, AccountsType 

router = APIRouter(prefix="/accounts-type")


@router.get('/')
async def get_all_accounts_type():
    return await AccountsType().get_all_accounts_type()



