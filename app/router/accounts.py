from fastapi import APIRouter, status, HTTPException
from db.queries.accounts import AccountsModal, Accounts

router = APIRouter(prefix="/accounts")


@router.get('/{user_id}')
async def get_accounts_by_user_id(user_id:int):
    return await Accounts().get_accouts_by_user_id(user_id=user_id)

@router.get('/')
async def get_accounts():
    return await Accounts().get_accounts()


@router.put('/')
async def update_account_row_value(response: dict):
    return await Accounts().update_value(row_id=int(response['row_id']), column=response['column'], value=response['value'])

@router.post('/', status_code=status.HTTP_201_CREATED)
async def insert_account(account: AccountsModal):
    row = await Accounts().insert_account(account=account) 
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"{id} content not found!")
    return row




