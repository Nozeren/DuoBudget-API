from fastapi import APIRouter
from db.queries.subcategories import Subcategories, SubcategoriesModel

router = APIRouter(prefix="/subcategories")


@router.get('/{subcategory_name}')
async def get_subcategory_id_by_name(subcategory_name:str):
    row = await Subcategories().get_subcategory_id_by_name(subcategory_name=subcategory_name)
    return row

@router.get('/all/{user_id}')
async def get_all_subcategories_categorized(user_id:int):
   return await Subcategories().get_all_subcategories_categorized(user_id)


@router.put('/name')
async def update_name(response: dict):
    return await Subcategories().update_name(row_id=int(response['row_id']), value=response['value'])


@router.delete('/{id}')
async def delete_subcategory(id:int):
    return await Subcategories().delete_subcategory(id) 

@router.post('/')
async def insert_user(response: SubcategoriesModel):
    user_id = await Subcategories().insert_subcategory(subcategory=response) 


