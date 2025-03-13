from fastapi import APIRouter
from db.queries.subcategories import Subcategories, SubcategoriesModel

router = APIRouter(prefix="/subcategories")


@router.get('/{subcategory_name}')
async def get_subcategory_id_by_name(subcategory_name:str):
    row = await Subcategories().get_subcategory_id_by_name(subcategory_name=subcategory_name)
    return row

@router.get('/')
async def get_all_subcategories_categorized():
   return await Subcategories().get_all_subcategories_categorized()



