from fastapi import FastAPI
from contextlib import asynccontextmanager
from db.database import database
from app.router.users import users_router 
from app.router.banks import banks_router 
import logging

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    yield
    await database.disconnect()

app = FastAPI(lifespan=lifespan)

app.include_router(users_router)
app.include_router(banks_router)


