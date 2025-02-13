import asyncpg
import logging
import dotenv
import os
# Environment Variables
dotenv.load_dotenv()
user = os.getenv("PSQL_USER")
password = os.getenv("PSQL_PASSWORD")
host = os.getenv("PSQL_HOST")
port = os.getenv("PSQL_PORT")
database = os.getenv("PSQL_DB")

DATABASE_URL = f"postgresql://{user}:{password}@{host}/{database}"

class Database: 
    def __init__(self):
       self.database_url = DATABASE_URL 
    
    async def connect(self):
        logging.info('Connecting to DB')
        self.pool = await asyncpg.create_pool(self.database_url)
        logging.info('Connected to DB')

    async def disconnect(self):
        logging.info('Disconecting from DB')
        self.pool.close()
        logging.info('Disconected from DB')

database = Database()
