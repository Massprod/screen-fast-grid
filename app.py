from fastapi import FastAPI
from dotenv import load_dotenv
from routers.grid import router

load_dotenv('.env')

app = FastAPI()
app.include_router(router)



@app.on_event('startup')
async def create_basic_grid():
    pass
