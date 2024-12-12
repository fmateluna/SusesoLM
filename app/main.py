# app/main.py
from fastapi import FastAPI
from app.api.endpoints import router

app = FastAPI()

# Incluir los endpoints
app.include_router(router)
