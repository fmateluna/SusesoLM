# app/main.py
from fastapi import FastAPI
from app.api.endpoints import router
from app.scheduler.cron_etl import run_cron_scheduler, start_scheduled_etl

app = FastAPI()

# Ejecutar el cron basado en el archivo JSON en la raíz
run_cron_scheduler(start_scheduled_etl)
# Incluir los endpoints
app.include_router(router)


