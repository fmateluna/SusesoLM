import json
import time
import threading
from datetime import datetime, timedelta
from croniter import croniter
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
from app.core.etl_services import ETLService
from app.core.ports.adapters import InMemoryTaskRepository
from app.core.ports.etl import TaskRepository
from app.models.request_models import ETLRequest

import logging

# Configurar logging


BASE_DIR = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = BASE_DIR / "cron_etl_config.json"


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename="cron_etl.log",  
    filemode='a' 
)

from datetime import datetime, timedelta
from croniter import croniter
import calendar

def obtener_rango_desde_cron(cron_expr: str, fecha_base: datetime = None):
    if fecha_base is None:
        fecha_base = datetime.now()

    fecha_base = fecha_base.replace(hour=0, minute=0, second=0, microsecond=0)

    # Obtener día de la semana del cron
    cron_dia_semana = int(cron_expr.split()[-1])  # Calculo rango semanal segun dia
    nombre_dia_cron = calendar.day_name[cron_dia_semana]  # Muestro el dia, para cuando cambien el json

    # Si hoy es el mismo día del cron, usar hoy como "fecha_actual" !!
    if fecha_base.weekday() == cron_dia_semana:
        fecha_actual = fecha_base
    else:
        cron = croniter(cron_expr, fecha_base)
        fecha_actual = cron.get_prev(datetime)
        fecha_actual = fecha_actual.replace(hour=0, minute=0, second=0, microsecond=0)

    # Obtener fecha anterior, una semana antes 
    fecha_anterior = fecha_actual - timedelta(weeks=1)

    # Calcular próxima ejecución cron
    cron_prox = croniter(cron_expr, fecha_base)
    fecha_proxima_ejecucion = cron_prox.get_next(datetime).replace(hour=0, minute=0, second=0, microsecond=0)
    logging.info("Calculo de rango de ETL - CRON")
    # Impresiones informativas
    logging.info(f"Hoy: {fecha_base.date()}")
    logging.info(f"Cron configura ejecución los días: {nombre_dia_cron} (#{cron_dia_semana})")
    logging.info(f"Fecha actual (última ejecución válida): {fecha_actual.date()}")
    logging.info(f"Fecha anterior (una semana antes): {fecha_anterior.date()}")
    logging.info(f"Próxima ejecución cron: {fecha_proxima_ejecucion.date()}")

    return fecha_anterior.date()

def start_scheduled_etl():
    fecha_hasta = datetime.now()

    # Leer la expresión cron desde archivo JSON
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    cron_expr = config["times"]["cron"]

    # Calcular el tiempo de inicio anterior según la expresión cron
    fecha_desde = obtener_rango_desde_cron(cron_expr,fecha_hasta)

    start_date = fecha_desde.strftime("%Y-%m-%d")
    end_date = fecha_hasta.strftime("%Y-%m-%d")

    logging.info(f"Ejecutando ETL programada desde {start_date} hasta {end_date} con cron '{cron_expr}'")

    etl_request = ETLRequest(start_date=start_date, end_date=end_date)

    task_repo = InMemoryTaskRepository("cron_etl.log")

    etl_service = ETLService(task_repository=task_repo)
    etl_service.start_etl_task(etl_request)

    logging.info("ETL ejecutada exitosamente")

class CronConfigHandler(FileSystemEventHandler):
    def __init__(self, task_func):
        self.task_func = task_func
        self.load_cron()
        self.lock = threading.Lock()
        self.schedule_next_run()

    def load_cron(self):
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
        logging.info("ETL Le definicion en JSON de configuracion y Ejecuto primer ETL")
        start_scheduled_etl()    
        self.cron_expr = config["times"]["cron"]
        self.cron = croniter(self.cron_expr, datetime.now())
        print(f"[CRON] Cargado: {self.cron_expr}")

    def schedule_next_run(self):
        self.next_run = self.cron.get_next(datetime)
        print(f"[CRON] Próxima ejecución: {self.next_run}")

    def on_modified(self, event):
        if Path(event.src_path).resolve() == CONFIG_PATH.resolve():
            print("[CRON] Cambio detectado en config_cron.json")
            with self.lock:
                self.load_cron()
                self.schedule_next_run()

    def start(self):
        start_scheduled_etl()
        def loop():
            while True:
                now = datetime.now()
                with self.lock:
                    if now >= self.next_run:
                        threading.Thread(target=self.task_func).start()
                        self.schedule_next_run()
                time.sleep(1)

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()

def run_cron_scheduler(task_func):
    #Carga Inicial
    #threading.Thread(target=task_func).start()
    
    handler = CronConfigHandler(task_func)
    
    observer = Observer()
    observer.schedule(handler, ".", recursive=False)
    observer.start()
    handler.start()
