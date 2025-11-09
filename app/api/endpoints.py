from fastapi import APIRouter, HTTPException
from app.models.request_models import ETLRequest
from app.core.etl_services import ETLService
from app.core.ports.adapters import InMemoryTaskRepository

# Instanciar el repositorio y el servicio
task_repository = InMemoryTaskRepository("etl_api.log")
etl_service = ETLService(task_repository)

router = APIRouter()

@router.post("/lm/etl")
async def upload_etl(data: ETLRequest):
    """
    Inicia una tarea de ETL (Extracción, Transformación y Carga) de forma asíncrona.

    Al recibir una solicitud, este endpoint comienza un proceso en segundo plano para procesar 
    grandes volúmenes de datos de licencias médicas. La tarea extrae datos de un sistema 
    origen, los transforma y los carga en una base de datos optimizada para análisis.

    El endpoint responde inmediatamente con un identificador de la tarea, permitiendo 
    al cliente monitorear el progreso de forma separada.

    Args:
        data (ETLRequest): Un objeto que contiene los parámetros para la tarea de ETL, 
                           como las fechas de inicio y fin para la extracción de datos.

    Returns:
        dict: Un diccionario con el estado inicial de la tarea y su identificador único.

    Raises:
        HTTPException: 
            - 400 (Bad Request): Si los datos de entrada son inválidos.
            - 500 (Internal Server Error): Si ocurre un error inesperado durante el 
              inicio de la tarea.
    """
    try:
        return etl_service.start_etl_task(data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")