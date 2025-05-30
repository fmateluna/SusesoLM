from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException
from app.core.services import (
    get_licenses_by_diagnostico,
    get_licenses_by_folio,
    get_licenses_by_trabajador,
    get_total_licenses, 
    get_licenses_by_doctor, 
    get_licenses_without_fundamento, 
    get_fundamento_indicator, 
    get_licenses_by_diagnosis, 
    get_licenses_by_region
)
from app.models.request_models import DiagnosticoLicenseByRangeDateRequest, DoctorLicenseByRangeDateRequest, LicenseByRangeDateRequest, LicenseRequest, DoctorLicenseRequest, NoFundamentoRequest, FundamentoIndicatorRequest, DiagnosisRequest, RegionRequest, TrabajadorLicenseByRangeDateRequest
from fastapi import APIRouter, HTTPException
from app.models.response_models import LicenseDetail, LicenseListResponse

from fastapi import APIRouter, HTTPException
from app.models.request_models import ETLRequest
from app.core.etl_services import ETLService
from app.core.ports.adapters import InMemoryTaskRepository

# Instanciar el repositorio y el servicio
task_repository = InMemoryTaskRepository("etl_api.log")
etl_service = ETLService(task_repository)


def set_default_dates(fecha_inicio: Optional[str], fecha_fin: Optional[str]) -> (str, str):
    """
    Función que asigna fechas por defecto si las fechas de entrada son None o vacías.
    :param fecha_inicio: Fecha de inicio en formato 'YYYY-MM-DD' o None
    :param fecha_fin: Fecha de fin en formato 'YYYY-MM-DD' o None
    :return: Tupla con las fechas (fecha_inicio, fecha_fin)
    """
    if not fecha_inicio:
        fecha_inicio = "1900-01-01"
    if not fecha_fin:
        fecha_fin = datetime.today().strftime('%Y-%m-%d')  # Fecha actual en formato 'YYYY-MM-DD'
    
    return fecha_inicio, fecha_fin


router = APIRouter()

@router.post("/lm/doctor/licenses", response_model=LicenseListResponse)
async def licenses_by_doctor(data: DoctorLicenseByRangeDateRequest):
    try:
        fecha_inicio, fecha_fin = set_default_dates(data.fecha_inicio, data.fecha_fin)
        
        result = get_licenses_by_doctor(data.rut_medico, fecha_inicio, fecha_fin)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    
@router.post("/lm/trabajador/licenses", response_model=LicenseListResponse)
async def licenses_by_trabajador(data: TrabajadorLicenseByRangeDateRequest):
    try:
        fecha_inicio, fecha_fin = set_default_dates(data.fecha_inicio, data.fecha_fin)
        
        result = get_licenses_by_trabajador(data.rut_trabajador, fecha_inicio, fecha_fin)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    
@router.post("/lm/diagnostico/licenses", response_model=LicenseListResponse)
async def licenses_by_diagnostico(data: DiagnosticoLicenseByRangeDateRequest):
    try:
        fecha_inicio, fecha_fin = set_default_dates(data.fecha_inicio, data.fecha_fin)
        
        result = get_licenses_by_diagnostico(data.codigo_diagnostico_pronunciamiento, fecha_inicio, fecha_fin)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")    

@router.post("/lm/dto/total")
async def total_licenses(data: LicenseRequest):
    try:
        result = get_total_licenses(data)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/lm/no-fundamento")
async def licenses_without_fundamento(data: NoFundamentoRequest):
    try:
        result = get_licenses_without_fundamento(data.fecha_inicio, data.fecha_fin)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@router.get("/lm/folio/licenses/{folio}", response_model=LicenseDetail)
async def licenses_by_folio(folio: str):
    try:
        result = get_licenses_by_folio(folio)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Lincencia no encontrada: {str(folio)}")    
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")




@router.post("/lm/fundamento-indicator")
async def fundamento_indicator(data: FundamentoIndicatorRequest):
    try:
        result = get_fundamento_indicator(data.folio)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@router.post("/lm/diagnosis")
async def licenses_by_diagnosis(data: DiagnosisRequest):
    try:
        result = get_licenses_by_diagnosis(data.cod_diagnostico, data.fecha_inicio, data.fecha_fin)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@router.post("/lm/region")
async def licenses_by_region(data: RegionRequest):
    try:
        result = get_licenses_by_region(data.comuna_reposo, data.fecha_inicio, data.fecha_fin)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/lm/etl")
async def upload_etl(data: ETLRequest):
    try:
        return etl_service.start_etl_task(data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")