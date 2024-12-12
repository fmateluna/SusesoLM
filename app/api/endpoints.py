from fastapi import APIRouter, HTTPException
from app.core.services import (
    get_total_licenses, 
    get_licenses_by_doctor, 
    get_licenses_without_fundamento, 
    get_fundamento_indicator, 
    get_licenses_by_diagnosis, 
    get_licenses_by_region
)
from app.models.request_models import LicenseRequest, DoctorLicenseRequest, NoFundamentoRequest, FundamentoIndicatorRequest, DiagnosisRequest, RegionRequest
from fastapi import APIRouter, HTTPException
from app.models.response_models import LicenseListResponse

router = APIRouter()

@router.post("/lm/dto/total")
async def total_licenses(data: LicenseRequest):
    try:
        result = get_total_licenses(data)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/lm/doctor/licenses", response_model=LicenseListResponse)
async def licenses_by_doctor(data: DoctorLicenseRequest):
    try:
        result = get_licenses_by_doctor(data.rut_medico)
        return result
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
