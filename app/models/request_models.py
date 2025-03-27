# app/models/request_models.py
from typing import List, Optional
from pydantic import BaseModel
from datetime import date

class LicenseRequest(BaseModel):
    fecha_inicio: date
    fecha_fin: date
    folio: str

class DoctorLicenseRequest(BaseModel):
    rut_medico: str
    
class LicenseByRangeDateRequest(BaseModel):
    folio: str
    fecha_inicio: str
    fecha_fin: str   

class NoFundamentoRequest(BaseModel):
    fecha_inicio: str
    fecha_fin: str

class FundamentoIndicatorRequest(BaseModel):
    folio: str

class DiagnosisRequest(BaseModel):
    cod_diagnostico: str
    fecha_inicio: str
    fecha_fin: str


class RegionRequest(BaseModel):
    comuna_reposo: str
    fecha_inicio: str
    fecha_fin: str

class DoctorLicenseByRangeDateRequest(BaseModel):
    rut_medico: str
    fecha_inicio: Optional[str] = "1900-01-01"  
    fecha_fin: Optional[str] = None  

class TrabajadorLicenseByRangeDateRequest(BaseModel):
    rut_trabajador: str
    fecha_inicio: Optional[str] = "1900-01-01" 
    fecha_fin: Optional[str] = None  

class DiagnosticoLicenseByRangeDateRequest(BaseModel):
    codigo_diagnostico_pronunciamiento: str
    fecha_inicio: Optional[str] = "1900-01-01"  
    fecha_fin: Optional[str] = None  

### CARGA ETL ###


class ETLRequest(BaseModel):
    start_date: str
    end_date: str