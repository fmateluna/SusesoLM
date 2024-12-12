# app/models/request_models.py
from pydantic import BaseModel
from datetime import date

class LicenseRequest(BaseModel):
    fecha_inicio: date
    fecha_fin: date
    folio: str

class DoctorLicenseRequest(BaseModel):
    rut_medico: str


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
