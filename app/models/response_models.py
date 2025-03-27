from pydantic import BaseModel
from typing import List, Optional
from datetime import date

class LicenseDetail(BaseModel):
    id_lic: Optional[str] = None
    operador: Optional[str] = None
    ccaf: Optional[str] = None
    entidad_pagadora: Optional[str] = None
    folio: Optional[str] = None
    fecha_emision: Optional[date] = None
    empleador_adscrito: Optional[int] = None
    codigo_interno_prestador: Optional[int] = None
    comuna_prestador: Optional[str] = None
    fecha_ultimo_estado: Optional[date] = None
    ultimo_estado: Optional[int] = None
    rut_trabajador: Optional[str] = None
    sexo_trabajador: Optional[str] = None
    edad_trabajador: Optional[float] = None
    tipo_reposo: Optional[str] = None
    dias_reposo: Optional[int] = None
    fecha_inicio_reposo: Optional[date] = None
    comuna_reposo: Optional[str] = None
    tipo_licencia: Optional[int] = None
    rut_medico: Optional[str] = None
    especialidad_profesional: Optional[str] = None
    tipo_profesional: Optional[str] = None
    tipo_licencia_pronunciamiento: Optional[float] = None
    codigo_continuacion_pronunciamiento: Optional[float] = None
    dias_autorizados_pronunciamiento: Optional[float] = None
    codigo_diagnostico_pronunciamiento: Optional[str] = None
    codigo_autorizacion_pronunciamiento: Optional[float] = None
    causa_rechazo_pronunciamiento: Optional[str] = None
    tipo_reposo_pronunciamiento: Optional[str] = None
    derecho_a_subsidio_pronunciamiento: Optional[str] = None
    rut_empleador: Optional[str] = None
    calidad_trabajador: Optional[str] = None
    actividad_laboral_trabajador: Optional[int] = None
    ocupacion: Optional[int] = None
    entidad_pagadora_zona_c: Optional[str] = None
    fecha_recepcion_empleador: Optional[date] = None
    regimen_previsional: Optional[int] = None
    entidad_pagadora_subsidio: Optional[str] = None
    comuna_laboral: Optional[str] = None
    comuna_uso_compin: Optional[str] = None
    cantidad_de_pronunciamientos: Optional[int] = None
    cantidad_de_zonas_d: Optional[int] = None
    secuencia_estados: Optional[str] = None
    cod_diagnostico_principal: Optional[str] = None
    cod_diagnostico_secundario: Optional[str] = None
    periodo: Optional[str] = None
    propensity_score_rn: Optional[int] = None
    propensity_score_rn2: Optional[int] = None
    propensity_score_frecuencia_mensual: Optional[float] = None
    propensity_score_frecuencia_semanal: Optional[float] = None
    propensity_score_otorgados_mensual: Optional[float] = None
    propensity_score_otorgados_semanal: Optional[float] = None
    propensity_score_ml: Optional[float] = None
    propensity_score: Optional[float] = None

    class Config:
        # Actualización a 'from_attributes' en lugar de 'orm_mode'
        from_attributes = True
        exclude_none = True  # Esto asegura que los campos con valor None no se incluyan en el JSON

class LicenseListResponse(BaseModel):
    licenses: List[LicenseDetail]

    class Config:
        # Actualización a 'from_attributes' en lugar de 'orm_mode'
        from_attributes = True
        exclude_none = True  # Esto asegura que los campos con valor None no se incluyan en el JSON

### ETL ###

from pydantic import BaseModel
from typing import Optional, Dict, Any

class ETL_Response(BaseModel):
    Status: str
    detail: Dict[str, Any]