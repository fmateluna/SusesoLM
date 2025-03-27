from app.core.database import SessionLM
from sqlalchemy import text, exc
from app.models.request_models import LicenseRequest, RegionRequest
from datetime import date, datetime
from app.models.response_models import LicenseListResponse, LicenseDetail


def read_sql_file(file_path: str) -> str:
    """Lee una consulta SQL desde un archivo y la devuelve como un string."""
    with open(file_path, "r") as file:
        return file.read()


def parse_dates(fecha_inicio: str, fecha_fin: str) -> tuple[date, date]:
    """Convierte las fechas de string a date, lanzando un error si el formato no es válido."""
    try:
        return (
            datetime.strptime(fecha_inicio, "%Y-%m-%d").date(),
            datetime.strptime(fecha_fin, "%Y-%m-%d").date(),
        )
    except ValueError:
        raise ValueError("Las fechas deben estar en formato YYYY-MM-DD")


def map_to_license_detail(row: dict) -> LicenseDetail:
    """Convierte una fila de resultados en una instancia de LicenseDetail."""
    try:
        mapping = row._mapping if hasattr(row, "_mapping") else row
        return LicenseDetail(
            id_lic=mapping.get("id_lic"),
            operador=mapping.get("operador"),
            ccaf=mapping.get("ccaf"),
            entidad_pagadora=mapping.get("entidad_pagadora"),
            folio=mapping.get("folio"),
            fecha_emision=(
                mapping.get("fecha_emision")
                if isinstance(mapping.get("fecha_emision"), date)
                else None
            ),
            empleador_adscrito=mapping.get("empleador_adscrito"),
            codigo_interno_prestador=mapping.get("codigo_interno_prestador"),
            comuna_prestador=mapping.get("comuna_prestador"),
            fecha_ultimo_estado=(
                mapping.get("fecha_ultimo_estado")
                if isinstance(mapping.get("fecha_ultimo_estado"), date)
                else None
            ),
            ultimo_estado=mapping.get("ultimo_estado"),
            rut_trabajador=mapping.get("rut_trabajador"),
            sexo_trabajador=mapping.get("sexo_trabajador"),
            edad_trabajador=mapping.get("edad_trabajador"),
            tipo_reposo=mapping.get("tipo_reposo"),
            dias_reposo=mapping.get("dias_reposo"),
            fecha_inicio_reposo=(
                mapping.get("fecha_inicio_reposo")
                if isinstance(mapping.get("fecha_inicio_reposo"), date)
                else None
            ),
            comuna_reposo=mapping.get("comuna_reposo"),
            tipo_licencia=mapping.get("tipo_licencia"),
            rut_medico=mapping.get("rut_medico"),
            especialidad_profesional=mapping.get("especialidad_profesional"),
            tipo_profesional=mapping.get("tipo_profesional"),
            tipo_licencia_pronunciamiento=mapping.get("tipo_licencia_pronunciamiento"),
            codigo_continuacion_pronunciamiento=mapping.get(
                "codigo_continuacion_pronunciamiento"
            ),
            dias_autorizados_pronunciamiento=mapping.get(
                "dias_autorizados_pronunciamiento"
            ),
            codigo_diagnostico_pronunciamiento=mapping.get(
                "codigo_diagnostico_pronunciamiento"
            ),
            codigo_autorizacion_pronunciamiento=mapping.get(
                "codigo_autorizacion_pronunciamiento"
            ),
            causa_rechazo_pronunciamiento=mapping.get("causa_rechazo_pronunciamiento"),
            tipo_reposo_pronunciamiento=mapping.get("tipo_reposo_pronunciamiento"),
            derecho_a_subsidio_pronunciamiento=mapping.get(
                "derecho_a_subsidio_pronunciamiento"
            ),
            rut_empleador=mapping.get("rut_empleador"),
            calidad_trabajador=mapping.get("calidad_trabajador"),
            actividad_laboral_trabajador=mapping.get("actividad_laboral_trabajador"),
            ocupacion=mapping.get("ocupacion"),
            entidad_pagadora_zona_c=mapping.get("entidad_pagadora_zona_c"),
            fecha_recepcion_empleador=(
                mapping.get("fecha_recepcion_empleador")
                if isinstance(mapping.get("fecha_recepcion_empleador"), date)
                else None
            ),
            regimen_previsional=mapping.get("regimen_previsional"),
            entidad_pagadora_subsidio=mapping.get("entidad_pagadora_subsidio"),
            comuna_laboral=mapping.get("comuna_laboral"),
            comuna_uso_compin=mapping.get("comuna_uso_compin"),
            cantidad_de_pronunciamientos=mapping.get("cantidad_de_pronunciamientos"),
            cantidad_de_zonas_d=mapping.get("cantidad_de_zonas_d"),
            secuencia_estados=mapping.get("secuencia_estados"),
            cod_diagnostico_principal=mapping.get("cod_diagnostico_principal"),
            cod_diagnostico_secundario=mapping.get("cod_diagnostico_secundario"),
            periodo=mapping.get("periodo"),
            propensity_score_rn=mapping.get("propensity_score_rn"),
            propensity_score_rn2=mapping.get("propensity_score_rn2"),
            propensity_score_frecuencia_mensual=mapping.get(
                "propensity_score_frecuencia_mensual"
            ),
            propensity_score_frecuencia_semanal=mapping.get(
                "propensity_score_frecuencia_semanal"
            ),
            propensity_score_otorgados_mensual=mapping.get(
                "propensity_score_otorgados_mensual"
            ),
            propensity_score_otorgados_semanal=mapping.get(
                "propensity_score_otorgados_semanal"
            ),
            propensity_score_ml=mapping.get("propensity_score_ml"),
            propensity_score=mapping.get("propensity_score"),
        )
    except Exception as e:
        print("Error al mapear la fila a LicenseDetail:", row, "Error:", e)
        raise


def execute_query(file_path: str, params: dict):
    """Ejecuta una consulta SQL desde un archivo con parámetros proporcionados."""
    session = SessionLM()
    query = read_sql_file(file_path)
    try:
        result = session.execute(text(query), params).fetchall()
        return result
    except exc.SQLAlchemyError as e:
        session.rollback()
        raise ValueError(f"Error en la ejecución de la consulta SQL: {str(e)}")
    except Exception as e:
        session.rollback()
        raise ValueError(f"Error inesperado: {str(e)}")
    finally:
        session.close()


def get_total_licenses(data: LicenseRequest):
    """Obtiene el total de licencias por profesional y diagnóstico."""
    if not all([data.fecha_inicio, data.fecha_fin, data.folio]):
        raise ValueError(
            "Todos los parámetros deben ser proporcionados: fecha_inicio, fecha_fin, folio"
        )

    result = execute_query(
        "./sql/licencias_1.sql",
        {
            "fecha_inicio": data.fecha_inicio,
            "fecha_fin": data.fecha_fin,
            "folio": data.folio,
        },
    )
    if result:
        rut_medico = result[0]._mapping["rut_medico"]
        licencias = [
            {
                "total_licencias": row._mapping["total_licencias"],
                "cod_diagnostico_principal": row._mapping["cod_diagnostico_principal"],
            }
            for row in result
        ]
        response = {
            "rut_medico": rut_medico,
            "fecha_inicio": data.fecha_inicio,
            "fecha_fin": data.fecha_fin,
            "licencias": licencias,
        }
        return response
    else:
        return {
            "rut_medico": None,
            "fecha_inicio": data.fecha_inicio.strftime("%Y-%m-%d"),
            "fecha_fin": data.fecha_fin.strftime("%Y-%m-%d"),
            "licencias": [],
        }


def get_licenses_by_doctor(
    rut_medico: str, fecha_inicio: str, fecha_fin: str
) -> LicenseListResponse:
    """Obtiene el listado de LM emitida por un médico."""
    fecha_inicio, fecha_fin = parse_dates(fecha_inicio, fecha_fin)
    try:
        result = execute_query(
            "./sql/licencias_2.sql",
            {
                "rut_medico": rut_medico,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
            },
        )
        if not result:
            return LicenseListResponse(licenses=[])
        licenses = [map_to_license_detail(row) for row in result if row]
        licenses = [l for l in licenses if l]  # Filtrar None
        return LicenseListResponse(licenses=licenses)
    except Exception as e:
        print(f"Error ejecutando la consulta get_licenses_by_doctor: {e}")
        raise


def get_licenses_without_fundamento(fecha_inicio: str, fecha_fin: str):
    """Obtiene el listado de LM sin fundamento médico en un rango de tiempo."""
    fecha_inicio, fecha_fin = parse_dates(fecha_inicio, fecha_fin)
    try:
        result = execute_query(
            "./sql/licencias_3.sql", {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin}
        )
        licenses = []
        if not result:
            return LicenseListResponse(licenses=[])
        licenses = [map_to_license_detail(row) for row in result if row]
        licenses = [l for l in licenses if l]  # Filtrar None
        return LicenseListResponse(licenses=licenses)
    except Exception as e:
        print(f"Error ejecutando la consulta get_licenses_by_doctor: {e}")
        raise


def get_licenses_by_folio(folio: str) -> LicenseDetail:
    """Obtiene el listado de LM emitida por el folio."""
    try:
        result = execute_query(
            "./sql/licencias_7.sql",
            {"folio": folio},
        )
        if not result:
            return None
        licenses = [map_to_license_detail(row) for row in result if row]
        licenses = [l for l in licenses if l]  # Filtrar None
        return licenses[0]
    except Exception as e:
        print(f"Error ejecutando la consulta get_licenses_by_doctor: {e}")
        raise


def get_fundamento_indicator(folio: str):
    """Obtiene el indicador de fundamento médico de una LM."""
    result = execute_query("./sql/licencias_4.sql", {"folio": folio})
    return result


def get_licenses_by_diagnosis(cod_diagnostico: str, fecha_inicio: str, fecha_fin: str):
    """Obtiene el listado de LM por diagnóstico específico, periodo de tiempo opcional."""
    fecha_inicio, fecha_fin = parse_dates(fecha_inicio, fecha_fin)

    result = execute_query(
        "./sql/licencias_5.sql",
        {
            "cod_diagnostico": cod_diagnostico,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
        },
    )
    return result


def get_licenses_by_region(comuna_reposo: str, fecha_inicio: str, fecha_fin: str):
    """Obtiene el listado de LM por región y periodo de tiempo opcional."""
    fecha_inicio, fecha_fin = parse_dates(fecha_inicio, fecha_fin)

    result = execute_query(
        "./sql/licencias_6.sql",
        {
            "comuna_reposo": comuna_reposo,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
        },
    )
    return result


def get_licenses_by_trabajador(
    rut_trabajador: str, fecha_inicio: str, fecha_fin: str
) -> LicenseListResponse:
    """Obtiene el listado de LM emitida para un trabajador."""
    try:
        fecha_inicio, fecha_fin = parse_dates(fecha_inicio, fecha_fin)
        result = execute_query(
            "./sql/licencias_8.sql",
            {
                "rut_trabajador": rut_trabajador,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
            },
        )
        if not result:
            return LicenseListResponse(licenses=[])
        licenses = [map_to_license_detail(row) for row in result if row]
        licenses = [l for l in licenses if l]  # Filtrar None
        return LicenseListResponse(licenses=licenses)
    except Exception as e:
        print(f"Error ejecutando la consulta get_licenses_by_doctor: {e}")
        raise

def get_licenses_by_diagnostico(
    codigo_diagnostico_pronunciamiento: str, fecha_inicio: str, fecha_fin: str
) -> LicenseListResponse:
    """Obtiene el listado de LM emitida para un trabajador."""
    try:
        fecha_inicio, fecha_fin = parse_dates(fecha_inicio, fecha_fin)
        result = execute_query(
            "./sql/licencias_9.sql",
            {
                "codigo_diagnostico_pronunciamiento": codigo_diagnostico_pronunciamiento,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
            },
        )
        if not result:
            return LicenseListResponse(licenses=[])
        licenses = [map_to_license_detail(row) for row in result if row]
        licenses = [l for l in licenses if l]  # Filtrar None
        return LicenseListResponse(licenses=licenses)
    except Exception as e:
        print(f"Error ejecutando la consulta get_licenses_by_doctor: {e}")
        raise
