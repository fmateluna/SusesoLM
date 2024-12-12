from app.core.database import SessionLocal
from sqlalchemy import text, exc
from app.models.request_models import LicenseRequest, RegionRequest
from datetime import date, datetime
from app.models.response_models import LicenseListResponse, LicenseDetail

def read_sql_file(file_path: str) -> str:
    """Lee una consulta SQL desde un archivo y la devuelve como un string."""
    with open(file_path, 'r') as file:
        return file.read()

def execute_query(file_path: str, params: dict):
    """Ejecuta una consulta SQL desde un archivo con parámetros proporcionados."""
    session = SessionLocal()
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
        raise ValueError("Todos los parámetros deben ser proporcionados: fecha_inicio, fecha_fin, folio")

    result = execute_query("./sql/licencias_1.sql", {
        "fecha_inicio": data.fecha_inicio,  
        "fecha_fin": data.fecha_fin,       
        "folio": data.folio
    })
    if result:
        rut_medico = result[0]._mapping["rut_medico"]
        licencias = [
            {
                "total_licencias": row._mapping["total_licencias"],
                "cod_diagnostico_principal": row._mapping["cod_diagnostico_principal"]
            }
            for row in result
        ]
        response = {
            "rut_medico": rut_medico,
            "fecha_inicio": data.fecha_inicio,
            "fecha_fin": data.fecha_fin,
            "licencias": licencias
        }
        return response
    else:
        return {
            "rut_medico": None,
            "fecha_inicio": data.fecha_inicio.strftime('%Y-%m-%d'),
            "fecha_fin": data.fecha_fin.strftime('%Y-%m-%d'),
            "licencias": []
        }



def get_licenses_by_doctor(rut_medico: str) -> LicenseListResponse:
    """Obtiene el listado de LM emitida por un médico."""
    result = execute_query("./sql/licencias_2.sql", {"rut_medico": rut_medico})
    licenses = [
        LicenseDetail(
            id_lic=row._mapping["id_lic"],
            operador=row._mapping["operador"],
            ccaf=row._mapping["ccaf"],
            entidad_pagadora=row._mapping["entidad_pagadora"],
            folio=row._mapping["folio"],
            fecha_emision=row._mapping["fecha_emision"],
            empleador_adscrito=row._mapping["empleador_adscrito"],
            codigo_interno_prestador=row._mapping["codigo_interno_prestador"],
            comuna_prestador=row._mapping["comuna_prestador"],
            fecha_ultimo_estado=row._mapping["fecha_ultimo_estado"],
            ultimo_estado=row._mapping["ultimo_estado"],
            rut_trabajador=row._mapping["rut_trabajador"],
            sexo_trabajador=row._mapping["sexo_trabajador"],
            edad_trabajador=row._mapping["edad_trabajador"],
            tipo_reposo=row._mapping["tipo_reposo"],
            dias_reposo=row._mapping["dias_reposo"],
            fecha_inicio_reposo=row._mapping["fecha_inicio_reposo"],
            comuna_reposo=row._mapping["comuna_reposo"],
            tipo_licencia=row._mapping["tipo_licencia"],
            rut_medico=row._mapping["rut_medico"],
            especialidad_profesional=row._mapping["especialidad_profesional"],
            tipo_profesional=row._mapping["tipo_profesional"],
            tipo_licencia_pronunciamiento=row._mapping["tipo_licencia_pronunciamiento"],
            codigo_continuacion_pronunciamiento=row._mapping["codigo_continuacion_pronunciamiento"],
            dias_autorizados_pronunciamiento=row._mapping["dias_autorizados_pronunciamiento"],
            codigo_diagnostico_pronunciamiento=row._mapping["codigo_diagnostico_pronunciamiento"],
            codigo_autorizacion_pronunciamiento=row._mapping["codigo_autorizacion_pronunciamiento"],
            causa_rechazo_pronunciamiento=row._mapping["causa_rechazo_pronunciamiento"],
            tipo_reposo_pronunciamiento=row._mapping["tipo_reposo_pronunciamiento"],
            derecho_a_subsidio_pronunciamiento=row._mapping["derecho_a_subsidio_pronunciamiento"],
            rut_empleador=row._mapping["rut_empleador"],
            calidad_trabajador=row._mapping["calidad_trabajador"],
            actividad_laboral_trabajador=row._mapping["actividad_laboral_trabajador"],
            ocupacion=row._mapping["ocupacion"],
            entidad_pagadora_zona_c=row._mapping["entidad_pagadora_zona_c"],
            fecha_recepcion_empleador=row._mapping["fecha_recepcion_empleador"],
            regimen_previsional=row._mapping["regimen_previsional"],
            entidad_pagadora_subsidio=row._mapping["entidad_pagadora_subsidio"],
            comuna_laboral=row._mapping["comuna_laboral"],
            comuna_uso_compin=row._mapping["comuna_uso_compin"],
            cantidad_de_pronunciamientos=row._mapping["cantidad_de_pronunciamientos"],
            cantidad_de_zonas_d=row._mapping["cantidad_de_zonas_d"],
            secuencia_estados=row._mapping["secuencia_estados"],
            cod_diagnostico_principal=row._mapping["cod_diagnostico_principal"],
            cod_diagnostico_secundario=row._mapping["cod_diagnostico_secundario"],
            periodo=row._mapping["periodo"],
            propensity_score_rn=row._mapping["propensity_score_rn"],
            propensity_score_rn2=row._mapping["propensity_score_rn2"],
            propensity_score_frecuencia_mensual=row._mapping["propensity_score_frecuencia_mensual"],
            propensity_score_frecuencia_semanal=row._mapping["propensity_score_frecuencia_semanal"],
            propensity_score_otorgados_mensual=row._mapping["propensity_score_otorgados_mensual"],
            propensity_score_otorgados_semanal=row._mapping["propensity_score_otorgados_semanal"],
            propensity_score_ml=row._mapping["propensity_score_ml"],
            propensity_score=row._mapping["propensity_score"]
        )
        for row in result
    ]
    return LicenseListResponse(licenses=licenses)


def get_licenses_without_fundamento(fecha_inicio: str, fecha_fin: str):
    """Obtiene el listado de LM sin fundamento médico en un rango de tiempo."""
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("Las fechas deben estar en formato YYYY-MM-DD")

    result = execute_query("./sql/licencias_3.sql", {
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin
    })
    licenses = []
    for row in result:
        try:
            mapping = row._mapping if hasattr(row, '_mapping') else row
            licenses.append(
                LicenseDetail(
                    id_lic=mapping.get("id_lic"),
                    operador=mapping.get("operador"),
                    ccaf=mapping.get("ccaf"),
                    entidad_pagadora=mapping.get("entidad_pagadora"),
                    folio=mapping.get("folio"),
                    fecha_emision=mapping.get("fecha_emision") if isinstance(mapping.get("fecha_emision"), date) else None,
                    empleador_adscrito=mapping.get("empleador_adscrito"),
                    codigo_interno_prestador=mapping.get("codigo_interno_prestador"),
                    comuna_prestador=mapping.get("comuna_prestador"),
                    fecha_ultimo_estado=mapping.get("fecha_ultimo_estado") if isinstance(mapping.get("fecha_ultimo_estado"), date) else None,
                    ultimo_estado=mapping.get("ultimo_estado"),
                    rut_trabajador=mapping.get("rut_trabajador"),
                    sexo_trabajador=mapping.get("sexo_trabajador"),
                    edad_trabajador=mapping.get("edad_trabajador"),
                    tipo_reposo=mapping.get("tipo_reposo"),
                    dias_reposo=mapping.get("dias_reposo"),
                    fecha_inicio_reposo=mapping.get("fecha_inicio_reposo") if isinstance(mapping.get("fecha_inicio_reposo"), date) else None,
                    comuna_reposo=mapping.get("comuna_reposo"),
                    tipo_licencia=mapping.get("tipo_licencia"),
                    rut_medico=mapping.get("rut_medico"),
                    especialidad_profesional=mapping.get("especialidad_profesional"),
                    tipo_profesional=mapping.get("tipo_profesional"),
                    tipo_licencia_pronunciamiento=mapping.get("tipo_licencia_pronunciamiento"),
                    codigo_continuacion_pronunciamiento=mapping.get("codigo_continuacion_pronunciamiento"),
                    dias_autorizados_pronunciamiento=mapping.get("dias_autorizados_pronunciamiento"),
                    codigo_diagnostico_pronunciamiento=mapping.get("codigo_diagnostico_pronunciamiento"),
                    codigo_autorizacion_pronunciamiento=mapping.get("codigo_autorizacion_pronunciamiento"),
                    causa_rechazo_pronunciamiento=mapping.get("causa_rechazo_pronunciamiento"),
                    tipo_reposo_pronunciamiento=mapping.get("tipo_reposo_pronunciamiento"),
                    derecho_a_subsidio_pronunciamiento=mapping.get("derecho_a_subsidio_pronunciamiento"),
                    rut_empleador=mapping.get("rut_empleador"),
                    calidad_trabajador=mapping.get("calidad_trabajador"),
                    actividad_laboral_trabajador=mapping.get("actividad_laboral_trabajador"),
                    ocupacion=mapping.get("ocupacion"),
                    entidad_pagadora_zona_c=mapping.get("entidad_pagadora_zona_c"),
                    fecha_recepcion_empleador=mapping.get("fecha_recepcion_empleador") if isinstance(mapping.get("fecha_recepcion_empleador"), date) else None,
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
                    propensity_score_frecuencia_mensual=mapping.get("propensity_score_frecuencia_mensual"),
                    propensity_score_frecuencia_semanal=mapping.get("propensity_score_frecuencia_semanal"),
                    propensity_score_otorgados_mensual=mapping.get("propensity_score_otorgados_mensual"),
                    propensity_score_otorgados_semanal=mapping.get("propensity_score_otorgados_semanal"),
                    propensity_score_ml=mapping.get("propensity_score_ml"),
                    propensity_score=mapping.get("propensity_score")
                )
            )
        except Exception as e:
            print("Error procesando la fila:", row, "Error:", e)
            continue

    return LicenseListResponse(licenses=licenses)



def get_licenses_by_doctor(rut_medico: str) -> LicenseListResponse:
    """Obtiene el listado de LM emitida por un médico."""
    try:
        result = execute_query("./sql/licencias_2.sql", {"rut_medico": rut_medico})
        if not result:
            print("No se encontraron resultados para el médico con RUT:", rut_medico)
            return LicenseListResponse(licenses=[])

        licenses = []
        for row in result:
            try:
                mapping = row._mapping if hasattr(row, '_mapping') else row
                licenses.append(
                    LicenseDetail(
                        id_lic=mapping.get("id_lic"),
                        operador=mapping.get("operador"),
                        ccaf=mapping.get("ccaf"),
                        entidad_pagadora=mapping.get("entidad_pagadora"),
                        folio=mapping.get("folio"),
                        fecha_emision=mapping.get("fecha_emision") if isinstance(mapping.get("fecha_emision"), date) else None,
                        empleador_adscrito=mapping.get("empleador_adscrito"),
                        codigo_interno_prestador=mapping.get("codigo_interno_prestador"),
                        comuna_prestador=mapping.get("comuna_prestador"),
                        fecha_ultimo_estado=mapping.get("fecha_ultimo_estado") if isinstance(mapping.get("fecha_ultimo_estado"), date) else None,
                        ultimo_estado=mapping.get("ultimo_estado"),
                        rut_trabajador=mapping.get("rut_trabajador"),
                        sexo_trabajador=mapping.get("sexo_trabajador"),
                        edad_trabajador=mapping.get("edad_trabajador"),
                        tipo_reposo=mapping.get("tipo_reposo"),
                        dias_reposo=mapping.get("dias_reposo"),
                        fecha_inicio_reposo=mapping.get("fecha_inicio_reposo") if isinstance(mapping.get("fecha_inicio_reposo"), date) else None,
                        comuna_reposo=mapping.get("comuna_reposo"),
                        tipo_licencia=mapping.get("tipo_licencia"),
                        rut_medico=mapping.get("rut_medico"),
                        especialidad_profesional=mapping.get("especialidad_profesional"),
                        tipo_profesional=mapping.get("tipo_profesional"),
                        tipo_licencia_pronunciamiento=mapping.get("tipo_licencia_pronunciamiento"),
                        codigo_continuacion_pronunciamiento=mapping.get("codigo_continuacion_pronunciamiento"),
                        dias_autorizados_pronunciamiento=mapping.get("dias_autorizados_pronunciamiento"),
                        codigo_diagnostico_pronunciamiento=mapping.get("codigo_diagnostico_pronunciamiento"),
                        codigo_autorizacion_pronunciamiento=mapping.get("codigo_autorizacion_pronunciamiento"),
                        causa_rechazo_pronunciamiento=mapping.get("causa_rechazo_pronunciamiento"),
                        tipo_reposo_pronunciamiento=mapping.get("tipo_reposo_pronunciamiento"),
                        derecho_a_subsidio_pronunciamiento=mapping.get("derecho_a_subsidio_pronunciamiento"),
                        rut_empleador=mapping.get("rut_empleador"),
                        calidad_trabajador=mapping.get("calidad_trabajador"),
                        actividad_laboral_trabajador=mapping.get("actividad_laboral_trabajador"),
                        ocupacion=mapping.get("ocupacion"),
                        entidad_pagadora_zona_c=mapping.get("entidad_pagadora_zona_c"),
                        fecha_recepcion_empleador=mapping.get("fecha_recepcion_empleador") if isinstance(mapping.get("fecha_recepcion_empleador"), date) else None,
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
                        propensity_score_frecuencia_mensual=mapping.get("propensity_score_frecuencia_mensual"),
                        propensity_score_frecuencia_semanal=mapping.get("propensity_score_frecuencia_semanal"),
                        propensity_score_otorgados_mensual=mapping.get("propensity_score_otorgados_mensual"),
                        propensity_score_otorgados_semanal=mapping.get("propensity_score_otorgados_semanal"),
                        propensity_score_ml=mapping.get("propensity_score_ml"),
                        propensity_score=mapping.get("propensity_score")
                    )
                )
            except Exception as e:
                print("Error procesando la fila:", row, "Error:", e)
                continue

        return LicenseListResponse(licenses=licenses)

    except Exception as e:
        print("Error en get_licenses_by_doctor:", e)
        raise

def get_fundamento_indicator(folio: str):
    """Obtiene el indicador de fundamento médico de una LM."""
    result = execute_query("./sql/licencias_4.sql", {"folio": folio})
    return result

def get_licenses_by_diagnosis(cod_diagnostico: str, fecha_inicio: str, fecha_fin: str):
    """Obtiene el listado de LM por diagnóstico específico, periodo de tiempo opcional."""
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("Las fechas deben estar en formato YYYY-MM-DD")

    result = execute_query("./sql/licencias_5.sql", {
        "cod_diagnostico": cod_diagnostico,
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin
    })
    return result

def get_licenses_by_region(comuna_reposo: str, fecha_inicio: str, fecha_fin: str):
    """Obtiene el listado de LM por región y periodo de tiempo opcional."""
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("Las fechas deben estar en formato YYYY-MM-DD")

    result = execute_query("./sql/licencias_6.sql", {
        "comuna_reposo": comuna_reposo,
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin
    })
    return result
