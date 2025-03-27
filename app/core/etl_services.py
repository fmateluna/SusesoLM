import hashlib
import time
import json
import threading

import psycopg2
from sqlalchemy import text
from app.core.ports.etl import TaskRepository
from app.models.request_models import ETLRequest

from app.core.database import SessionLM, get_db_etl_connection
from sqlalchemy.exc import IntegrityError

### lógica principal del ETL, incluyendo la generación del hash y la ejecución asíncrona de la tarea. ###

def generate_task_id(etl_request: ETLRequest) -> str:    
    data_str = json.dumps(etl_request.dict(), sort_keys=True)
    hash_input = f"{data_str}"
    return hashlib.sha256(hash_input.encode()).hexdigest()

class ETLService:
    
    def __init__(self, task_repository: TaskRepository):
        self.task_repository = task_repository
           
           
    def start_etl_task(self, etl_request: ETLRequest) -> dict:
        task_id = generate_task_id(etl_request)
        current_status = self.task_repository.get_task_status(task_id)
        if current_status:
            return current_status
        
        self.task_repository.set_task_status(task_id, "initial", {"idtask": task_id})

        thread = threading.Thread(target=self.run_etl_task, args=(etl_request, task_id))
        thread.start()
        
        return {"Status": "initial", "detail": {"idtask": task_id}}



    def run_etl_task(self, etl_request: ETLRequest, task_id: str) -> None:
        try:
            self.task_repository.set_task_status(task_id, "in process", {"idtask": task_id, "record_process": 0})            
            conn = get_db_etl_connection()
            cursor = conn.cursor()
            
            # Definir la query base con segmentación por hora/minuto
            base_query = """
            SELECT 
                id_lic, operador, ccaf, entidad_pagadora, folio, fecha_emision, 
                empleador_adscrito, codigo_interno_prestador, comuna_prestador, 
                fecha_ultimo_estado, ultimo_estado, rut_trabajador, sexo_trabajador, 
                edad_trabajador, tipo_reposo, dias_reposo, fecha_inicio_reposo, 
                comuna_reposo, tipo_licencia, rut_medico, especialidad_profesional, 
                tipo_profesional, zbtipo_licencia_entidad AS "tipo_licencia_pronunciamiento", 
                zbcodigo_continuacion AS "codigo_continuacion_pronunciamiento", 
                zbdias_autorizados AS "dias_autorizados_pronunciamiento", 
                zbcodigo_diagnostico AS "codigo_diagnostico_pronunciamiento", 
                zbcodigo_autorizacion AS "codigo_autorizacion_pronunciamiento", 
                zbcausa_rechazo AS "causa_rechazo_pronunciamiento", 
                zbtipo_reposo AS "tipo_reposo_pronunciamiento", 
                zbderecho_a_subsidio AS "derecho_a_subsidio_pronunciamiento", 
                rut_empleador, calidad_trabajador, actividad_laboral_trabajador, 
                ocupacion, entidad_pagadora2 AS "entidad_pagadora_zona_C", 
                fecha_recepcion_empleador, regimen_previsional, 
                entidad_pagadora_subsidio, comuna_laboral, comuna_uso_compin, 
                cantidad_de_pronunciamientos, cantidad_de_zonas_d, 
                secuencia_estados, cod_diagnostico_principal, 
                cod_diagnostico_secundario, periodo, NULL AS "propensity_score_rn", 
                NULL AS "propensity_score_rn2", NULL AS "propensity_score_frecuencia_mensual", 
                NULL AS "propensity_score_frecuencia_semanal", 
                NULL AS "propensity_score_otorgados_mensual", 
                NULL AS "propensity_score_otorgados_semanal", 
                NULL AS "propensity_score_ml", NULL AS "propensity_score"
            FROM lme.sabana_fiscalizador_lme
            WHERE fecha_emision >= '{start_time}' AND fecha_emision < '{end_time}'
            """

            from datetime import datetime, timedelta

            start_date = datetime.strptime(etl_request.start_date, '%Y-%m-%d')
            end_date = datetime.strptime(etl_request.end_date, '%Y-%m-%d')
            
            current_time = start_date
            record_count = 0
            
            while current_time < end_date:
                start_time = current_time
                end_time = current_time + timedelta(hours=1)  
                
                query = base_query.format(start_time=start_time, end_time=end_time)                
                cursor.execute(query)
                column_names = [desc[0] for desc in cursor.description]
                    
                while True:
                    rows = cursor.fetchmany(10) 
                    if not rows:
                        break
                    
                    for row in rows:
                        sabana_fiscalizador_lme_row = dict(zip(column_names, row))          
                        sabana_fiscalizador_lme_row['empleador_adscrito'] = 0 if sabana_fiscalizador_lme_row['empleador_adscrito'] == "No" else 1
                                      
                        self.upload_to_lm(sabana_fiscalizador_lme_row)
                    record_count += len(rows)
                    self.task_repository.set_task_status(task_id, "in process", {
                        "idtask": task_id, "record_process": record_count
                    })
                
                current_time = end_time

            self.task_repository.set_task_status(task_id, "finish", {
                "idtask": task_id, "record_process": record_count
            })
        
        except Exception as e:
            self.task_repository.set_task_status(task_id, "error", {
                "idtask": task_id, "record_process": record_count if 'record_count' in locals() else 0,
                "id_error": 500, "message": str(e)
            })
        finally:
            if 'conn' in locals() and conn:
                conn.close()
                
                
    def create_especialidad(self, sabana_fiscalizador_lme_row):      
        session_especialidad = SessionLM()
        
        try:
            # Manejar especialidad_profesional_medicos VALIDANDO REPLICAS
            especialidad = sabana_fiscalizador_lme_row['especialidad_profesional']
            if especialidad is None or especialidad == '-':
                esp_descripcion = 'No informada'
            else:
                esp_descripcion = especialidad
            esp_descripcion = esp_descripcion.title()

            # Buscar si la especialidad ya existe
            result = session_especialidad.execute(text("""
                SELECT id_especialidad_profesional FROM lm_dev.especialidad_profesional
                WHERE descripcion_especialidad_profesional = :esp_descripcion
            """), {'esp_descripcion': esp_descripcion})
            id_especialidad = result.scalar()

            # Si no existe, insertarla
            if id_especialidad is None:
                session_especialidad.execute(text("""
                    INSERT INTO lm_dev.especialidad_profesional (descripcion_especialidad_profesional)
                    VALUES (:esp_descripcion)
                    RETURNING id_especialidad_profesional
                """), {'esp_descripcion': esp_descripcion})
                id_especialidad = session_especialidad.execute(text("""
                    SELECT id_especialidad_profesional FROM lm_dev.especialidad_profesional
                    WHERE descripcion_especialidad_profesional = :esp_descripcion
                """), {'esp_descripcion': esp_descripcion}).scalar()

            # Confirmar la transacción
            session_especialidad.commit()
            return id_especialidad

        except Exception as e:
            session_especialidad.rollback()
            print(f"Error al crear o buscar especialidad: {e}")
            raise
        finally:
            session_especialidad.close()
                     
            
    def create_profesional(self, sabana_fiscalizador_lme_row):
        session = SessionLM()
        
        try:
            # Determinar la descripción de la profesionalidad
            tipo_profesional = sabana_fiscalizador_lme_row['tipo_profesional']
            if tipo_profesional is None or tipo_profesional == '-' or str(tipo_profesional).strip() == '':
                descripcion = 'No informada'
            else:
                descripcion = str(tipo_profesional)
            descripcion = descripcion.title()

            # Buscar si la profesionalidad ya existe
            result = session.execute(text("""
                SELECT id_profesionalidad FROM lm_dev.profesionalidad
                WHERE descripcion_profesionalidad = :descripcion
            """), {'descripcion': descripcion})
            id_profesionalidad = result.scalar()

            # Si no existe, insertarla
            if id_profesionalidad is None:
                session.execute(text("""
                    INSERT INTO lm_dev.profesionalidad (descripcion_profesionalidad)
                    VALUES (:descripcion)
                    RETURNING id_profesionalidad
                """), {'descripcion': descripcion})
                # Obtener el ID recién insertado
                result = session.execute(text("""
                    SELECT id_profesionalidad FROM lm_dev.profesionalidad
                    WHERE descripcion_profesionalidad = :descripcion
                """), {'descripcion': descripcion})
                id_profesionalidad = result.scalar()

            # Confirmar la transacción
            session.commit()
            return id_profesionalidad

        except Exception as e:
            session.rollback()
            print(f"Error al crear o buscar profesionalidad: {e}")
            raise
        finally:
            session.close()       
            
    def setting_doctor(self, rut_medico, id_especialidad):
        if rut_medico is not None:
            session = SessionLM()
            try:
                # Intentar insertar en lm_dev.medicos
                try:
                    session.execute(text("""
                        INSERT INTO lm_dev.medicos (rut_medico)
                        VALUES (:rut_medico)
                    """), {'rut_medico': rut_medico})
                    session.commit()
                except IntegrityError as e:
                    session.rollback()
                    print(f"Error al insertar en lm_dev.medicos: {e}")
                
                # Intentar insertar en lm_dev.especialidad_profesional_medicos
                try:
                    session.execute(text("""
                        INSERT INTO lm_dev.especialidad_profesional_medicos (id_especialidad_profesional, rut_medico)
                        VALUES (:id_especialidad, :rut_medico)
                    """), {'id_especialidad': id_especialidad, 'rut_medico': rut_medico})
                    session.commit()
                except IntegrityError as e:
                    session.rollback()

            except Exception as e:
                session.rollback()
            finally:
                session.close()     
                               

    def upload_to_lm(self, sabana_fiscalizador_lme_row):
        
        id_especialidad = self.create_especialidad(sabana_fiscalizador_lme_row)
        rut_medico = sabana_fiscalizador_lme_row['rut_medico']
        self.setting_doctor(rut_medico, id_especialidad)
        
        session = SessionLM()
        try:

            # Insertar en la tabla propensity_score VALIDANDO REPLICAS
            propensity_values = {
                'id_lic': sabana_fiscalizador_lme_row['id_lic'],
                'folio': sabana_fiscalizador_lme_row['folio'],
                'rn': sabana_fiscalizador_lme_row['propensity_score_rn'],
                'rn2': sabana_fiscalizador_lme_row['propensity_score_rn2'],
                'frecuencia_mensual': sabana_fiscalizador_lme_row['propensity_score_frecuencia_mensual'],
                'frecuencia_semanal': sabana_fiscalizador_lme_row['propensity_score_frecuencia_semanal'],
                'otorgados_mensual': sabana_fiscalizador_lme_row['propensity_score_otorgados_mensual'],
                'otorgados_semanal': sabana_fiscalizador_lme_row['propensity_score_otorgados_semanal'],
                'ml': sabana_fiscalizador_lme_row['propensity_score_ml'],
                'score': sabana_fiscalizador_lme_row['propensity_score']
            }
            session.execute(text("""
                INSERT INTO lm_dev.propensity_score (
                    id_lic, folio, rn, rn2, frecuencia_mensual, frecuencia_semanal,
                    otorgados_mensual, otorgados_semanal, ml, score
                ) VALUES (
                    :id_lic, :folio, :rn, :rn2, :frecuencia_mensual, :frecuencia_semanal,
                    :otorgados_mensual, :otorgados_semanal, :ml, :score
                )
            """), propensity_values)


            id_profesionalidad = self.create_profesional(sabana_fiscalizador_lme_row)

            # Insertar en medicos si rut_medico no es NULL VALIDANDO REPLICAS
            rut_medico = sabana_fiscalizador_lme_row['rut_medico']
            if rut_medico is not None:
                # Insertar en profesionalidad_medicos VALIDANDO REPLICAS
                if id_profesionalidad is not None:
                    session.execute(text("""
                        INSERT INTO lm_dev.profesionalidad_medicos (id_profesionalidad, rut_medico)
                        VALUES (:id_profesionalidad, :rut_medico)
                        ON CONFLICT (id_profesionalidad, rut_medico) DO NOTHING;
                    """), {'id_profesionalidad': id_profesionalidad, 'rut_medico': rut_medico})
           
            # Insertar en la tabla licencias VALIDANDO REPLICAS
            session.execute(text("""
                INSERT INTO lm_dev.licencias (
                    id_lic, operador, ccaf, entidad_pagadora, folio, fecha_emision, empleador_adscrito,
                    codigo_interno_prestador, comuna_prestador, fecha_ultimo_estado, ultimo_estado,
                    rut_trabajador, sexo_trabajador, edad_trabajador, tipo_reposo, dias_reposo,
                    fecha_inicio_reposo, comuna_reposo, tipo_licencia, rut_medico,
                    tipo_licencia_pronunciamiento, codigo_continuacion_pronunciamiento,
                    dias_autorizados_pronunciamiento, codigo_diagnostico_pronunciamiento,
                    codigo_autorizacion_pronunciamiento, causa_rechazo_pronunciamiento,
                    tipo_reposo_pronunciamiento, derecho_a_subsidio_pronunciamiento, rut_empleador,
                    calidad_trabajador, actividad_laboral_trabajador, ocupacion, entidad_pagadora_zona_c,
                    fecha_recepcion_empleador, regimen_previsional, entidad_pagadora_subsidio,
                    comuna_laboral, comuna_uso_compin, cantidad_de_pronunciamientos, cantidad_de_zonas_d,
                    secuencia_estados, cod_diagnostico_principal, cod_diagnostico_secundario, periodo
                ) VALUES (
                    :id_lic, :operador, :ccaf, :entidad_pagadora, :folio, :fecha_emision, :empleador_adscrito,
                    :codigo_interno_prestador, :comuna_prestador, :fecha_ultimo_estado, :ultimo_estado,
                    :rut_trabajador, :sexo_trabajador, :edad_trabajador, :tipo_reposo, :dias_reposo,
                    :fecha_inicio_reposo, :comuna_reposo, :tipo_licencia, :rut_medico,
                    :tipo_licencia_pronunciamiento, :codigo_continuacion_pronunciamiento,
                    :dias_autorizados_pronunciamiento, :codigo_diagnostico_pronunciamiento,
                    :codigo_autorizacion_pronunciamiento, :causa_rechazo_pronunciamiento,
                    :tipo_reposo_pronunciamiento, :derecho_a_subsidio_pronunciamiento, :rut_empleador,
                    :calidad_trabajador, :actividad_laboral_trabajador, :ocupacion, :entidad_pagadora_zona_C,
                    :fecha_recepcion_empleador, :regimen_previsional, :entidad_pagadora_subsidio,
                    :comuna_laboral, :comuna_uso_compin, :cantidad_de_pronunciamientos, :cantidad_de_zonas_d,
                    :secuencia_estados, :cod_diagnostico_principal, :cod_diagnostico_secundario, :periodo
                )
            """), sabana_fiscalizador_lme_row)
            # Confirmar todas las inserciones y relaciones
            session.commit()
            print(f"Inserción  id_lic: {sabana_fiscalizador_lme_row['id_lic']} - folio {sabana_fiscalizador_lme_row['folio']} ")

        except psycopg2.errors.UniqueViolation as e:
            session.rollback()
            print(f"Registro id_lic: {sabana_fiscalizador_lme_row['id_lic']} - folio {sabana_fiscalizador_lme_row['folio']}  duplicado ignorado: {e} ")
        except Exception as e:
            session.rollback()
            print(f"Registro id_lic: {sabana_fiscalizador_lme_row['id_lic']} - folio {sabana_fiscalizador_lme_row['folio']}  Error durante la inserción: {e}")
            raise