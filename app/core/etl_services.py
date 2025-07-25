import hashlib
import time
import json
import threading
import os
import csv
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import text
from app.core.ports.etl import TaskRepository
from app.models.request_models import ETLRequest

from app.core.database import SessionML, get_db_ETL_connection
from sqlalchemy.exc import IntegrityError

import logging
import asyncio
import aiohttp

### lógica principal del ETL, incluyendo la generación del hash y la ejecución asíncrona de la tarea. ###

def generate_task_id(etl_request: ETLRequest) -> str:    
    data_str = json.dumps(etl_request.dict(), sort_keys=True)
    hash_input = f"{data_str}"
    return hashlib.sha256(hash_input.encode()).hexdigest()

class ETLService:
    
    def __init__(self, task_repository: TaskRepository, config_log: bool = False):
        self.task_repository = task_repository
        self.__id_licencias = set() 
        self.__medicos = set()
        self.__especialidades = set()
        # Configurar logging
        if not config_log:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                filename=task_repository._get_log_file(),  
                filemode='a' 
            )
                
    def start_etl_task(self, etl_request: ETLRequest) -> dict:
        task_id = generate_task_id(etl_request)
        current_status = self.task_repository.get_task_status(task_id)
        if current_status:
            return current_status
        
        self.task_repository.set_task_status(task_id, "initial", {"idtask": task_id})

        thread = threading.Thread(target=self.run_etl_task, args=(etl_request, task_id))
        thread.start()
        
        return {"Status": "initial", "detail": {"idtask": task_id}}

    async def execute_rn_api_call(self, start_date: str, end_date: str, task_id: str) -> None:
        try:
            # Obtener IP y puerto desde variables de entorno, con valores por defecto
            api_ip = os.getenv('DB_ML_HOST', '192.168.150.84')
            api_port = os.getenv('API_ML_PORT', '9000')
            api_url = f'http://{api_ip}:{api_port}/lm/ml/score/'
            
            payload = {
                "fecha_inicio": start_date,
                "fecha_fin": end_date
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, json=payload, headers={'Content-Type': 'application/json'}) as response:
                    if response.status == 200:
                        logging.info(f"API retorno API de ejecucion de regla de negocio exitoso task_id {task_id}: {await response.text()}")
                    else:
                        logging.error(f"API llamada API de ejecucion de regla de negocio fallida sk_id {task_id}: Status {response.status}")
        except Exception as e:
            logging.error(f"Error al llamar API de ejecucion de regla de negocio task_id {task_id}: {str(e)}")

    def run_etl_task(self, etl_request: ETLRequest, task_id: str) -> None:
        try:
            self.task_repository.set_task_status(task_id, "in process", {"idtask": task_id, "record_process": 0})            
            conn = get_db_ETL_connection()
            cursor = conn.cursor()
            current_time_str = datetime.now().strftime("%Y%m%d_%H%M")
            output_dir = os.path.join(os.getcwd(), "etl", current_time_str)
            os.makedirs(output_dir, exist_ok=True)

            csv_file_path = os.path.join(output_dir, f"registros_{task_id}.csv")            
            
            # Definir la query base con segmentación por hora/minuto
            base_query = """
            SELECT 
                lic.id_lic,
                com.marca_otorgamiento,
                lic.operador,
                lic.ccaf,
                lic.entidad_pagadora,
                lic.folio,
                lic.fecha_emision,
                lic.empleador_adscrito,
                lic.codigo_interno_prestador,
                lic.comuna_prestador,
                lic.fecha_ultimo_estado,
                lic.ultimo_estado,
                lic.rut_trabajador,
                lic.sexo_trabajador,
                lic.edad_trabajador,
                lic.tipo_reposo,
                lic.dias_reposo,
                lic.fecha_inicio_reposo,
                lic.comuna_reposo,
                lic.tipo_licencia,
                lic.rut_medico,
                lic.especialidad_profesional,
                lic.tipo_profesional,
                lic.zbtipo_licencia_entidad AS tipo_licencia_pronunciamiento,
                lic.zbcodigo_continuacion AS codigo_continuacion_pronunciamiento,
                lic.zbdias_autorizados AS dias_autorizados_pronunciamiento,
                lic.zbcodigo_diagnostico AS codigo_diagnostico_pronunciamiento,
                lic.zbcodigo_autorizacion AS codigo_autorizacion_pronunciamiento,
                lic.zbcausa_rechazo AS causa_rechazo_pronunciamiento,
                lic.zbtipo_reposo AS tipo_reposo_pronunciamiento,
                lic.zbderecho_a_subsidio AS derecho_a_subsidio_pronunciamiento,
                lic.rut_empleador,
                lic.calidad_trabajador,
                lic.actividad_laboral_trabajador,
                lic.ocupacion,
                lic.entidad_pagadora2 AS entidad_pagadora_zona_c,
                lic.fecha_recepcion_empleador,
                lic.regimen_previsional,
                lic.entidad_pagadora_subsidio,
                lic.comuna_laboral,
                lic.comuna_uso_compin,
                lic.cantidad_de_pronunciamientos,
                lic.cantidad_de_zonas_d,
                lic.secuencia_estados,
                lic.cod_diagnostico_principal,
                lic.cod_diagnostico_secundario,
                lic.periodo
            FROM lme.sabana_fiscalizador_lme lic
            LEFT JOIN lme.sabana_complementaria com
                ON lic.folio = com.folio AND lic.rut_trabajador = com.rut_trabajador
            WHERE lic.fecha_emision BETWEEN '{start_time}' AND '{end_time}';
            """
           
            #Ajustar las fechas para incluir el rango completo del dia, en el caso que el ETL solo sea de un dia particular, tambien sirve.
            start_date = datetime.strptime(f"{etl_request.start_date} 00:00:00", '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(f"{etl_request.end_date} 23:59:59", '%Y-%m-%d %H:%M:%S')
            
            current_time = start_date
            record_count = 0
            header_written = False
            
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
                        
                        with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=sabana_fiscalizador_lme_row.keys())
                            if not header_written:
                                writer.writeheader()
                                header_written = True
                            writer.writerow(sabana_fiscalizador_lme_row)
                        
                        self.upload_to_lm(sabana_fiscalizador_lme_row)
                    record_count += len(rows)
                    self.task_repository.set_task_status(task_id, "in process", {
                        "idtask": task_id, "record_process": record_count
                    })
                
                current_time = end_time

            # Se setea la ejecucion de Reglas
            self.task_repository.set_task_status(task_id, "execute_rn", {
                "idtask": task_id, "record_process": record_count
            })
            
            # API de ejecucion de REGLAS!
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.execute_rn_api_call(
                etl_request.start_date, 
                etl_request.end_date, 
                task_id
            ))
            loop.close()

            # Se notifica Fin de la Tarea
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
        session_especialidad = SessionML()
        
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
                SELECT id_especialidad_profesional FROM ml.especialidad_profesional
                WHERE descripcion_especialidad_profesional = :esp_descripcion
            """), {'esp_descripcion': esp_descripcion})
            id_especialidad = result.scalar()

            # Si no existe, insertarla
            if id_especialidad is None:
                session_especialidad.execute(text("""
                    INSERT INTO ml.especialidad_profesional (descripcion_especialidad_profesional)
                    VALUES (:esp_descripcion)
                    RETURNING id_especialidad_profesional
                """), {'esp_descripcion': esp_descripcion})
                id_especialidad = session_especialidad.execute(text("""
                    SELECT id_especialidad_profesional FROM ml.especialidad_profesional
                    WHERE descripcion_especialidad_profesional = :esp_descripcion
                """), {'esp_descripcion': esp_descripcion}).scalar()

            # Confirmar la transacción
            session_especialidad.commit()
            return id_especialidad

        except Exception as e:
            session_especialidad.rollback()
            logging.info(f"Error al crear o buscar especialidad: {e}")
            raise
        finally:
            session_especialidad.close()

    def create_profesionalidad(self, sabana_fiscalizador_lme_row):
        session = SessionML()
        
        try:
            # Determinar la descripción de la profesionalidad
            tipo_profesional = sabana_fiscalizador_lme_row['tipo_profesional']
            if tipo_profesional is None or tipo_profesional == '-' or str(tipo_profesional).strip() == '':
                descripcion = 'No informada'
            else:
                #TODO: Tema pendiente, no hay una descripcion del tipo profesional
                descripcion = str(tipo_profesional) 
            descripcion = descripcion.title()

            # Buscar si la profesionalidad ya existe
            result = session.execute(text("""
                SELECT id_profesionalidad FROM ml.profesionalidad
                WHERE descripcion_profesionalidad = :descripcion
            """), {'descripcion': descripcion})
            id_profesionalidad = result.scalar()

            # Si no existe, insertarla
            if id_profesionalidad is None:
                session.execute(text("""
                    INSERT INTO ml.profesionalidad (descripcion_profesionalidad)
                    VALUES (:descripcion)
                    RETURNING id_profesionalidad
                """), {'descripcion': descripcion})
                # Obtener el ID recién insertado
                result = session.execute(text("""
                    SELECT id_profesionalidad FROM ml.profesionalidad
                    WHERE descripcion_profesionalidad = :descripcion
                """), {'descripcion': descripcion})
                id_profesionalidad = result.scalar()

            # Confirmar la transacción
            session.commit()
            return id_profesionalidad

        except Exception as e:
            session.rollback()
            logging.info(f"Error al crear o buscar profesionalidad: {e}")
            raise
        finally:
            session.close()

    def setting_doctor(self, rut_medico, id_especialidad):
        if rut_medico is not None and id_especialidad is not None:
            if rut_medico in self.__medicos and id_especialidad in self.__especialidades:
                return
            
            session = SessionML() 
            try:
                # Consultar si el rut_medico ya existe en ml.medicos
                result_medico = session.execute(text("""
                    SELECT rut_medico 
                    FROM ml.medicos 
                    WHERE rut_medico = :rut_medico
                """), {'rut_medico': rut_medico}).fetchone()
                
                # Si no existe, insertar en ml.medicos
                if not result_medico:
                    session.execute(text("""
                        INSERT INTO ml.medicos (rut_medico)
                        VALUES (:rut_medico)
                    """), {'rut_medico': rut_medico})
                    session.commit()
                    logging.info(f"Insertado rut_medico {rut_medico} en ml.medicos")
                #else:
                #    logging.info(f"El rut_medico {rut_medico} ya existe en ml.medicos")

                # Consultar si la combinación id_especialidad y rut_medico ya existe en especialidad_profesional_medicos
                result_especialidad = session.execute(text("""
                    SELECT id_especialidad_profesional, rut_medico 
                    FROM ml.especialidad_profesional_medicos 
                    WHERE id_especialidad_profesional = :id_especialidad_profesional AND rut_medico = :rut_medico
                """), {'id_especialidad_profesional': id_especialidad, 'rut_medico': rut_medico}).fetchone()
                self.__medicos.add(rut_medico)
                # Si no existe, insertar en especialidad_profesional_medicos
                if not result_especialidad:
                    session.execute(text("""
                        INSERT INTO ml.especialidad_profesional_medicos (id_especialidad_profesional, rut_medico)
                        VALUES (:id_especialidad, :rut_medico)
                    """), {'id_especialidad': id_especialidad, 'rut_medico': rut_medico})
                    session.commit()
                    logging.info(f"Insertada especialidad {id_especialidad} para rut_medico {rut_medico}")
                #else:
                    #logging.info(f"La especialidad {id_especialidad} para rut_medico {rut_medico} ya existe")
                self.__especialidades.add(id_especialidad)
                    
            except IntegrityError as e:
                session.rollback()
                logging.info(f"Error de integridad: {e}")
            except Exception as e:
                session.rollback()
                logging.info(f"Error inesperado: {e}")
            finally:
                session.close()

    def upload_to_lm(self, sabana_fiscalizador_lme_row):
        id_especialidad = self.create_especialidad(sabana_fiscalizador_lme_row)
        rut_medico = sabana_fiscalizador_lme_row['rut_medico']
        self.setting_doctor(rut_medico, id_especialidad)
        
        session = SessionML()
        try:
            id_profesionalidad = self.create_profesionalidad(sabana_fiscalizador_lme_row)

            # Insertar en profesionalidad_medicos VALIDANDO REPLICAS
            if rut_medico is not None and id_profesionalidad is not None:
                session.execute(text("""
                    INSERT INTO ml.profesionalidad_medicos (id_profesionalidad, rut_medico)
                    VALUES (:id_profesionalidad, :rut_medico)
                    ON CONFLICT (id_profesionalidad, rut_medico) DO NOTHING
                """), {'id_profesionalidad': id_profesionalidad, 'rut_medico': rut_medico})

            # Validar id_lic en memoria antes de consultar la base de datos
            id_lic = sabana_fiscalizador_lme_row['id_lic']
            if id_lic not in self.__id_licencias:
                # Insertar en la tabla licencias VALIDANDO REPLICAS
                session.execute(text("""
                    INSERT INTO ml.licencias (
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
                        secuencia_estados, cod_diagnostico_principal, cod_diagnostico_secundario, periodo,
                        marca_otorgamiento
                    ) VALUES (
                        :id_lic, :operador, :ccaf, :entidad_pagadora, :folio, :fecha_emision, :empleador_adscrito,
                        :codigo_interno_prestador, :comuna_prestador, :fecha_ultimo_estado, :ultimo_estado,
                        :rut_trabajador, :sexo_trabajador, :edad_trabajador, :tipo_reposo, :dias_reposo,
                        :fecha_inicio_reposo, :comuna_reposo, :tipo_licencia, :rut_medico,
                        :tipo_licencia_pronunciamiento, :codigo_continuacion_pronunciamiento,
                        :dias_autorizados_pronunciamiento, :codigo_diagnostico_pronunciamiento,
                        :codigo_autorizacion_pronunciamiento, :causa_rechazo_pronunciamiento,
                        :tipo_reposo_pronunciamiento, :derecho_a_subsidio_pronunciamiento, :rut_empleador,
                        :calidad_trabajador, :actividad_laboral_trabajador, :ocupacion, :entidad_pagadora_zona_c,
                        :fecha_recepcion_empleador, :regimen_previsional, :entidad_pagadora_subsidio,
                        :comuna_laboral, :comuna_uso_compin, :cantidad_de_pronunciamientos, :cantidad_de_zonas_d,
                        :secuencia_estados, :cod_diagnostico_principal, :cod_diagnostico_secundario, :periodo,
                        :marca_otorgamiento
                    ) ON CONFLICT (id_lic) DO NOTHING
                """), sabana_fiscalizador_lme_row)
                # Agregar id_lic al arreglo privado
                self.__id_licencias.add(id_lic)
                logging.info(f"Procesado id_lic: {id_lic} - folio: {sabana_fiscalizador_lme_row['folio']}")
            else:
                logging.info(f"Atención id_lic: {id_lic} - folio: {sabana_fiscalizador_lme_row['folio']} duplicado en memoria, ignorado")
            self.save_diagnostico_especialidad(id_lic, sabana_fiscalizador_lme_row['cod_diagnostico_principal'], sabana_fiscalizador_lme_row['especialidad_profesional'])
                # print(id_lic)
            # Confirmar todas las inserciones y relaciones
            session.commit()

        except IntegrityError as e:
            session.rollback()
            logging.info(f"Error de integridad para id_lic: {sabana_fiscalizador_lme_row['id_lic']} - folio: {sabana_fiscalizador_lme_row['folio']}: {e}")
        except Exception as e:
            session.rollback()
            logging.info(f"Error inesperado para id_lic: {sabana_fiscalizador_lme_row['id_lic']} - folio: {sabana_fiscalizador_lme_row['folio']}: {e}")
            raise
        finally:
            session.close()

    def save_diagnostico_especialidad(self, id_lic, cod_diagnostico, especialidad_profesional):
        session = SessionML()

        try:
            result = session.execute(text("""
                SELECT 1 FROM ml.licencia_diagnostico_especialidad
                WHERE id_licencia = :id_lic
            """), {'id_lic': id_lic})

            existe = result.scalar()

            # Si no existe, insertar
            if not existe:
                session.execute(text("""
                    INSERT INTO ml.licencia_diagnostico_especialidad (
                        id_licencia,
                        cod_diagnostico,
                        especialidad_medico
                    ) VALUES (
                        :id_lic,
                        :cod_diagnostico,
                        :especialidad
                    )
                """), {
                    'id_lic': id_lic,
                    'cod_diagnostico': cod_diagnostico,
                    'especialidad': especialidad_profesional
                })

            # Confirmar transacción
            session.commit()

        except Exception as e:
            session.rollback()
            logging.info(f"Error al guardar diagnostico/especialidad para reglas de negocio - licencia {id_lic}: {e}")
            raise

        finally:
            session.close()