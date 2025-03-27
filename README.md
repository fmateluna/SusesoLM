 
uvicorn app.main:app --reload    
# Proyecto SUSESO

## Descripción

SUSESO es un sistema diseñado para gestionar y analizar licencias médicas electrónicas (LM) en Chile. Este proyecto centraliza información sobre licencias emitidas, diagnósticos, prestadores, y otros datos relevantes, permitiendo consultas rápidas y precisas a través de una API desarrollada en Python.

## Características principales

1. **Consulta de licencias por médico**: Filtrado de licencias emitidas por un profesional específico mediante su RUT.
2. **Análisis por diagnóstico y región**: Capacidad de segmentar y analizar datos con base en diagnósticos o ubicación.
3. **Detalles de licencias**: Provisión de información detallada sobre licencias individuales, como fecha de emisión, diagnóstico, y puntajes de probabilidad.
4. **Estructura modular**: Construcción basada en principios claros de diseño, con uso de Pydantic para validación de datos y una conexión fluida a bases de datos SQL.

## Tecnologías utilizadas

- **Python**: Lenguaje principal del proyecto.
- **Pydantic**: Para la definición y validación de modelos de datos.
- **SQL**: Consultas a través de scripts personalizados.
- **Bases de datos**: PostgreSQL o DuckDB para almacenamiento y consulta.


## Carga ETL

El endpoint es "/lm/etl"

El request seria asi:
{
    "start_date":"2025-02-14",
    "end_date":"2025-02-16"
}


**Estados de tarea ETL**

1) Iniciando : Aca la tarea esta empezando a cargar, previo a la ejecucion de la consulta
    Response :
    {
        "Status":"initial",
        "detail":{
            "idtask":"HASH(timestamp + definicion desde requests)"
        }
    }

2) En proceso : Aca la tarea se encuentra corriendo, es decir, aun esta desplegando data
    Response :
    {
        "Status":"in process",
        "detail":{
            "idtask":"HASH(timestamp + definicion desde requests)",
            "record_process":"10"
        }
    }

3) Error: Aca la tarea es interrumpida por cualquier tipo de error
    Response :
    {
        "Status":"error",
        "detail":{
            "idtask":"HASH(timestamp + definicion desde requests)",
            "record_process":"10",
            "id_error":"500",
            "message":"Internal Error"
        }
    }

4) Finalizado: La tarea a finalizado, entregando un resultado.
    Response:
    {
        "Status":"finish",
        "detail":{
            "idtask":"HASH(timestamp + definicion desde requests)",
            "record_process":"1010000"
        }
    }

Primero el usuario ingresa un request en el path o url "/lm/etl", ese ingresa un listado de tablas existente en el modelo de base de datos (Definicion de Base de datos cargado desde variables de sistema)

Segundo el sistema, lee las tablas, las agrega a la query, y empieza la ejecucion, asociando un hash a esta tarea la cual va retornada en el response con el valor "idtask":"HASH(timestamp + definicion desde requests)", el cual es un hash que identifica la peticion, si se ejecuta el mismo request, se hace el calculo del hash, para identificar la tarea y  para poder retonar los estados de esta, los cualees eran : initial, in process, error , finish

Tercero el endpoint puede recibir muchas veces el mismo request, pero gracias al calculo del hash se puede identificar si es la misma peticion y asi retonar al usuario el estado en el cual se encuentra

