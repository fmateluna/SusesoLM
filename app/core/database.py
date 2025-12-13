# app/core/database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Leer datos de conexión desde las variables de entorno (SQLAlchemy - ML)
DB_HOST = os.getenv("DB_ML_HOST")
DB_PORT = os.getenv("DB_ML_PORT", "5432")  # valor por defecto por si falta
DB_NAME = os.getenv("DB_ML_NAME")
DB_USER = os.getenv("DB_ML_USER")
DB_PASS = os.getenv("DB_ML_PASS")

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
    raise ValueError("Faltan variables de entorno obligatorias para la conexión ML a PostgreSQL")

# Crear URL de conexión a la base de datos
SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_pre_ping=True,                    
    pool_recycle=1800,                     
    connect_args={
        "keepalives": 1,                   
        "keepalives_idle": 30,             
        "keepalives_interval": 10,         
        "keepalives_count": 5              
    }
)

SessionML = sessionmaker(autocommit=False, autoflush=False, bind=engine)


import psycopg2

def get_db_ETL_connection():
    """
    Retorna una conexión directa con psycopg2 para tareas ETL pesadas.
    Incluye keepalives!!!!! para evitar cierres por timeout en consultas ETERNAAAAAAAS!!!.
    """
    return psycopg2.connect(
        host=os.getenv('DB_ETL_HOST'),
        port=os.getenv('DB_ETL_PORT', '5432'),
        dbname=os.getenv('DB_ETL_NAME'),
        user=os.getenv('DB_ETL_USER'),
        password=os.getenv('DB_ETL_PASS'),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )