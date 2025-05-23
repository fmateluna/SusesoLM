# app/core/database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Leer datos de conexión desde las variables de entorno
DB_HOST = os.getenv("DB_ML_HOST")
DB_PORT = os.getenv("DB_ML_PORT")
DB_NAME = os.getenv("DB_ML_NAME")
DB_USER = os.getenv("DB_ML_USER")
DB_PASS = os.getenv("DB_ML_PASS")

# Crear URL de conexión a la base de datos
SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Crear el motor de la base de datos
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_size=10, max_overflow=20, pool_timeout=30)

# Crear una sesión para interactuar con la base de datos
SessionML = sessionmaker(autocommit=False, autoflush=False, bind=engine)

import psycopg2
from os import environ

def get_db_ETL_connection():
    return psycopg2.connect(
        host=environ    ['DB_ETL_HOST'],
        port=environ    ['DB_ETL_PORT'],
        dbname=environ  ['DB_ETL_NAME'],
        user=environ    ['DB_ETL_USER'],
        password=environ['DB_ETL_PASS']
    )