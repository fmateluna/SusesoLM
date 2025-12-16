# app/core/database.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DB_HOST = os.getenv("DB_ML_HOST")
DB_PORT = os.getenv("DB_ML_PORT", "5432")
DB_NAME = os.getenv("DB_ML_NAME")
DB_USER = os.getenv("DB_ML_USER")
DB_PASS = os.getenv("DB_ML_PASS")

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
    raise ValueError(
        "Faltan variables de entorno obligatorias para la conexión ML a PostgreSQL"
    )

SQLALCHEMY_DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


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
        "keepalives_count": 5,

        "options": (
            "-c statement_timeout=0 "
            "-c idle_in_transaction_session_timeout=0"
        )
    }
)

SessionML = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

engine_etl = create_engine(
    SQLALCHEMY_DATABASE_URL,
    poolclass=None,  # 🔥 SIN POOL
    connect_args={
        "options": (
            "-c statement_timeout=0 "
            "-c idle_in_transaction_session_timeout=0"
        )
    }
)

SessionETL = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine_etl
)

def get_db_ETL_connection():
    """
    Retorna una conexión directa psycopg2 para ETL largos.
    - Sin statement_timeout
    - Sin idle_in_transaction_session_timeout
    - Con keepalives TCP
    """

    conn = psycopg2.connect(
        host=os.getenv("DB_ETL_HOST"),
        port=os.getenv("DB_ETL_PORT", "5432"),
        dbname=os.getenv("DB_ETL_NAME"),
        user=os.getenv("DB_ETL_USER"),
        password=os.getenv("DB_ETL_PASS"),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout TO 0;")
        cur.execute("SET idle_in_transaction_session_timeout TO 0;")

    return conn