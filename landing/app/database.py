import psycopg2
from psycopg2.extras import RealDictCursor
from config import POSTGRESQL_CONFIG

def get_connection():
    """Получить подключение к БД"""
    return psycopg2.connect(**POSTGRESQL_CONFIG)

def get_cursor(conn):
    """Получить курсор с RealDictCursor"""
    return conn.cursor(cursor_factory=RealDictCursor)
