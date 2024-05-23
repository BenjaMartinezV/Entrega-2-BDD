import psycopg2
from psycopg2 import sql
from psycopg2 import Error
from consultas import (consulta1, consulta5, consulta8, consulta10)

#para conectarse a la base
DB_HOST = 'pavlov.ing.puc.cl'
DB_PORT = '5432'
DB_USER = 'grupo64'
DB_PASSWORD = 'grupo64'
DB_NAME = 'grupo64e2'

