import psycopg2
from psycopg2 import sql
from psycopg2 import Error

DB_HOST = 'pavlov.ing.puc.cl'
DB_PORT = '5432'
DB_USER = 'grupo64'
DB_PASSWORD = 'grupo64'
DB_NAME = 'grupo64e2'

# Ruta del archivo CSV
archivo_csv = '../datos e2/calificacion.csv'

# Configuración de la conexión
try:
    conexion = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    # Crear un cursor
    cursor = conexion.cursor()

    # Dropear la tabla si ya existe
    drop_table_query = "DROP TABLE IF EXISTS calificacion;"
    cursor.execute(drop_table_query)
    print("Tabla calificacion eliminada si existía")

    # Crear la tabla (ajusta esto según tu esquema de datos)
    crear_tabla_query = """
    CREATE TABLE IF NOT EXISTS calificacion (
        id SERIAL,
        id_pedido INT  NOT NULL,
        ev_restaurant INT  NOT NULL,
        ev_cliente INT   NOT NULL,
        CONSTRAINT pk_cal PRIMARY KEY (
            id))
    """
    cursor.execute(crear_tabla_query)
    conexion.commit()

    # Preparar el query de inserción
    insercion_query = """
    INSERT INTO calificacion (id_pedido, ev_restaurant, ev_cliente)
    VALUES (%s, %s, %s);
    """

    # Leer y procesar el archivo CSV
    with open(archivo_csv, 'r') as file:
        lista_calificacion = []
        cal_actual = ""
        next(file)
        
        for linea in file:
            cal_actual += linea.strip()
            valores = cal_actual.split(';')
            if len(valores) == 3:
                lista_calificacion.append(valores)
                cal_actual = ""
            elif len(valores) > 3:
                print(f"Línea malformada: {cal_actual}")
                cal_actual = ""


        for cal in lista_calificacion:
            try:
                cursor.execute(insercion_query, (cal[0], cal[1], cal[2]))
            except Error as e:
                    print(f"Error al insertar la línea {linea}: {e}")
                    conexion.rollback()
                    continue

    # Confirmar la inserción
    conexion.commit()

except Error as e:
    print(f"Error de conexión o durante la operación en la base de datos: {e}")

finally:

    if cursor:
        cursor.close()
    if conexion:
        conexion.close()