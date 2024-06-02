import psycopg2
from psycopg2 import sql
from psycopg2 import Error


DB_HOST = 'pavlov.ing.puc.cl'
DB_PORT = '5432'
DB_USER = 'grupo64'
DB_PASSWORD = 'grupo64'
DB_NAME = 'grupo64e2'
archivo_csv = '../datos e2/pedidos2.csv'


# Ruta del archivo CSV
archivo_csv = '../datos e2/clientes.csv'

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
    drop_table_query = "DROP TABLE IF EXISTS clientes;"
    cursor.execute(drop_table_query)
    print("Tabla clientes eliminada si existía")

    # Crear la tabla (ajusta esto según tu esquema de datos)
    crear_tabla_query = """
    CREATE TABLE IF NOT EXISTS clientes (
        id SERIAL,
        nombre varchar   NOT NULL,
        email varchar   NOT NULL,
        telefono varchar   NOT NULL,
        clave varchar       NOT NULL,
        direccion varchar   NOT NULL,
        comuna varchar    NOT NULL,
        CONSTRAINT pk_Cliente PRIMARY KEY (
            id))
    """
    cursor.execute(crear_tabla_query)
    conexion.commit()

    # Preparar el query de inserción
    insercion_query = """
    INSERT INTO clientes (nombre, email, telefono, clave, direccion, comuna)
    VALUES (%s, %s, %s, %s, %s, %s);
    """

    # Leer y procesar el archivo CSV
    with open(archivo_csv, 'r') as file:
        lista_clientes = []
        cliente_actual = ""

        for linea in file:
            cliente_actual += linea.strip()
            valores = cliente_actual.split(';')
            if len(valores) == 6:
                lista_clientes.append(valores)
                cliente_actual = ""
            elif len(valores) > 6:
                print(f"Línea malformada: {cliente_actual}")
                cliente_actual = ""


        for cliente in lista_clientes:
            try:
                cursor.execute(insercion_query, (cliente[0], cliente[1], cliente[2], cliente[3], cliente[4], cliente[5]))
            except Error as e:
                    print(f"Error al insertar la línea {linea}: {e}")
                    conexion.rollback()
                    continue

    # Confirmar la inserción
    conexion.commit()

    fetch_cleaned_data_query = "SELECT * FROM Clientes"
    cursor.execute(fetch_cleaned_data_query)
    cleaned_data = cursor.fetchall()
    
    for row in cleaned_data:
        print(row)

except Error as e:
    print(f"Error de conexión o durante la operación en la base de datos: {e}")

finally:

    if cursor:
        cursor.close()
    if conexion:
        conexion.close()