import psycopg2
from psycopg2 import sql
from psycopg2 import Error

DB_HOST = 'pavlov.ing.puc.cl'
DB_PORT = '5432'
DB_USER = 'grupo64'
DB_PASSWORD = 'grupo64'
DB_NAME = 'grupo64e2'

# Ruta del archivo CSV
archivo_csv = '../datos e2/restaurantes2.csv'

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
    drop_table_query = "DROP TABLE IF EXISTS restaurantes;"
    cursor.execute(drop_table_query)
    print("Tabla restaurantes eliminada si existía")

    # Crear la tabla (ajusta esto según tu esquema de datos)
    crear_tabla_query = """
    CREATE TABLE IF NOT EXISTS restaurantes (
        id SERIAL,
        nombre varchar   NOT NULL,
        vigente bool   NOT NULL,
        estilo varchar   NOT NULL,
        repartomin int  NOT NULL,
        sucursal varchar NOT NULL,
        direccion varchar   NOT NULL,
        telefono varchar    NOT NULL,
        area varchar NOT NULL,
        CONSTRAINT pk_restaurante PRIMARY KEY (
            id))
    """
    cursor.execute(crear_tabla_query)
    conexion.commit()

    # Preparar el query de inserción
    insercion_query = """
    INSERT INTO restaurante (nombre, vigente, estilo, repartomin, sucursal, direccion, telefono, area)
    VALUES (%s, %s, %s, %s, %s, %s);
    """

    # Leer y procesar el archivo CSV
    with open(archivo_csv, 'r') as file:
        lista_restaurant = []
        restaurant_actual = ""
        next(file)

        for linea in file:
            restaurant_actual += linea.strip()
            valores = restaurant_actual.split(';')
            if len(valores) == 8:
                lista_restaurant.append(valores)
                restaurant_actual = ""
            elif len(valores) > 8:
                print(f"Línea malformada: {restaurant_actual}")
                restaurant_actual = ""

        for restaurant in lista_restaurant:
            try:
                cursor.execute(insercion_query, (restaurant[0], restaurant[1], restaurant[2], int(restaurant[3]), restaurant[4], restaurant[5], restaurant[6], restaurant[7]))
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