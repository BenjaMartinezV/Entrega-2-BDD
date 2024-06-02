import psycopg2
from psycopg2 import sql
from psycopg2 import Error


DB_HOST = 'pavlov.ing.puc.cl'
DB_PORT = '5432'
DB_USER = 'grupo64'
DB_PASSWORD = 'grupo64'
DB_NAME = 'grupo64e2'
archivo_csv= '../datos e2/cldeldes.csv'

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
    print(cursor)

    drop_table_query = "DROP TABLE IF EXISTS ServicioDespacho;"
    cursor.execute(drop_table_query)
    print("Tabla ServicioDespacho eliminada si existía")

    # Crear la tabla (ajusta esto según tu esquema de datos)
    crear_tabla_query = """
    CREATE TABLE IF NOT EXISTS ServicioDespacho(
    id INT  NOT NULL,
    costo INT  NOT NULL,
    tiempo INT  NOT NULL);""" 

    cursor.execute(crear_tabla_query)
    conexion.commit()

    # Preparar el query de inserción
    insercion_query = """
    INSERT INTO ServicioDespacho(id, costo, tiempo)
    VALUES (%s, %s, %s);
    """
    
    #######################
 
    with open(archivo_csv, 'r') as file:
        contador = 0
        next(file)

        lista_clientes = []
        cliente_actual = ""
        lista_sub = []

        # Iterar sobre cada línea en el archivo CSV
        for linea in file:
            # Dividir la línea en valores
            valores = linea.strip().split(';')

            if len(valores) == 13:
                datos =[]
                datos.append(contador)
                datos.append(valores[8])
                datos.append(valores[7])
                lista_clientes.append(datos)

            elif len(valores) > 13:
                print(f"Línea malformada: {cliente_actual}")
                cliente_actual = ""

            id, costo, tiempo = datos
            print(datos)


            lista_sub.append((id, costo, tiempo))
            contador = contador + 1
            
        for sub in lista_sub:
            try:
                cursor.execute(insercion_query, sub)
            except Error as e:
                    print(f"Error al insertar la línea {linea}: {e}")
                    conexion.rollback()
                    continue

    conexion.commit()

except Error as e:
    print(f"Error de conexión o durante la operación en la base de datos: {e}")

finally:

    if cursor:
        cursor.close()
    if conexion:
        conexion.close()