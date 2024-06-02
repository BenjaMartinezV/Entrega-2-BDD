import psycopg2
from psycopg2 import sql
from psycopg2 import Error


DB_HOST = 'pavlov.ing.puc.cl'
DB_PORT = '5432'
DB_USER = 'grupo64'
DB_PASSWORD = 'grupo64'
DB_NAME = 'grupo64e2'
archivo_csv = '../datos e2/pedidos2.csv'

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

    # Crear la tabla (ajusta esto según tu esquema de datos)
    crear_tabla_query = """
    CREATE TABLE IF NOT EXISTS Pedidos (
        id INT PRIMARY KEY,
        cliente VARCHAR,
        delivery VARCHAR,
        despachador VARCHAR,
        plato VARCHAR,
        fecha VARCHAR,
        hora VARCHAR(255) NOT NULL,
        estado VARCHAR(255) NOT NULL);
    """
    cursor.execute(crear_tabla_query)
    conexion.commit()

    # Preparar el query de inserción
    insercion_query = """
    INSERT INTO Pedidos (id, cliente, delivery, despachador, plato, fecha, hora, estado)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

 
    with open(archivo_csv, 'r') as file:
        
        next(file)

        # Iterar sobre cada línea en el archivo CSV
        for linea in file:
            # Dividir la línea en valores
            valores = linea.strip().split(';')
            lista_pedidos = []

            if len(valores) != 8:
                print(f"La línea no tiene el número correcto de columnas: {linea}")
                continue

            #id, cliente, delivery, despachador, plato, fecha, hora, estado = valores
            id = int(valores[0])
            cliente = valores[1]
            delivery = valores[2]
            despachador = valores[3]
            plato = valores[4]
            fecha = valores[5]
            hora = valores[6]
            estado = valores[7]

            lista_pedidos.append((id, cliente, delivery, despachador, plato, fecha, hora, estado))
        
        for pedido in lista_pedidos:
            try:
                cursor.execute(insercion_query, pedido)
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