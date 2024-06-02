import psycopg2
from psycopg2 import sql
from psycopg2 import Error


DB_HOST = 'pavlov.ing.puc.cl'
DB_PORT = '5432'
DB_USER = 'grupo64'
DB_PASSWORD = 'grupo64'
DB_NAME = 'grupo64e2'
archivo_csv = '../datos e2/suscripciones.csv'
archivo_csv2= '../datos e2/cldeldes.csv'

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

    drop_table_query = "DROP TABLE IF EXISTS Suscripciones;"
    cursor.execute(drop_table_query)
    print("Tabla clientes eliminada si existía")

    # Crear la tabla (ajusta esto según tu esquema de datos)
    crear_tabla_query = """
    CREATE TABLE IF NOT EXISTS Suscripciones (
    id int   NOT NULL,
    email varchar  NOT NULL,
    empresadespacho varchar NOT NULL,
    estado varchar  NOT NULL,
    ultimopago int   NOT NULL,
    fechaultimopago VARCHAR  NOT NULL,
    ciclofacturacion varchar   NOT NULL,
    precio INT   NOT NULL);""" 

    cursor.execute(crear_tabla_query)
    conexion.commit()

    # Preparar el query de inserción
    insercion_query = """
    INSERT INTO suscripciones(id, email, delivery, empresadespacho, estado, ultimopago, fechaultimopago, ciclofacturacion, precio)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    with open(archivo_csv2, 'r') as file:

        lista_clientes = []
        cliente_actual = ""
        next(file)
        for linea in file:
            valores = linea.strip().split(';')

            if len(valores) == 13:
                cliente = (valores[4],valores[8],valores[9])
                lista_clientes.append(cliente)

            elif len(valores) > 13:
                print(f"Línea malformada: {cliente_actual}")
                cliente_actual = ""



    
    #######################
 
    with open(archivo_csv, 'r') as file:
        contador = 0
        lista_sub = []
        next(file)

        # Iterar sobre cada línea en el archivo CSV
        for linea in file:
            # Dividir la línea en valores
            valores = linea.strip().split(';')

            if len(valores) != 6:
                print(f"La línea no tiene el número correcto de columnas: {linea}")
                continue
            
            delivery_found = False
            for delivery in lista_clientes:
                if delivery[0]==valores[1] and (len(valores) < 8):
                    if valores[5]== "mensual":
                        valores.append(delivery[1])
                    elif valores[5]== "anual":
                        valores.append(delivery[2])
                    delivery_found = True
                    break
                
            valores.insert(0, contador)
            id, email, delivery, empresadespacho, estado, fechaultimopago, ciclofacturacion, precio = valores


            lista_sub.append((id, email, delivery, empresadespacho, estado, fechaultimopago, ciclofacturacion, precio))
            contador = contador + 1
        
        for sub in lista_sub:
            try:
                cursor.execute(insercion_query, sub)
            except Error as e:
                    print(f"Error al insertar la línea {linea}: {e}")
                    conexion.rollback()
                    continue

    conexion.commit()

    delete_query = """
    DELETE FROM suscripciones
    WHERE email NOT IN (SELECT email FROM clientes);
    """

    cursor.execute(delete_query)
    conexion.commit()

    delete_query = """
    DELETE FROM suscripciones
    WHERE delivery NOT IN (SELECT nombre FROM EmpresaDespacho);
    """

    cursor.execute(delete_query)
    conexion.commit()


    fetch_cleaned_data_query = "SELECT DISTINCT email FROM suscripciones ;"
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

(379, 'Andres.Cortes@bbdduc.utopia', 'Speed', 'cancelada', '155520', '18-04-24', 'anual', 20952)