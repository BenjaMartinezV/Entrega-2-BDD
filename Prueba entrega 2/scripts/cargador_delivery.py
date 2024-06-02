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

    drop_table_query = "DROP TABLE IF EXISTS EmpresaDespacho;"
    cursor.execute(drop_table_query)
    print("Tabla clientes eliminada si existía")

    # Crear la tabla (ajusta esto según tu esquema de datos)
    crear_tabla_query = """
    CREATE TABLE IF NOT EXISTS tempEmpresaDespacho (
    id INT  NOT NULL,
    nombre VARCHAR  NOT NULL,
    vigencia BOOL   NOT NULL,
    telefono VARCHAR   NOT NULL);""" 

    cursor.execute(crear_tabla_query)
    conexion.commit()

    # Preparar el query de inserción
    insercion_query = """
    INSERT INTO tempEmpresaDespacho(id, nombre, vigencia, telefono)
    VALUES (%s, %s, %s, %s);
    """
    
    #######################
 
    with open(archivo_csv, 'r') as file:
        contador = 0
        next(file)

        lista_clientes = []
        lista_sub = []
        cliente_actual = ""

        # Iterar sobre cada línea en el archivo CSV
        for linea in file:
            # Dividir la línea en valores
            valores = linea.strip().split(';')

            if len(valores) == 13:
                datos =[]
                datos.append(contador)
                datos.append(valores[4])
                datos.append(valores[5])
                datos.append(valores[6])
                lista_clientes.append(datos)

            elif len(valores) > 13:
                print(f"Línea malformada: {cliente_actual}")
                cliente_actual = ""

            id, nombre, vigencia, telefono = datos
            print(datos)


            lista_sub.append((id, nombre, vigencia, telefono))
            contador = contador + 1
        
        for sub in lista_sub:
            try:
                cursor.execute(insercion_query, sub)
            except Error as e:
                    print(f"Error al insertar la línea {linea}: {e}")
                    conexion.rollback()
                    continue
    conexion.commit()

    ##################LIMPIEZA
    clean_duplicates_query = """
    DELETE FROM tempEmpresaDespacho a
    USING tempEmpresaDespacho b
    WHERE a.ctid < b.ctid
    AND a.nombre = b.nombre
    AND a.vigencia = b.vigencia
    AND a.telefono = b.telefono;
    """
    cursor.execute(clean_duplicates_query)
    conexion.commit()

    create_temp_table_query = """
    CREATE TABLE EmpresaDespacho (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR NOT NULL,
    vigencia BOOL NOT NULL,
    telefono VARCHAR NOT NULL
    );
    """
    cursor.execute(create_temp_table_query)

    #poner la data limpia
    insert_temp_table_query = """
    INSERT INTO EmpresaDespacho (nombre, vigencia, telefono)
    SELECT nombre, vigencia, telefono FROM tempEmpresaDespacho;
    """
    cursor.execute(insert_temp_table_query)
    conexion.commit()


    fetch_cleaned_data_query = "SELECT * FROM EmpresaDespacho;"
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