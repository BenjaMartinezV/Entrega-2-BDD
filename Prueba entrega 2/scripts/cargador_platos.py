import psycopg2
from psycopg2 import sql
from psycopg2 import Error


DB_HOST = 'pavlov.ing.puc.cl'
DB_PORT = '5432'
DB_USER = 'grupo64'
DB_PASSWORD = 'grupo64'
DB_NAME = 'grupo64e2'
# Ruta del archivo CSV
archivo_csv_platos = '../datos e2/platos.csv'
archivo_csv_restaurantes= '../datos e2/restaurantes2.csv'

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
    drop_table_query = """
    DROP TABLE IF EXISTS platos;
    DROP TABLE IF EXISTS servido_en;
    """
    cursor.execute(drop_table_query)
    print("Tablas platos, y servido_en eliminada si existía")

    # Crear la tabla (ajusta esto según tu esquema de datos)
    crear_tabla_query = """
    CREATE TABLE IF NOT EXISTS platos (
        id INT NOT NULL,
        nombre varchar   NOT NULL,
        descripcion varchar NOT NULL,
        disponibilidad bool   NOT NULL,
        estilo varchar   NOT NULL,
        restriccion varchar  NOT NULL,
        ingredientes varchar NOT NULL,
        porciones INT   NOT NULL,
        tiempo INT NOT NULL,
        vigente bool   NOT NULL,
        CONSTRAINT pk_plato PRIMARY KEY (
            id))
    """

    crear_tabla_query_relation = """
    CREATE TABLE IF NOT EXISTS servido_en (
        restaurante varchar NOT NULL,
        id_plato INT   NOT NULL,
        precio INT NOT NULL)
    """
    cursor.execute(crear_tabla_query)
    conexion.commit()

    cursor.execute(crear_tabla_query_relation)
    conexion.commit()

    # Preparar el query de inserción
    insercion_query = """
    INSERT INTO platos (id, nombre, descripcion, disponibilidad, estilo, restriccion, ingredientes, porciones, tiempo, vigente)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    insercion_query_relation = """
    INSERT INTO servido_en (restaurante, id_plato, precio)
    VALUES (%s, %s, %s);
    """

    with open(archivo_csv_restaurantes, 'r') as file:
        lista_restaurante = []
        restaurante_actual = ""
        next(file)

        for linea in file:
            restaurante_actual += linea.strip()
            valores = restaurante_actual.split(';')
            if len(valores) == 8:
                lista_restaurante.append(valores)
                restaurante_actual = ""
            elif len(valores) > 8:
                print(f"Línea malformada: {restaurante_actual}")
                restaurante_actual = ""



        # Leer y procesar el archivo CSV
        with open(archivo_csv_platos, 'r') as file:
            lista_plato = []
            plato_actual = ""
            next(file)
            relations = []
            list_of_completed = []

            for linea in file:
                plato_actual += linea.strip()
                valores = plato_actual.split(';')
                if len(valores) == 13:
                    lista_plato.append(valores)
                    plato_actual = ""
                elif len(valores) > 13:
                    print(f"Línea malformada: {plato_actual}")
                    plato_actual = ""

            
            for plato in lista_plato:
                for restaurant in lista_restaurante:
                    if plato[10] == restaurant[0]:
                        id_plato = plato[0]
                        precio = plato[8]
                        restaurante = restaurant[0]
                        relations.append([restaurante, id_plato, precio])
                        
                        try:
                            for relation in relations:
                                if relation not in list_of_completed:
                                    cursor.execute(insercion_query_relation, (relation[0], int(relation[1]), int(relation[2])))
                                    list_of_completed.append(relation)
                        except Error as e:
                                print(f"Error al insertar la línea {linea}: {e}")
                                conexion.rollback()
                                continue
                try:
                    cursor.execute(insercion_query, (int(plato[0]), plato[1], plato[2], plato[3], plato[4], plato[5], plato[6], int(plato[7]), int(plato[9]),  plato[12]))
                except Error as e:
                        print(f"Error al insertar la línea {linea}: {e}")
                        conexion.rollback()
                        continue

    # Confirmar la inserción
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET nombre = REPLACE(nombre, 'Ã\xa0', 'a')
    WHERE nombre LIKE '%Ã\xa0%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET descripcion = REPLACE(descripcion, 'Ã±', 'ñ')
    WHERE descripcion LIKE '%Ã±%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET ingredientes = REPLACE(ingredientes, 'Ã±', 'ñ')
    WHERE ingredientes LIKE '%Ã±%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET nombre = REPLACE(nombre, 'Ã¡', 'a')
    WHERE nombre LIKE '%Ã¡%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET descripcion = REPLACE(descripcion, 'Ã¡', 'a')
    WHERE descripcion LIKE '%Ã¡%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET descripcion = REPLACE(descripcion, 'Ã³', 'o')
    WHERE descripcion LIKE '%Ã³%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET nombre = REPLACE( nombre , 'Ã³', 'o')
    WHERE  nombre  LIKE '%Ã³%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET ingredientes = REPLACE(ingredientes, 'Ã³', 'o')
    WHERE ingredientes LIKE '%Ã³%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET ingredientes = REPLACE(ingredientes, 'Ãº', 'u')
    WHERE ingredientes LIKE '%Ãº%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET descripcion = REPLACE(descripcion, 'Ãº', 'u')
    WHERE descripcion LIKE '%Ãº%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET nombre = REPLACE(nombre , 'Ãº', 'u')
    WHERE nombre  LIKE '%Ãº%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()


    update_nunoa_query = """
    UPDATE platos
    SET ingredientes = REPLACE(ingredientes, 'Ã©', 'e')
    WHERE ingredientes LIKE '%Ã©%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET descripcion = REPLACE(descripcion, 'Ã©', 'e')
    WHERE descripcion LIKE '%Ã©%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET nombre = REPLACE(nombre, 'Ã©', 'e')
    WHERE nombre LIKE '%Ã©%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    update_nunoa_query = """
    UPDATE platos
    SET ingredientes = REPLACE(ingredientes, 'Ã¡', 'a')
    WHERE ingredientes LIKE '%Ã¡%';
    """
    cursor.execute(update_nunoa_query)
    conexion.commit()

    fetch_cleaned_data_query = "SELECT * FROM platos;"
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