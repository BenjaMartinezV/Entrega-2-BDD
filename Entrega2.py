import psycopg2
import json
from flask import Flask, request

app = Flask(__name__)

queries = [
    "INSERT INTO table1...",
    "INSERT INTO table2...",
    "INSERT INTO table3...",
    "INSERT INTO table4...",
    "INSERT INTO table5...",
    "INSERT INTO table6...",
    "INSERT INTO table7...",
    "INSERT INTO table8...",
    "INSERT INTO table9...",
    "INSERT INTO table10...",
    "INSERT INTO table11...",
    "INSERT INTO table12...",
    "INSERT INTO table13...",
]

try:
    conn = psycopg2.connect(
        database="entrega2",
        user="grupo64",
        host="localhost",
        port=5432,
        password="grupo64")
except:
    print("No se pudo conectar a la base de datos")

cur = conn.cursor()
for query in queries:
    try:
        cur.execute(query)
    except psycopg2.Error as e:
        print(e.pgerror)

@app.route('/query', methods=['GET'])
def query():
    A = request.args.get('A')
    T = request.args.get('T')
    C = request.args.get('C')

    if A is None or T is None or C is None:
        return "Error: Faltan parametros", 400
    
    cur = conn.cursor()
    cur.execute(f"SELECT {A} FROM {T} WHERE {C}")
    result = [[str(item) for item in row] for row in result]

    return json.dumps(result), 200

if __name__ == '__main__':
    app.run()


