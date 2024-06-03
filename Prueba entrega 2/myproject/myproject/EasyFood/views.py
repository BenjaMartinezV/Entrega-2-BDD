from django.shortcuts import render
from django.db import connection
from .forms import ConsultaForm

# Vista para la página principal
def home(request):
    return render(request, 'home.html')

# Vista para mostrar el formulario de consultas
def consultas(request):
    error_message = ""
    consultas = ""
    if request.method == 'POST':
        form = ConsultaForm(request.POST)
        if form.is_valid():
            consulta = form.save(commit=False)
            sql = f"SELECT {consulta.atributos} FROM {consulta.tablas}"
            if consulta.condiciones:
                sql += f" WHERE {consulta.condiciones}"
            print(f"Executing SQL query: {sql}")
            try:
                resultados = ejecutar_consulta(sql)
                return render(request, 'resultados.html', {'resultados': resultados})
            except Exception as e:
                error_message = f"Error en la consulta: {str(e)}"
                consultas += f"Consulta fallida: Atributos - {consulta.atributos}, Tablas - {consulta.tablas}, Condiciones - {consulta.condiciones}\n"
        else:
            error_message = "Formulario inválido. Por favor, verifica los campos."
    else:
        form = ConsultaForm()
    return render(request, 'consultas.html', {'form': form, 'error_message': error_message, 'consultas': consultas})
# Función para ejecutar la consulta SQL
def ejecutar_consulta(sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return [columns, rows]
