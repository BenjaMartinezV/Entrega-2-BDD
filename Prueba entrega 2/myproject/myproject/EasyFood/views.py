from django.shortcuts import render

# Create your views here.
def home(request):
    return render(request, 'home.html')

def consultas(request):
    return render(request, 'consultas.html')