from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),  # Example URL pattern
    path('consultas/', views.consultas, name='consultas'),
    # Add more app-specific URL patterns for views
]