# Easyfood/models.py
from django.db import models

class Consulta(models.Model):
    atributos = models.CharField(max_length=255)
    tablas = models.CharField(max_length=255)
    condiciones = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return f"SELECT {self.atributos} FROM {self.tablas} WHERE {self.condiciones}"
