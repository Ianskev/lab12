# Laboratorio 12 - Base de Datos Distribuidas

Este proyecto implementa un sistema de bases de datos distribuidas usando PostgreSQL en contenedores Docker.

## Requisitos previos

- Docker
- Docker Compose
- Python 3.x

## Cómo ejecutar el proyecto

1. Asegúrate de tener todos los archivos en la misma carpeta:
   - `docker-compose.yml`
   - `p3.sql`
   - `p3.py`

2. Abre una terminal en la ubicación de los archivos

3. Ejecuta el script Python:
   ```
   python p3.py
   ```

## ¿Qué hace el script?

El script `p3.py` realiza las siguientes operaciones:
- Inicia los contenedores Docker con PostgreSQL
- Espera a que los servicios de PostgreSQL estén listos
- Ejecuta el script SQL (p3.sql) que configura la base de datos distribuida
- Muestra estadísticas de los pacientes por diagnóstico 
- Muestra información de pacientes en riesgo
- Finalmente detiene y elimina los contenedores

Al finalizar la ejecución, podrás ver en la terminal los resultados de las consultas.
