import os
import time
import subprocess

def iniciar_contenedores():
    print("Iniciando contenedores Docker...")
    subprocess.run(["docker-compose", "up", "-d"], check=True)
    print("Contenedores Docker iniciados exitosamente.")

def deteniendo_contenedores():
    print("Iniciando contenedores Docker...")
    subprocess.run(["docker-compose", "down", "--volumes", "--remove-orphans"], check=True)
    print("Contenedores Docker iniciados exitosamente.")    

def esperar_postgres_listo():
    print("Esperando a que los servicios de PostgreSQL estén listos...")
    max_intentos = 24
    intento = 0
    while intento < max_intentos:
        try:
            subprocess.run(
                ["docker", "exec", "postgres1", "psql", "-U", "postgres", "-c", "SELECT 1"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            print("PostgreSQL en postgres1 está listo.")
            return True
        except subprocess.CalledProcessError:
            print("Esperando a que PostgreSQL en postgres1 esté listo...")
            time.sleep(5)
            intento += 1
    print("Tiempo de espera agotado para PostgreSQL en postgres1.")
    return False

def ejecutar_sql_en_contenedor(sql_file):
    print("Copiando script SQL al contenedor postgres1...")
    subprocess.run(["docker", "cp", sql_file, "postgres1:/tmp/p3.sql"], check=True)
    print("Ejecutando script SQL en el contenedor postgres1...")
    try:
        resultado = subprocess.run(
            ["docker", "exec", "postgres1", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-f", "/tmp/p3.sql"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        print(resultado.stdout)
        if resultado.stderr:
            print(resultado.stderr)
        consulta1 = subprocess.run(
            ["docker", "exec", "postgres1", "psql", "-U", "postgres", "-c",
             "SELECT Diagnóstico, COUNT(*) AS Total_Pacientes FROM Paciente GROUP BY Diagnóstico ORDER BY Diagnóstico"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        print("\nDistribución de pacientes por diagnóstico:")
        print(consulta1.stdout)
        consulta2 = subprocess.run(
            ["docker", "exec", "postgres1", "psql", "-U", "postgres", "-c",
             "SELECT DNI, Nombre, Ciudad, Diagnóstico, Edad, Peso, CASE WHEN Edad > 50 AND Peso > 80 THEN 'Alto riesgo' WHEN Edad > 50 OR Peso > 80 THEN 'Riesgo moderado' ELSE 'Bajo riesgo' END AS nivel_riesgo FROM Paciente WHERE Edad > 50 OR Peso > 80 ORDER BY Diagnóstico, nivel_riesgo;"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        print("\nPacientes en riesgo:")
        print(consulta2.stdout)
    except subprocess.CalledProcessError as e:
        print(e.stdout)
        print(e.stderr)

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file = os.path.join(script_dir, 'p3.sql')
    if not os.path.exists(sql_file):
        print(f"ERROR: El archivo SQL {sql_file} no existe.")
        return
    iniciar_contenedores()
    if not esperar_postgres_listo():
        print("No se pudo conectar al servidor PostgreSQL principal. Abortando.")
        subprocess.run(["docker-compose", "down"], check=True)
        return
    ejecutar_sql_en_contenedor(sql_file)
    print("Proceso completado. Los contenedores Docker siguen ejecutándose.")
    print("Detemiendo contenedores.")
    deteniendo_contenedores()

main()