CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE SERVER servidor_cardiovascular FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'postgres2', dbname 'postgres', port '5432');
CREATE SERVER servidor_metabolico    FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'postgres3', dbname 'postgres', port '5432');

CREATE USER MAPPING FOR CURRENT_USER SERVER servidor_cardiovascular OPTIONS (user 'postgres', password '123456');
CREATE USER MAPPING FOR CURRENT_USER SERVER servidor_metabolico    OPTIONS (user 'postgres', password '123456');

CREATE SCHEMA IF NOT EXISTS cardiovascular_schema;
CREATE SCHEMA IF NOT EXISTS metabolico_schema;

IMPORT FOREIGN SCHEMA public FROM SERVER servidor_cardiovascular INTO cardiovascular_schema;
IMPORT FOREIGN SCHEMA public FROM SERVER servidor_metabolico    INTO metabolico_schema;

DROP TABLE IF EXISTS Paciente CASCADE;

CREATE TABLE Paciente (
    DNI CHAR(8) NOT NULL,
    Nombre VARCHAR(100),
    Ciudad VARCHAR(50),
    Diagnóstico VARCHAR(50),
    Peso DECIMAL(5,2),
    Edad INTEGER CHECK (Edad >= 0),
    Sexo CHAR(1) CHECK (Sexo IN ('M','F'))
) PARTITION BY LIST (Diagnóstico);

CREATE TABLE paciente_migrana   PARTITION OF Paciente FOR VALUES IN ('Migraña');
CREATE TABLE paciente_depresion PARTITION OF Paciente FOR VALUES IN ('Depresión');
CREATE TABLE paciente_anemia    PARTITION OF Paciente FOR VALUES IN ('Anemia');

ALTER TABLE paciente_migrana   ADD PRIMARY KEY (DNI, Diagnóstico);
ALTER TABLE paciente_depresion ADD PRIMARY KEY (DNI, Diagnóstico);
ALTER TABLE paciente_anemia    ADD PRIMARY KEY (DNI, Diagnóstico);

DO $$
BEGIN
    PERFORM dblink_exec('host=postgres2 dbname=postgres user=postgres password=123456',
        'CREATE TABLE IF NOT EXISTS paciente_cardiopatia (DNI CHAR(8), Nombre VARCHAR(100), Ciudad VARCHAR(50), Diagnóstico VARCHAR(50), Peso DECIMAL(5,2), Edad INTEGER, Sexo CHAR(1), PRIMARY KEY (DNI, Diagnóstico))');
    PERFORM dblink_exec('host=postgres2 dbname=postgres user=postgres password=123456',
        'CREATE TABLE IF NOT EXISTS paciente_hipertension (DNI CHAR(8), Nombre VARCHAR(100), Ciudad VARCHAR(50), Diagnóstico VARCHAR(50), Peso DECIMAL(5,2), Edad INTEGER, Sexo CHAR(1), PRIMARY KEY (DNI, Diagnóstico))');
END$$;

DO $$
BEGIN
    PERFORM dblink_exec('host=postgres3 dbname=postgres user=postgres password=123456',
        'CREATE TABLE IF NOT EXISTS paciente_diabetes (DNI CHAR(8), Nombre VARCHAR(100), Ciudad VARCHAR(50), Diagnóstico VARCHAR(50), Peso DECIMAL(5,2), Edad INTEGER, Sexo CHAR(1), PRIMARY KEY (DNI, Diagnóstico))');
    PERFORM dblink_exec('host=postgres3 dbname=postgres user=postgres password=123456',
        'CREATE TABLE IF NOT EXISTS paciente_obesidad (DNI CHAR(8), Nombre VARCHAR(100), Ciudad VARCHAR(50), Diagnóstico VARCHAR(50), Peso DECIMAL(5,2), Edad INTEGER, Sexo CHAR(1), PRIMARY KEY (DNI, Diagnóstico))');
    PERFORM dblink_exec('host=postgres3 dbname=postgres user=postgres password=123456',
        'CREATE TABLE IF NOT EXISTS paciente_otros (DNI CHAR(8), Nombre VARCHAR(100), Ciudad VARCHAR(50), Diagnóstico VARCHAR(50), Peso DECIMAL(5,2), Edad INTEGER, Sexo CHAR(1), PRIMARY KEY (DNI, Diagnóstico))');
END$$;

CREATE FOREIGN TABLE paciente_cardiopatia   PARTITION OF Paciente FOR VALUES IN ('Cardiopatía')  SERVER servidor_cardiovascular OPTIONS (schema_name 'public', table_name 'paciente_cardiopatia');
CREATE FOREIGN TABLE paciente_hipertension PARTITION OF Paciente FOR VALUES IN ('Hipertensión') SERVER servidor_cardiovascular OPTIONS (schema_name 'public', table_name 'paciente_hipertension');
CREATE FOREIGN TABLE paciente_diabetes     PARTITION OF Paciente FOR VALUES IN ('Diabetes')     SERVER servidor_metabolico    OPTIONS (schema_name 'public', table_name 'paciente_diabetes');
CREATE FOREIGN TABLE paciente_obesidad     PARTITION OF Paciente FOR VALUES IN ('Obesidad')     SERVER servidor_metabolico    OPTIONS (schema_name 'public', table_name 'paciente_obesidad');
CREATE FOREIGN TABLE paciente_otros        PARTITION OF Paciente DEFAULT                        SERVER servidor_metabolico    OPTIONS (schema_name 'public', table_name 'paciente_otros');

CREATE OR REPLACE PROCEDURE insertar_paciente(
    p_dni CHAR(8),
    p_nombre VARCHAR(100),
    p_ciudad VARCHAR(50),
    p_diagnostico VARCHAR(50),
    p_peso DECIMAL(5,2),
    p_edad INTEGER,
    p_sexo CHAR(1)
)
LANGUAGE plpgsql
AS $$
DECLARE
    partition_exists BOOLEAN;
    partition_name TEXT;
    remote_server TEXT;
    conn_string TEXT;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_inherits i ON c.oid = i.inhrelid
        JOIN pg_class p ON i.inhparent = p.oid
        WHERE p.relname = 'paciente'
        AND c.relname = unaccent(lower('paciente_' || replace(p_diagnostico,' ','_')))
    ) INTO partition_exists;

    IF NOT partition_exists THEN
        IF p_diagnostico IN ('Cardiopatía','Hipertensión','Angina','Arritmia') THEN
            remote_server := 'servidor_cardiovascular';
            conn_string := 'host=postgres2 dbname=postgres user=postgres password=123456';
        ELSIF p_diagnostico IN ('Diabetes','Obesidad','Hipotiroidismo','Hipertiroidismo') THEN
            remote_server := 'servidor_metabolico';
            conn_string := 'host=postgres3 dbname=postgres user=postgres password=123456';
        ELSE
            remote_server := '';
        END IF;

        partition_name := unaccent(lower('paciente_' || replace(p_diagnostico,' ','_')));

        IF remote_server <> '' THEN
            PERFORM dblink_exec(conn_string,
                format('CREATE TABLE IF NOT EXISTS %I (DNI CHAR(8), Nombre VARCHAR(100), Ciudad VARCHAR(50), Diagnóstico VARCHAR(50), Peso DECIMAL(5,2), Edad INTEGER, Sexo CHAR(1), PRIMARY KEY (DNI, Diagnóstico))', partition_name));
            EXECUTE format('CREATE FOREIGN TABLE %I PARTITION OF Paciente FOR VALUES IN (%L) SERVER %I OPTIONS (schema_name ''public'', table_name %L)',
                partition_name, p_diagnostico, remote_server, partition_name);
        ELSE
            EXECUTE format('CREATE TABLE %I PARTITION OF Paciente FOR VALUES IN (%L)', partition_name, p_diagnostico);
            EXECUTE format('ALTER TABLE %I ADD PRIMARY KEY (DNI, Diagnóstico)', partition_name);
        END IF;
    END IF;

    INSERT INTO Paciente VALUES (p_dni, p_nombre, p_ciudad, p_diagnostico, p_peso, p_edad, p_sexo);
END$$;


CALL insertar_paciente('11111111','Juan Pérez','Lima','Migraña',75.50,35,'M');
CALL insertar_paciente('22222222','María López','Arequipa','Depresión',62.30,40,'F');
CALL insertar_paciente('33333333','Carlos Rodríguez','Trujillo','Obesidad',80.10,45,'M');
CALL insertar_paciente('44444444','Ana Torres','Lima','Anemia',55.20,28,'F');
CALL insertar_paciente('55555555','Roberto Gómez','Cusco','Cardiopatía',70.80,50,'M');
CALL insertar_paciente('66666666','Diana Ramírez','Piura','Diabetes',65.40,38,'F');
CALL insertar_paciente('77777777','Eduardo Castro','Tacna','Hipertensión',82.60,55,'M');
CALL insertar_paciente('88888888','Carmen Navarro','Iquitos','Artritis',59.70,42,'F');
CALL insertar_paciente('45781236','Carla María Romero Díaz','Lima','Diabetes',70.00,45,'F');
CALL insertar_paciente('08569321','Luis Alberto Díaz Mendoza','Lima','Hipertensión',85.00,60,'M');
CALL insertar_paciente('72103654','Ana Paula Torres Castro','Callao','Obesidad',90.00,35,'F');
CALL insertar_paciente('25963147','Jorge Luis Ramírez Vargas','Callao','Cardiopatía',78.00,50,'M');
CALL insertar_paciente('15478962','María Carmen Suárez López','Lima','Diabetes',65.00,42,'F');
CALL insertar_paciente('36987412','Pedro José Quispe Huamán','Lima','Obesidad',95.00,38,'M');
CALL insertar_paciente('65412398','Rosa Isabel Valle García','Lima','Hipertensión',72.00,55,'F');
CALL insertar_paciente('89632147','Miguel Ángel Castro Rivas','Callao','Cardiopatía',82.00,48,'M');
CALL insertar_paciente('12345678','Carmen Rosa Mendoza Silva','Arequipa','Diabetes',68.50,52,'F');
CALL insertar_paciente('23456789','Roberto Carlos Chávez López','Arequipa','Hipertensión',88.00,58,'M');
CALL insertar_paciente('34567890','Patricia Elena Flores Vargas','Arequipa','Obesidad',92.30,41,'F');
CALL insertar_paciente('45678901','Fernando José Morales Ruiz','Arequipa','Cardiopatía',75.80,47,'M');
CALL insertar_paciente('56789012','Mónica Isabel Reyes Castro','Trujillo','Diabetes',63.20,39,'F');
CALL insertar_paciente('67890123','Andrés Manuel Torres Díaz','Trujillo','Hipertensión',91.50,62,'M');
CALL insertar_paciente('78901234','Claudia Beatriz Herrera Ramos','Trujillo','Obesidad',87.60,36,'F');
CALL insertar_paciente('89012345','Ricardo Augusto Vega Paredes','Trujillo','Cardiopatía',80.40,53,'M');
CALL insertar_paciente('90123456','Esperanza Lucía Quispe Mamani','Cusco','Diabetes',59.80,44,'F');
CALL insertar_paciente('01234567','Julio César Condori Huanca','Cusco','Hipertensión',77.90,56,'M');
CALL insertar_paciente('11223344','Marisol Yolanda Ccopa Choque','Cusco','Obesidad',94.20,43,'F');
CALL insertar_paciente('22334455','Víctor Hugo Mamani Flores','Cusco','Cardiopatía',73.50,49,'M');
CALL insertar_paciente('33445566','Gladys Rosario Pinedo Vásquez','Iquitos','Diabetes',66.70,40,'F');
CALL insertar_paciente('44556677','Oswaldo Enrique Tello Ramírez','Iquitos','Hipertensión',83.10,59,'M');
CALL insertar_paciente('55667788','Roxana Pilar Ruiz Gonzales','Iquitos','Obesidad',89.40,37,'F');
CALL insertar_paciente('66778899','Edinson Raúl Fasabi Macedo','Iquitos','Cardiopatía',76.30,51,'M');
CALL insertar_paciente('77889900','Elena Beatriz Sánchez Rivera','Piura','Asma',62.50,33,'F');
CALL insertar_paciente('88990011','Carlos Eduardo Paredes Luna','Chiclayo','Gastritis',74.20,28,'M');
CALL insertar_paciente('99001122','Patricia Morales Vega','Tacna','Artritis',68.30,48,'F');

EXPLAIN ANALYZE
SELECT
    Diagnóstico,
    Ciudad,
    COUNT(*) AS cantidad_pacientes,
    AVG(Peso) AS peso_promedio
FROM Paciente
GROUP BY Diagnóstico, Ciudad
ORDER BY Diagnóstico, Ciudad;

EXPLAIN ANALYZE

SELECT
    DNI,
    Nombre,
    Ciudad,
    Diagnóstico,
    Edad,
    Peso,
    CASE
        WHEN Edad > 50 AND Peso > 80 THEN 'Alto riesgo'
        WHEN Edad > 50 OR  Peso > 80 THEN 'Riesgo moderado'
        ELSE                                'Bajo riesgo'
    END AS nivel_riesgo
FROM Paciente
WHERE Edad > 50 OR Peso > 80
ORDER BY Diagnóstico, nivel_riesgo;
