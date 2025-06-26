-- Creación de la tabla Paciente con particionamiento en 4 fragmentos por diagnóstico
DROP TABLE IF EXISTS Paciente CASCADE;
CREATE TABLE Paciente (
    DNI CHAR(8),
    Nombre VARCHAR(100) NOT NULL,
    Ciudad VARCHAR(50) NOT NULL,
    Diagnóstico VARCHAR(50) NOT NULL,
    Peso DECIMAL(5,2) NOT NULL,
    Edad INTEGER NOT NULL CHECK (Edad >= 0),
    Sexo CHAR(1) NOT NULL CHECK (Sexo IN ('M', 'F')),
    PRIMARY KEY (DNI, Diagnóstico)
) PARTITION BY LIST (Diagnóstico);

CREATE TABLE Paciente_diabetes PARTITION OF Paciente FOR VALUES IN ('Diabetes');
CREATE TABLE Paciente_obesidad PARTITION OF Paciente FOR VALUES IN ('Obesidad');
CREATE TABLE Paciente_cardiopatia PARTITION OF Paciente FOR VALUES IN ('Cardiopatía');
CREATE TABLE Paciente_hipertension PARTITION OF Paciente FOR VALUES IN ('Hipertensión');

INSERT INTO Paciente VALUES 
('45781236', 'Carla María Romero Díaz', 'Lima', 'Diabetes', 70.00, 45, 'F'),
('08569321', 'Luis Alberto Díaz Mendoza', 'Lima', 'Hipertensión', 85.00, 60, 'M'),
('72103654', 'Ana Paula Torres Castro', 'Callao', 'Obesidad', 90.00, 35, 'F'),
('25963147', 'Jorge Luis Ramírez Vargas', 'Callao', 'Cardiopatía', 78.00, 50, 'M'),
('15478962', 'María Carmen Suárez López', 'Lima', 'Diabetes', 65.00, 42, 'F'),
('36987412', 'Pedro José Quispe Huamán', 'Lima', 'Obesidad', 95.00, 38, 'M'),
('65412398', 'Rosa Isabel Valle García', 'Lima', 'Hipertensión', 72.00, 55, 'F'),
('89632147', 'Miguel Ángel Castro Rivas', 'Callao', 'Cardiopatía', 82.00, 48, 'M');

-- Población de la tabla Paciente con nuevos datos

INSERT INTO Paciente VALUES 
('12345678', 'Carmen Rosa Mendoza Silva', 'Arequipa', 'Diabetes', 68.50, 52, 'F'),
('23456789', 'Roberto Carlos Chávez López', 'Arequipa', 'Hipertensión', 88.00, 58, 'M'),
('34567890', 'Patricia Elena Flores Vargas', 'Arequipa', 'Obesidad', 92.30, 41, 'F'),
('45678901', 'Fernando José Morales Ruiz', 'Arequipa', 'Cardiopatía', 75.80, 47, 'M'),
('56789012', 'Mónica Isabel Reyes Castro', 'Trujillo', 'Diabetes', 63.20, 39, 'F'),
('67890123', 'Andrés Manuel Torres Díaz', 'Trujillo', 'Hipertensión', 91.50, 62, 'M'),
('78901234', 'Claudia Beatriz Herrera Ramos', 'Trujillo', 'Obesidad', 87.60, 36, 'F'),
('89012345', 'Ricardo Augusto Vega Paredes', 'Trujillo', 'Cardiopatía', 80.40, 53, 'M'),
('90123456', 'Esperanza Lucía Quispe Mamani', 'Cusco', 'Diabetes', 59.80, 44, 'F'),
('01234567', 'Julio César Condori Huanca', 'Cusco', 'Hipertensión', 77.90, 56, 'M'),
('11223344', 'Marisol Yolanda Ccopa Choque', 'Cusco', 'Obesidad', 94.20, 43, 'F'),
('22334455', 'Víctor Hugo Mamani Flores', 'Cusco', 'Cardiopatía', 73.50, 49, 'M'),
('33445566', 'Gladys Rosario Pinedo Vásquez', 'Iquitos', 'Diabetes', 66.70, 40, 'F'),
('44556677', 'Oswaldo Enrique Tello Ramírez', 'Iquitos', 'Hipertensión', 83.10, 59, 'M'),
('55667788', 'Roxana Pilar Ruiz Gonzales', 'Iquitos', 'Obesidad', 89.40, 37, 'F'),
('66778899', 'Edinson Raúl Fasabi Macedo', 'Iquitos', 'Cardiopatía', 76.30, 51, 'M');

SELECT 
    c.relname as partition_name,
    pg_size_pretty(pg_total_relation_size(c.oid)) as size
FROM pg_class c
JOIN pg_inherits i ON c.oid = i.inhrelid
JOIN pg_class p ON i.inhparent = p.oid
WHERE p.relname = 'paciente'
ORDER BY c.relname;

SELECT 
    Diagnóstico,
    COUNT(*) as Total_Pacientes,
    COUNT(DISTINCT Ciudad) as Ciudades_Representadas
FROM Paciente 
GROUP BY Diagnóstico 
ORDER BY Diagnóstico;

-- Población de la tabla Paciente con nuevos diagnósticos

CREATE TABLE Paciente_asma PARTITION OF Paciente FOR VALUES IN ('Asma');
CREATE TABLE Paciente_gastritis PARTITION OF Paciente FOR VALUES IN ('Gastritis');

-- Función para agregar particiones dinámicamente
CREATE OR REPLACE FUNCTION agregar_particion_diagnostico(nuevo_diagnostico VARCHAR(50))
RETURNS TEXT AS $$
DECLARE
    partition_name TEXT;
    sql_statement TEXT;
BEGIN
    partition_name := 'paciente_' || lower(replace(nuevo_diagnostico, ' ', '_'));
    
    sql_statement := format('CREATE TABLE %I PARTITION OF Paciente FOR VALUES IN (%L)', 
                           partition_name, nuevo_diagnostico);
    
    EXECUTE sql_statement;
    
    RETURN 'Partición agregada exitosamente: ' || partition_name;
END;
$$ LANGUAGE plpgsql;

SELECT agregar_particion_diagnostico('Artritis');

-- Población de la tabla Paciente con nuevos datos (Diagnósticos adicionales)
INSERT INTO Paciente VALUES 
('77889900', 'Elena Beatriz Sánchez Rivera', 'Piura', 'Asma', 62.50, 33, 'F'),
('88990011', 'Carlos Eduardo Paredes Luna', 'Chiclayo', 'Gastritis', 74.20, 28, 'M'),
('99001122', 'Patricia Morales Vega', 'Tacna', 'Artritis', 68.30, 48, 'F');

SELECT 
    Diagnóstico,
    COUNT(*) as Total_Pacientes,
    COUNT(DISTINCT Ciudad) as Ciudades_Representadas
FROM Paciente 
GROUP BY Diagnóstico 
ORDER BY Diagnóstico;