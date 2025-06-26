-- Procedimiento almacenado para insertar pacientes y crear particiones dinámicamente
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
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_inherits i ON c.oid = i.inhrelid
        JOIN pg_class p ON i.inhparent = p.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE p.relname = 'paciente'
        AND n.nspname = 'public'
        AND c.relname = 'paciente_' || lower(replace(p_diagnostico, ' ', '_'))
    ) INTO partition_exists;

    IF NOT partition_exists THEN
        PERFORM agregar_particion_diagnostico(p_diagnostico);
        RAISE NOTICE 'Nueva partición creada para diagnóstico: %', p_diagnostico;
    END IF;

    INSERT INTO Paciente VALUES (p_dni, p_nombre, p_ciudad, p_diagnostico, p_peso, p_edad, p_sexo);
    
    RAISE NOTICE 'Paciente con DNI % insertado correctamente con diagnóstico %', p_dni, p_diagnostico;
END;
$$;

CALL insertar_paciente('11111111', 'Juan Pérez', 'Lima', 'Migraña', 75.50, 35, 'M');
CALL insertar_paciente('22222222', 'María López', 'Arequipa', 'Depresión', 62.30, 40, 'F');
CALL insertar_paciente('33333333', 'Carlos Rodríguez', 'Trujillo', 'Obesidad', 80.10, 45, 'M');
CALL insertar_paciente('44444444', 'Ana Torres', 'Lima', 'Anemia', 55.20, 28, 'F');
CALL insertar_paciente('55555555', 'Roberto Gómez', 'Cusco', 'Anemia', 70.80, 50, 'M');

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