-- ============================================================================
-- SCRIPT DE FUSIÓN TRANSACCIONAL: Staging → Producción
-- ============================================================================
-- Este script SQL realiza la carga atómica desde las tablas de staging
-- hacia las tablas de producción del sistema OLTP.
--
-- Estrategia:
-- 1. UPDATE ... FROM staging: Actualiza registros existentes
-- 2. INSERT ... ON CONFLICT: Inserta nuevos registros o actualiza en conflicto
--
-- Tablas involucradas:
-- - staging_inscripciones → inscripciones (producción)
-- - staging_calificaciones → calificaciones (producción)
-- ============================================================================

BEGIN;

-- ============================================================================
-- FUSIÓN DE INSCRIPCIONES
-- ============================================================================

-- Actualizar inscripciones existentes
-- TODO: Definir estructura de tablas y campos a actualizar
UPDATE inscripciones AS prod
SET 
    -- campo1 = stg.campo1,
    -- campo2 = stg.campo2,
    -- fecha_actualizacion = CURRENT_TIMESTAMP
    updated_at = CURRENT_TIMESTAMP
FROM staging_inscripciones AS stg
WHERE prod.id_inscripcion = stg.id_inscripcion;

-- Insertar nuevas inscripciones (o actualizar si hay conflicto de PK)
INSERT INTO inscripciones (
    -- id_inscripcion,
    -- id_estudiante,
    -- id_curso,
    -- fecha_inscripcion,
    -- estado,
    -- created_at,
    -- updated_at
    -- TODO: Definir columnas reales
)
SELECT 
    -- stg.id_inscripcion,
    -- stg.id_estudiante,
    -- stg.id_curso,
    -- stg.fecha_inscripcion,
    -- stg.estado,
    -- CURRENT_TIMESTAMP,
    -- CURRENT_TIMESTAMP
    -- TODO: Mapear columnas desde staging
FROM staging_inscripciones AS stg
ON CONFLICT (id_inscripcion) DO UPDATE SET
    -- campo1 = EXCLUDED.campo1,
    -- campo2 = EXCLUDED.campo2,
    updated_at = CURRENT_TIMESTAMP;


-- ============================================================================
-- FUSIÓN DE CALIFICACIONES
-- ============================================================================

-- Actualizar calificaciones existentes
UPDATE calificaciones AS prod
SET 
    -- nota = stg.nota,
    -- tipo_evaluacion = stg.tipo_evaluacion,
    -- fecha_registro = stg.fecha_registro,
    updated_at = CURRENT_TIMESTAMP
FROM staging_calificaciones AS stg
WHERE prod.id_calificacion = stg.id_calificacion;

-- Insertar nuevas calificaciones (o actualizar si hay conflicto)
INSERT INTO calificaciones (
    -- id_calificacion,
    -- id_inscripcion,
    -- id_evaluacion,
    -- nota,
    -- tipo_evaluacion,
    -- fecha_registro,
    -- created_at,
    -- updated_at
    -- TODO: Definir columnas reales
)
SELECT 
    -- stg.id_calificacion,
    -- stg.id_inscripcion,
    -- stg.id_evaluacion,
    -- stg.nota,
    -- stg.tipo_evaluacion,
    -- stg.fecha_registro,
    -- CURRENT_TIMESTAMP,
    -- CURRENT_TIMESTAMP
    -- TODO: Mapear columnas desde staging
FROM staging_calificaciones AS stg
ON CONFLICT (id_calificacion) DO UPDATE SET
    -- nota = EXCLUDED.nota,
    -- tipo_evaluacion = EXCLUDED.tipo_evaluacion,
    updated_at = CURRENT_TIMESTAMP;


-- ============================================================================
-- LIMPIEZA DE TABLAS DE STAGING (Opcional)
-- ============================================================================

-- Opción 1: Truncar staging para la próxima ejecución
-- TRUNCATE TABLE staging_inscripciones;
-- TRUNCATE TABLE staging_calificaciones;

-- Opción 2: Mantener staging para auditoría/debugging
-- (No hacer nada, serán reemplazadas en la próxima ejecución)


COMMIT;

-- ============================================================================
-- NOTAS DE IMPLEMENTACIÓN
-- ============================================================================
-- 
-- - Ajustar los nombres de columnas según el esquema real de la BD
-- - Considerar agregar índices en staging para mejorar performance de JOIN
-- - Evaluar si se necesitan triggers o validaciones adicionales
-- - Implementar logging/auditoría de registros procesados
-- - Considerar particionamiento si el volumen de datos es muy grande
--
-- ============================================================================
