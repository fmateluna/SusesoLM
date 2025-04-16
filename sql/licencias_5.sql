-- SQLBook: Code

-- SQLBook: Code
-- Endpoint: /lm/diagnosis
-- Listado con licencias por diagnóstico específico, periodo de tiempo opcional
SELECT rut_medico, rut_trabajador, fecha_emision,codigo_autorizacion_pronunciamiento
FROM ml.licencias l 
WHERE cod_diagnostico_principal = :cod_diagnostico
AND fecha_emision BETWEEN :fecha_inicio AND :fecha_fin;
