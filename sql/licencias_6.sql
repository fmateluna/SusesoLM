-- SQLBook: Code

-- SQLBook: Code
-- Endpoint: /lm/region
-- Listado con licencias por región y periodo de tiempo opcional
SELECT rut_medico, rut_trabajador, fecha_emision, codigo_autorizacion_pronunciamiento 
FROM ml.licencias l 
WHERE comuna_reposo = :comuna_reposo
AND fecha_emision BETWEEN :fecha_inicio AND :fecha_fin;
