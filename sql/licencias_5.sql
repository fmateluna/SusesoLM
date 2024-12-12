-- SQLBook: Code

-- SQLBook: Code
-- Endpoint: /lm/diagnosis
-- Listado con licencias por diagnóstico específico, periodo de tiempo opcional
SELECT rut_medico, rut_trabajador, fecha_emision, probabilidad_sin_fundamento
FROM lme.df_propensity_score
WHERE cod_diagnostico_principal = :cod_diagnostico
AND fecha_emision BETWEEN :fecha_inicio AND :fecha_fin;
