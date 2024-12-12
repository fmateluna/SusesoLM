-- SQLBook: Code
-- Endpoint: /lm/dto/total
-- Obtiene el total de licencias emitidas por el mismo profesional en un rango de tiempo
SELECT
    a.rut_medico,
    COUNT(0) AS total_licencias,
    a.cod_diagnostico_principal
FROM
    lme.df_propensity_score a
WHERE
    a.folio = :folio
GROUP BY
    a.rut_medico,
    a.cod_diagnostico_principal;
