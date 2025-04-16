-- SQLBook: Code
-- Endpoint: /lm/dto/total
-- Obtiene el total de licencias emitidas por el mismo profesional del folio, en un rango de tiempo
SELECT
    l.rut_medico,
    COUNT(0) AS total_licencias,
    l.cod_diagnostico_principal
FROM
    ml.propensity_score as a,    
    ml.licencias as l     
WHERE
    a.folio = :folio
    and l.folio = a.folio     
GROUP BY
    l.rut_medico,
    l.cod_diagnostico_principal;