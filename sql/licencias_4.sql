-- SQLBook: Code

-- SQLBook: Code
-- Endpoint: /lm/fundamento-indicator
-- Indicador binario o probabilístico de fundamento médico
SELECT folio,
       CASE
           WHEN probabilidad_sin_fundamento >= 0.80 THEN 'Probabilidad alta de ser sin fundamento'
           ELSE 'Fundamento médico probable'
       END AS indicador
FROM lme.df_propensity_score
WHERE folio = :folio;
