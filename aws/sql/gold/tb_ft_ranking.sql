SELECT
    a.cd_geocode,
    m.nm_municipio,
    m.nm_mesorregiao,
    a.ds_doenca,
    SUM(a.vl_casos)          AS vl_total_casos,
    p.vl_populacao,
    ROUND(
        (CAST(SUM(a.vl_casos) AS DOUBLE) / CAST(p.vl_populacao AS DOUBLE)) * 100000, 2
    )                        AS vl_incidencia_acumulada_100k,
    ROW_NUMBER() OVER (
        PARTITION BY a.ds_doenca
        ORDER BY SUM(a.vl_casos) DESC
    )                        AS nr_ranking
FROM silver.tb_alertas a
INNER JOIN silver.tb_municipios m
    ON a.cd_geocode = m.cd_geocode
INNER JOIN silver.tb_populacao p
    ON a.cd_geocode = p.cd_geocode
WHERE a.nr_ano_epi = YEAR(CURRENT_DATE)
  AND p.vl_populacao > 0
GROUP BY
    a.cd_geocode,
    m.nm_municipio,
    m.nm_mesorregiao,
    a.ds_doenca,
    p.vl_populacao
