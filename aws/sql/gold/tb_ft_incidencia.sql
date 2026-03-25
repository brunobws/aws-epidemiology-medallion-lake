SELECT
    a.cd_geocode,
    m.nm_municipio,
    m.nm_mesorregiao,
    a.ds_doenca,
    a.dt_semana_epidemiologica,
    a.nr_semana_epi,
    a.nr_ano_epi,
    a.vl_casos,
    a.vl_casos_estimados,
    p.vl_populacao,
    ROUND((CAST(a.vl_casos AS DOUBLE) / CAST(p.vl_populacao AS DOUBLE)) * 100000, 2) AS vl_incidencia_100k,
    a.nr_nivel_alerta,
    a.vl_rt
FROM silver.tb_alertas a
INNER JOIN silver.tb_municipios m
    ON a.cd_geocode = m.cd_geocode
INNER JOIN silver.tb_populacao p
    ON a.cd_geocode = p.cd_geocode
WHERE p.vl_populacao > 0
