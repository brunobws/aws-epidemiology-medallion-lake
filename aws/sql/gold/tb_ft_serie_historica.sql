SELECT
    a.nr_ano_epi,
    a.nr_semana_epi,
    a.ds_doenca,
    m.nm_mesorregiao,
    SUM(a.vl_casos)            AS vl_total_casos,
    SUM(a.vl_casos_estimados)  AS vl_total_estimados,
    ROUND(AVG(a.vl_rt), 3)    AS vl_media_rt,
    ROUND(AVG(a.vl_temp_min), 1) AS vl_media_temp_min,
    ROUND(AVG(a.vl_temp_max), 1) AS vl_media_temp_max,
    ROUND(AVG(a.vl_umid_min), 1) AS vl_media_umid_min,
    ROUND(AVG(a.vl_umid_max), 1) AS vl_media_umid_max
FROM silver.tb_alertas a
INNER JOIN silver.tb_municipios m
    ON a.cd_geocode = m.cd_geocode
GROUP BY
    a.nr_ano_epi,
    a.nr_semana_epi,
    a.ds_doenca,
    m.nm_mesorregiao
ORDER BY
    a.nr_ano_epi,
    a.nr_semana_epi
