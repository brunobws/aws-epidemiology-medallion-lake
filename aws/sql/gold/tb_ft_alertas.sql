SELECT
    a.cd_geocode,
    m.nm_municipio,
    m.nm_mesorregiao,
    a.ds_doenca,
    a.nr_nivel_alerta,
    a.vl_casos,
    a.vl_casos_estimados,
    a.vl_rt,
    a.vl_temp_min,
    a.vl_temp_max,
    a.vl_umid_min,
    a.vl_umid_max,
    a.dt_semana_epidemiologica,
    a.nr_semana_epi,
    a.nr_ano_epi
FROM silver.tb_alertas a
INNER JOIN silver.tb_municipios m
    ON a.cd_geocode = m.cd_geocode
WHERE a.nr_ano_epi = YEAR(CURRENT_DATE)
