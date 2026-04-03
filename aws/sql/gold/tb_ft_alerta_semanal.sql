-- Transformação: silver -> gold_tb_ft_alerta_semanal
-- Enriquece os alertas semanais com hierarquia geográfica, população e flags derivados
--
-- Execução: semanal (após ingestão InfoDengue + IBGE)
-- Partições de saída: nr_ano_epi / ds_doenca / dt_semana_epidemiologica
--
-- Joins:
--   tb_alertas    -> tb_municipios  : cd_geocode (1:1)
--   tb_alertas    -> pop_latest     : cd_geocode — usa o ano mais recente disponível em tb_populacao
--                                     (evita NULL quando o ano do alerta ainda não tem estimativa publicada)


WITH pop_latest AS (
    SELECT cd_geocode, vl_populacao
    FROM (
        SELECT
            cd_geocode,
            vl_populacao,
            ROW_NUMBER() OVER (PARTITION BY cd_geocode ORDER BY dt_ano_referencia DESC) AS rn
        FROM silver.tb_populacao
    )
    WHERE rn = 1
)

SELECT
    a.cd_geocode,
    m.nm_municipio,
    m.nm_microrregiao,
    m.nm_mesorregiao,
    p.vl_populacao,
    a.nr_semana_epi,
    a.nr_nivel_alerta,
    CASE a.nr_nivel_alerta
        WHEN 1 THEN 'verde'
        WHEN 2 THEN 'amarelo'
        WHEN 3 THEN 'laranja'
        WHEN 4 THEN 'vermelho'
        ELSE        'desconhecido'
    END                                                             AS ds_nivel_alerta,
    a.vl_casos,
    a.vl_casos_estimados,
    a.vl_incidencia,
    a.vl_rt,
    CASE WHEN a.vl_rt > 1.0 AND a.vl_transmissao = 1 THEN 1
         ELSE 0
    END                                                             AS fl_epidemia,
    a.vl_transmissao                                                AS fl_transmissao,
    a.vl_receptividade                                              AS fl_receptividade,
    a.vl_temp_min,
    a.vl_temp_max,
    a.vl_umid_min,
    a.vl_umid_max,
    -- partition columns
    a.nr_ano_epi,
    a.ds_doenca,
    a.dt_semana_epidemiologica

FROM silver.tb_alertas a

LEFT JOIN silver.tb_municipios m
    ON a.cd_geocode = m.cd_geocode

LEFT JOIN pop_latest p
    ON a.cd_geocode = p.cd_geocode
