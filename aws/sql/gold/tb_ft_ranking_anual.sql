-- Transformação: silver -> gold_tb_ft_ranking_anual
-- Agrega alertas semanais em visão anual por município + doença
-- Calcula carga de doença acumulada e rankings intra-estado e intra-mesorregião
--
-- Execução: semanal (atualiza o ano corrente) — ou anual para fechamento
-- Partições de saída: nr_ano_epi / ds_doenca
--
-- Nota: nr_rank_* depende de window functions — requer CTAS ou INSERT OVERWRITE completo por partição
-- Nota: pop_latest resolve o caso em que tb_populacao ainda não tem o ano do alerta publicado;
--       usa sempre a estimativa mais recente disponível por município


WITH pop_latest AS (

    SELECT cd_geocode, vl_populacao
    FROM (
        SELECT
            cd_geocode,
            vl_populacao,
            ROW_NUMBER() OVER (PARTITION BY cd_geocode ORDER BY dt_ano_referencia DESC) AS rn
        FROM tb_populacao
    )
    WHERE rn = 1

),

base AS (

    SELECT
        a.cd_geocode,
        m.nm_municipio,
        m.nm_microrregiao,
        m.nm_mesorregiao,
        a.ds_doenca,
        a.nr_ano_epi,
        MAX(p.vl_populacao)                                             AS vl_populacao,
        SUM(a.vl_casos)                                                 AS vl_total_casos,
        MAX(a.nr_nivel_alerta)                                          AS nr_max_alerta,
        SUM(CASE WHEN a.nr_nivel_alerta = 4  THEN 1 ELSE 0 END)        AS nr_semanas_alerta_vermelho,
        SUM(CASE WHEN a.nr_nivel_alerta >= 3 THEN 1 ELSE 0 END)        AS nr_semanas_alerta_alto,
        SUM(CASE WHEN a.vl_transmissao = 1   THEN 1 ELSE 0 END)        AS nr_semanas_transmissao_ativa,
        SUM(CASE WHEN a.vl_rt > 1.0          THEN 1 ELSE 0 END)        AS nr_semanas_rt_acima_1,
        ROUND(AVG(a.vl_rt), 4)                                         AS vl_rt_medio

    FROM tb_alertas a

    LEFT JOIN tb_municipios m
        ON a.cd_geocode = m.cd_geocode

    LEFT JOIN pop_latest p
        ON a.cd_geocode = p.cd_geocode

    GROUP BY
        a.cd_geocode,
        m.nm_municipio,
        m.nm_microrregiao,
        m.nm_mesorregiao,
        a.ds_doenca,
        a.nr_ano_epi

)

SELECT
    cd_geocode,
    nm_municipio,
    nm_microrregiao,
    nm_mesorregiao,
    vl_populacao,
    vl_total_casos,
    ROUND(
        CAST(vl_total_casos AS DOUBLE) * 100000.0 / NULLIF(vl_populacao, 0),
        2
    )                                                                   AS vl_incidencia_acumulada,
    nr_max_alerta,
    nr_semanas_alerta_vermelho,
    nr_semanas_alerta_alto,
    nr_semanas_transmissao_ativa,
    nr_semanas_rt_acima_1,
    vl_rt_medio,
    CAST(
        RANK() OVER (
            PARTITION BY nr_ano_epi, ds_doenca
            ORDER BY CAST(vl_total_casos AS DOUBLE) * 100000.0 / NULLIF(vl_populacao, 0) DESC
        ) AS INT
    )                                                                   AS nr_rank_estado,
    CAST(
        RANK() OVER (
            PARTITION BY nr_ano_epi, ds_doenca, nm_mesorregiao
            ORDER BY CAST(vl_total_casos AS DOUBLE) * 100000.0 / NULLIF(vl_populacao, 0) DESC
        ) AS INT
    )                                                                   AS nr_rank_mesorregiao,
    -- partition columns
    nr_ano_epi,
    ds_doenca

FROM base
