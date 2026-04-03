-- Transformação: silver -> gold_tb_ft_perfil_demografico
-- Agrega notificações individuais do SINAN em perfil demográfico e clínico
--
-- Execução: semanal ou mensal (após ingestão SINAN)
-- Partições de saída: nr_ano_notificacao / nr_mes_notificacao
-- Filtro obrigatório: sg_uf = 35 (São Paulo)
--
-- Nota sobre join municipio:
--   SINAN usa código IBGE de 6 dígitos (id_municip)
--   tb_municipios usa código IBGE de 7 dígitos (cd_geocode)
--   Mapeamento: id_municip = FLOOR(cd_geocode / 10)  — remove o dígito verificador


SELECT
    n.id_municip                                                        AS cd_geocode_ibge,
    m.nm_municipio,
    m.nm_microrregiao,
    m.nm_mesorregiao,
    n.id_agravo,
    CASE n.id_agravo
        WHEN 'A90'   THEN 'dengue'
        WHEN 'A92'   THEN 'chikungunya'
        WHEN 'A92.8' THEN 'zika'
        ELSE               n.id_agravo
    END                                                                 AS ds_doenca,
    CASE
        WHEN FLOOR(n.nu_idade_n / 365.25) < 5  THEN '0-4'
        WHEN FLOOR(n.nu_idade_n / 365.25) < 15 THEN '5-14'
        WHEN FLOOR(n.nu_idade_n / 365.25) < 30 THEN '15-29'
        WHEN FLOOR(n.nu_idade_n / 365.25) < 60 THEN '30-59'
        ELSE                                         '60+'
    END                                                                 AS ds_faixa_etaria,
    COALESCE(NULLIF(n.cs_sexo, ''), 'I')                               AS cs_sexo,
    COUNT(*)                                                            AS nr_notificacoes,
    SUM(CASE WHEN n.classi_fin = '10'           THEN 1 ELSE 0 END)     AS nr_casos_confirmados,
    SUM(CASE WHEN n.evolucao IN ('3', '9')      THEN 1 ELSE 0 END)     AS nr_obitos,
    SUM(CASE WHEN n.evolucao = '1'              THEN 1 ELSE 0 END)     AS nr_curas,
    -- partition columns
    YEAR(n.dt_notific)                                                  AS nr_ano_notificacao,
    MONTH(n.dt_notific)                                                 AS nr_mes_notificacao

FROM silver.tb_notificacoes n

LEFT JOIN silver.tb_municipios m
    ON CAST(m.cd_geocode / 10 AS INT) = n.id_municip

WHERE n.sg_uf = 35

GROUP BY
    n.id_municip,
    m.nm_municipio,
    m.nm_microrregiao,
    m.nm_mesorregiao,
    n.id_agravo,
    CASE n.id_agravo
        WHEN 'A90'   THEN 'dengue'
        WHEN 'A92'   THEN 'chikungunya'
        WHEN 'A92.8' THEN 'zika'
        ELSE               n.id_agravo
    END,
    CASE
        WHEN FLOOR(n.nu_idade_n / 365.25) < 5  THEN '0-4'
        WHEN FLOOR(n.nu_idade_n / 365.25) < 15 THEN '5-14'
        WHEN FLOOR(n.nu_idade_n / 365.25) < 30 THEN '15-29'
        WHEN FLOOR(n.nu_idade_n / 365.25) < 60 THEN '30-59'
        ELSE                                         '60+'
    END,
    COALESCE(NULLIF(n.cs_sexo, ''), 'I'),
    YEAR(n.dt_notific),
    MONTH(n.dt_notific)
