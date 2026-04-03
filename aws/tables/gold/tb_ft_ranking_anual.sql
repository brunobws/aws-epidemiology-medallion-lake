-- gold_tb_ft_ranking_anual: Ranking anual de municípios por carga de doença e exposição a alertas
-- Fonte: tb_alertas (InfoDengue) + tb_municipios (IBGE) + tb_populacao (IBGE SIDRA)
-- Granularidade: Município + Doença + Ano
-- Uso: Top-N municípios mais afetados, comparativo regional anual, identificação de hotspots


CREATE EXTERNAL TABLE IF NOT EXISTS `tb_ft_ranking_anual`(
  `cd_geocode`                    int     COMMENT 'Código geocódigo IBGE do município (7 dígitos)',
  `nm_municipio`                  string  COMMENT 'Nome do município',
  `nm_microrregiao`               string  COMMENT 'Microrregião IBGE',
  `nm_mesorregiao`                string  COMMENT 'Mesorregião IBGE',
  `vl_populacao`                  int     COMMENT 'Estimativa populacional do município no ano',
  `vl_total_casos`                int     COMMENT 'Total de casos confirmados acumulados no ano epidemiológico',
  `vl_incidencia_acumulada`       double  COMMENT 'Incidência acumulada anual por 100 mil habitantes',
  `nr_max_alerta`                 int     COMMENT 'Nível máximo de alerta atingido no ano (1-4)',
  `nr_semanas_alerta_vermelho`    int     COMMENT 'Número de semanas em alerta vermelho (nível 4) no ano',
  `nr_semanas_alerta_alto`        int     COMMENT 'Número de semanas em alerta alto ou crítico (nível >= 3) no ano',
  `nr_semanas_transmissao_ativa`  int     COMMENT 'Número de semanas com transmissão ativa confirmada no ano',
  `nr_semanas_rt_acima_1`         int     COMMENT 'Número de semanas com Rt > 1 (crescimento epidêmico) no ano',
  `vl_rt_medio`                   double  COMMENT 'Média do Rt ao longo do ano epidemiológico',
  `nr_rank_estado`                int     COMMENT 'Posição no ranking estadual (SP) por incidência acumulada — 1 = mais afetado',
  `nr_rank_mesorregiao`           int     COMMENT 'Posição no ranking da mesorregião por incidência acumulada — 1 = mais afetado')
PARTITIONED BY (
  `nr_ano_epi`  int    COMMENT 'Ano epidemiológico (ex: 2026)',
  `ds_doenca`   string COMMENT 'Doença monitorada: dengue, chikungunya ou zika')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bws-dl-gold-sae1-prd/tb_ft_ranking_anual'
