-- gold_tb_ft_alerta_semanal: Situação epidemiológica semanal enriquecida por município e doença
-- Fonte: tb_alertas (InfoDengue) + tb_municipios (IBGE) + tb_populacao (IBGE SIDRA)
-- Granularidade: Município + Doença + Semana Epidemiológica
-- Uso: Mapa de alertas, série temporal, situação atual, comparativo entre municípios


CREATE EXTERNAL TABLE IF NOT EXISTS `tb_ft_alerta_semanal`(
  `cd_geocode`        int     COMMENT 'Código geocódigo IBGE do município (7 dígitos)',
  `nm_municipio`      string  COMMENT 'Nome do município',
  `nm_microrregiao`   string  COMMENT 'Microrregião IBGE (divisão administrativa intermediária)',
  `nm_mesorregiao`    string  COMMENT 'Mesorregião IBGE (maior divisão administrativa regional)',
  `vl_populacao`      int     COMMENT 'Estimativa populacional do município no ano de referência',
  `nr_semana_epi`     int     COMMENT 'Número da semana epidemiológica (1-53)',
  `nr_nivel_alerta`   int     COMMENT 'Nível de alerta: 1=verde (baixo), 2=amarelo (médio), 3=laranja (alto), 4=vermelho (crítico)',
  `ds_nivel_alerta`   string  COMMENT 'Descrição textual do nível de alerta: verde, amarelo, laranja, vermelho',
  `vl_casos`          int     COMMENT 'Número confirmado de casos na semana',
  `vl_casos_estimados` double COMMENT 'Estimativa pontual de casos na semana (modelo epidemiológico)',
  `vl_incidencia`     double  COMMENT 'Taxa de incidência por 100 mil habitantes na semana epidemiológica',
  `vl_rt`             double  COMMENT 'Taxa de Reprodução (Rt) — Rt > 1 indica crescimento epidêmico ativo',
  `fl_epidemia`       int     COMMENT 'Flag de epidemia ativa: 1 quando Rt > 1 com transmissão confirmada simultaneamente',
  `fl_transmissao`    int     COMMENT 'Indicador de transmissão ativa confirmada na semana (0=não / 1=sim)',
  `fl_receptividade`  int     COMMENT 'Indicador de ambiente climático favorável para transmissão (0=não / 1=sim)',
  `vl_temp_min`       double  COMMENT 'Temperatura mínima média da semana (°C)',
  `vl_temp_max`       double  COMMENT 'Temperatura máxima média da semana (°C)',
  `vl_umid_min`       double  COMMENT 'Umidade relativa mínima média da semana (%)',
  `vl_umid_max`       double  COMMENT 'Umidade relativa máxima média da semana (%)')
PARTITIONED BY (
  `nr_ano_epi`               int    COMMENT 'Ano epidemiológico (ex: 2026)',
  `ds_doenca`                string COMMENT 'Doença monitorada: dengue, chikungunya ou zika',
  `dt_semana_epidemiologica` date   COMMENT 'Data do domingo da semana epidemiológica (formato ISO)')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bws-dl-gold-sae1-prd/tb_ft_alerta_semanal'
