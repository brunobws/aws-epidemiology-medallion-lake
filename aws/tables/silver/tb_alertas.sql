-- tb_alertas: Alertas epidemiológicos semanais com indicadores de transmissão
-- Fonte: InfoDengue API
-- Granularidade: Município + Doença + Semana Epidemiológica


CREATE EXTERNAL TABLE IF NOT EXISTS `tb_alertas`(
  `cd_geocode` int COMMENT 'Código geocódigo IBGE do município - chave para junção com tb_municipios',
  `ds_doenca` string COMMENT 'Tipo de doença: dengue, chikungunya ou zika',
  `nr_semana_epi` int COMMENT 'Número da semana epidemiológica (1-53) dentro do ano',
  `nr_nivel_alerta` int COMMENT 'Nível de alerta: 1=verde (baixo), 2=amarelo (médio), 3=laranja (alto), 4=vermelho (crítico)',
  `vl_casos_estimados` double COMMENT 'Estimativa pontual de casos na semana (baseada em modelo epidemiológico)',
  `vl_casos_estimados_min` double COMMENT 'Limite inferior (IC 95%) da estimativa de casos',
  `vl_casos_estimados_max` double COMMENT 'Limite superior (IC 95%) da estimativa de casos',
  `vl_casos` int COMMENT 'Número confirmado de casos na semana',
  `vl_rt` double COMMENT 'Taxa de Reprodução (Rt) - número médio de pessoas infectadas por um casos (Rt>1=crescimento)',
  `vl_incidencia` double COMMENT 'Taxa de incidência por 100 mil habitantes na semana epidemiológica',
  `vl_temp_min` double COMMENT 'Temperatura mínima média da semana em graus Celsius',
  `vl_temp_max` double COMMENT 'Temperatura máxima média da semana em graus Celsius',
  `vl_umid_min` double COMMENT 'Umidade relativa do ar mínima média da semana em percentual',
  `vl_umid_max` double COMMENT 'Umidade relativa do ar máxima média da semana em percentual',
  `vl_receptividade` int COMMENT 'Indicador binário de receptividade ambiental: 1=ambiente favorável para transmissão (clima/mosquito propenso)',
  `vl_transmissao` int COMMENT 'Indicador binário de transmissão ativa: 1=transmissão confirmada, 0=sem evidência de transmissão',
  `nm_municipio` string COMMENT 'Nome do município (denormalizado por performance em consultas)')
PARTITIONED BY (
  `nr_ano_epi` int COMMENT 'Ano epidemiológico (ex: 2026)',
  `dt_semana_epidemiologica` date COMMENT 'Data de domingo da semana epidemiológica (formato ISO)')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bws-dl-silver-sae1-prd/infodengue/tb_alertas'
