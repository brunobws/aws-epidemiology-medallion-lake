-- tb_populacao: Dados de população por município e ano de referência
-- Fonte: IBGE SIDRA (Tabela 6579 - Estimativas de população)
-- Granularidade: Município + Ano


CREATE EXTERNAL TABLE IF NOT EXISTS `tb_populacao`(
  `cd_geocode` int COMMENT 'Código geocódigo IBGE do município - chave para junção com tb_municipios',
  `nm_municipio` string COMMENT 'Nome do município (denormalizado por performance)',
  `vl_populacao` int COMMENT 'Estimativa de população total do município no ano de referência')
PARTITIONED BY (
  `dt_ano_referencia` int COMMENT 'Ano de referência da estimativa populacional (ex: 2025, 2026)')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bws-dl-silver-sae1-prd/ibge/tb_populacao'
