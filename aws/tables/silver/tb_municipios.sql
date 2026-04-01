-- tb_municipios: Dados cadastrais de municípios de São Paulo com hierarquia geográfica
-- Fonte: IBGE API - Localidades


CREATE EXTERNAL TABLE IF NOT EXISTS `tb_municipios`(
  `cd_geocode` int COMMENT 'Código geocódigo IBGE do município - identificador único',
  `nm_municipio` string COMMENT 'Nome do município (ex: Adamantina, São Paulo)',
  `nm_microrregiao` string COMMENT 'Nome da microrregião - divisão administrativa intermediária',
  `nm_mesorregiao` string COMMENT 'Nome da mesorregião - maior divisão administrativa da região')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bws-dl-silver-sae1-prd/ibge/tb_municipios'
