-- gold_tb_ft_perfil_demografico: Perfil demográfico e desfechos clínicos das notificações (SINAN)
-- Fonte: tb_notificacoes (SINAN/Ministério da Saúde) + tb_municipios (IBGE)
-- Granularidade: Município + Doença + Faixa Etária + Sexo + Mês de Notificação
-- Filtro: apenas SP (sg_uf = 35)
-- Uso: Pirâmide etária, análise de mortalidade, perfil epidemiológico por grupo populacional


CREATE EXTERNAL TABLE IF NOT EXISTS `gold_tb_ft_perfil_demografico`(
  `cd_geocode_ibge`      int     COMMENT 'Código IBGE 6 dígitos do município (id_municip do SINAN)',
  `nm_municipio`         string  COMMENT 'Nome do município — NULL se código não mapeado em tb_municipios',
  `nm_microrregiao`      string  COMMENT 'Microrregião IBGE do município',
  `nm_mesorregiao`       string  COMMENT 'Mesorregião IBGE do município',
  `id_agravo`            string  COMMENT 'Código CID-10: A90=dengue, A92=chikungunya, A92.8=zika',
  `ds_doenca`            string  COMMENT 'Descrição da doença: dengue, chikungunya, zika',
  `ds_faixa_etaria`      string  COMMENT 'Faixa etária do paciente: 0-4, 5-14, 15-29, 30-59, 60+',
  `cs_sexo`              string  COMMENT 'Sexo do paciente: M=masculino, F=feminino, I=ignorado/não informado',
  `nr_notificacoes`      int     COMMENT 'Total de notificações no grupo (inclui casos em investigação)',
  `nr_casos_confirmados` int     COMMENT 'Casos com classificação final confirmada (classi_fin = 10)',
  `nr_obitos`            int     COMMENT 'Óbitos pela doença (evolucao=3) ou em investigação (evolucao=9)',
  `nr_curas`             int     COMMENT 'Casos com desfecho de cura registrado (evolucao = 1)')
PARTITIONED BY (
  `nr_ano_notificacao` int COMMENT 'Ano de notificação ao sistema SINAN',
  `nr_mes_notificacao` int COMMENT 'Mês de notificação ao sistema SINAN (1-12)')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bws-dl-gold-sae1-prd/tb_ft_perfil_demografico'
