-- tb_notificacoes: Notificações individuais de casos de arbovírus (SINAN - Diagnóstico e Notificação)
-- Fonte: SINAN / Ministério da Saúde (Download direto de CSV S3)
-- Granularidade: Caso individual (linha por notificação/paciente)


CREATE EXTERNAL TABLE IF NOT EXISTS `tb_notificacoes`(
  `id_agravo` string COMMENT 'Código da doença (ICD-10): A90=dengue, A92=chikungunya, A92.8=zika',
  `dt_sin_pri` date COMMENT 'Data do primeiro sintoma do paciente (critério de definição de caso)',
  `id_municip` int COMMENT 'Código IBGE do município de residência do paciente (anterior à adequação geocódigo - 6 dígitos)',
  `sg_uf` int COMMENT 'Código numérico da Unidade Federativa: 35=SP, 25=BA, 23=CE, etc.',
  `nu_idade_n` int COMMENT 'Idade do paciente em dias desde nascimento (converter para anos: dividir por 365.25)',
  `cs_sexo` string COMMENT 'Sexo do paciente: M=Masculino, F=Feminino, (vazio)=ignorado/não informado',
  `classi_fin` string COMMENT 'Classificação final: 10=confirmado (laboratorial/clínico-laboratorial), 0=outros/descartado, (vazio)=investigação em andamento',
  `evolucao` string COMMENT 'Desfecho clínico: 1=cura esperada, 2=cura com seqüela, 3=óbito por doença, 4=óbito por outras causas, 9=óbito por doença em investigação, (vazio)=caso em evolução')
PARTITIONED BY (
  `dt_notific` date COMMENT 'Data de notificação ao sistema (quando o caso foi registrado)')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bws-dl-silver-sae1-prd/sinan/tb_notificacoes'

