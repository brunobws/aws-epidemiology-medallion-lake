# ArboVigilancia SP

Plataforma de vigilancia epidemiologica de arboviroses (dengue, chikungunya, zika) no estado de Sao Paulo, construida na AWS com arquitetura Medallion (Bronze / Silver / Gold).

## Arquitetura

```
Fontes (APIs Publicas)          Ingestao            Processamento           Consumo
+-----------------+         +----------+         +-----------+         +------------+
| InfoDengue      |-------->|          |         |           |         |            |
| IBGE Municipios |-------->|  Lambda  |--S3---->|   Glue    |--S3---->|   Athena   |---> Streamlit
| IBGE Populacao  |-------->| (Bronze) |         | (B>S>G)   |         |  (Iceberg) |---> Bedrock/IA
| SINAN/DataSUS   |-------->|          |         |           |         |            |
+-----------------+         +----------+         +-----------+         +------------+
                                 |                                          |
                          EventBridge + Step Functions (Orquestracao Semanal)
```

## Fontes de Dados

| Fonte | Descricao | Frequencia |
|---|---|---|
| **InfoDengue** (Fiocruz) | Alertas epidemiologicos por municipio/semana | Semanal |
| **IBGE Municipios** | Cadastro de municipios de SP (geocodes, regioes) | Estatico |
| **IBGE SIDRA** | Populacao estimada por municipio | Anual |
| **SINAN/OpenDataSUS** | Notificacoes individuais de arboviroses | Semanal |

## Camadas do Data Lake

| Camada | Descricao | Bucket |
|---|---|---|
| **Bronze** | Dados brutos das APIs (JSON/CSV) | `bws-dl-bronze-sae1-prd` |
| **Silver** | Dados limpos, tipados, com qualidade validada (Iceberg) | `bws-dl-silver-sae1-prd` |
| **Gold** | Indicadores epidemiologicos agregados (Iceberg) | `bws-dl-gold-sae1-prd` |

## Tabelas Silver

| target_table | Tabela Athena | Fonte |
|---|---|---|
| `ibge_tb_municipios` | `silver.tb_municipios` | IBGE Localidades |
| `ibge_tb_populacao` | `silver.tb_populacao` | IBGE SIDRA |
| `infodengue_tb_alertas` | `silver.tb_alertas` | InfoDengue |
| `sinan_tb_notificacoes` | `silver.tb_notificacoes` | SINAN/OpenDataSUS |

## Tabelas Gold

| target_table | Indicador |
|---|---|
| `gold_tb_ft_incidencia` | Taxa de incidencia por 100 mil habitantes |
| `gold_tb_ft_alertas` | Alertas consolidados por municipio |
| `gold_tb_ft_ranking` | Ranking de municipios por incidencia |
| `gold_tb_ft_serie_historica` | Series temporais por regiao/doenca |

## Stack

- **Ingestao:** AWS Lambda (Python 3.11)
- **Armazenamento:** Amazon S3 + Apache Iceberg
- **Processamento:** AWS Glue (PySpark)
- **Consulta:** Amazon Athena
- **Orquestracao:** AWS Step Functions + EventBridge
- **Dashboard:** Streamlit (Docker em EC2 t3.micro)
- **Qualidade:** Great Expectations
- **Observabilidade:** Logs estruturados em Parquet + Athena
- **IA (futuro):** Amazon Bedrock + RAG

## Como Executar

### Dashboard (local/EC2)
```bash
make docker-build
make docker-up
# Acesse http://localhost:8501
```

### Testes
```bash
make test
```

## Estrutura do Projeto

```
aws/
  glue_scripts/          # Engine generica Medallion (B>S, S>G)
  lambda_scripts/        # Lambdas de ingestao (1 por fonte)
  modules/               # Modulos compartilhados (logs, utils, quality)
  sql/gold/              # SQLs de agregacao Gold
  step_functions/        # Definicao ASL da state machine
  dynamo_params/         # JSON dos itens DynamoDB
streamlit_app/           # Dashboard interativo
docker/                  # Docker Compose (Streamlit)
tests/                   # Testes unitarios
docs/                    # Documentacao
```
