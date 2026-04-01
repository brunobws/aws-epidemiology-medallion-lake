# Silver Tables - Epidemiological Data

Tabelas de dados refinados (Silver layer) do Medallion Lake para vigilância epidemiológica de arbovírus em São Paulo.

## Tabelas

### tb_municipios
Dados cadastrais de municípios de São Paulo com hierarquia geográfica (microrregião e mesorregião).
**Fonte:** IBGE API - Localidades
**Granularidade:** Um registro por município de SP
**Particionamento:** Não particionada

### tb_alertas
Alertas epidemiológicos semanais com indicadores de transmissão, casos estimados e variáveis ambientais.
**Fonte:** InfoDengue API
**Granularidade:** Município + Doença + Semana Epidemiológica
**Particionamento:** `nr_ano_epi`, `dt_semana_epidemiologica`
**Cobertura:** Dengue, Chikungunya, Zika

### tb_populacao
Dados de população por município com ano de referência para cálculos de incidência.
**Fonte:** IBGE SIDRA
**Granularidade:** Município + Ano
**Particionamento:** `dt_ano_referencia`

### tb_notificacoes
Notificações individuais de casos de arbovírus do Sistema de Informação de Agravos de Notificação (SINAN).
**Fonte:** SINAN - Ministério da Saúde (CSV)
**Granularidade:** Caso individual
**Particionamento:** `dt_notific`
**Cobertura:** Todos os estados - filtrar por sg_uf=35 para SP

---

## Dicionário de Dados

Veja os arquivos `tb_municipios.sql`, `tb_alertas.sql`, `tb_notificacoes.sql` e `tb_populacao.sql` para DDL detalhado com comentários de cada coluna.
