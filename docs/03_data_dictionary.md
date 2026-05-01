# Data Lake Tables Dictionary

This document details all tables available in the Silver and Gold layers of the EpiMind Data Lake. It serves as a data dictionary and a cheat sheet for frequent queries.

---

## Silver Layer (Cleaned & Standardized)

Tabelas de dados refinados para vigilância epidemiológica de arbovírus em São Paulo. Esses dados não estão perfeitamente agregados, mas já estão limpos, padronizados e em formato Parquet para alta performance.

### 1. Resumo das Tabelas

| Tabela | Granularidade | Records | Partition | Source | Update |
|--------|---------------|---------|-----------|--------|--------|
| **tb_municipios** | Município | 645 | None | IBGE API | Static |
| **tb_alertas** | Mun + Doença + Week | ~1,935/semana | ano_epi + dt_semana | InfoDengue API | Weekly |
| **tb_populacao** | Município + Ano | 645/ano | ano_referencia | IBGE SIDRA | Annual |
| **tb_notificacoes** | Caso Individual | ~50-500/semana | dt_notific | SINAN CSV | Weekly |

- **tb_municipios**: Dados cadastrais de municípios de São Paulo com hierarquia geográfica (microrregião e mesorregião).
- **tb_alertas**: Alertas epidemiológicos semanais com indicadores de transmissão (Rt), casos estimados e variáveis ambientais.
- **tb_populacao**: Dados de população por município com ano de referência para cálculos de incidência.
- **tb_notificacoes**: Notificações individuais de casos de arbovírus do Sistema de Informação de Agravos de Notificação (SINAN).

### 2. Tipos de Dados Importantes (Enums)

**Doenças:**
- `A90` = Dengue
- `A92` = Chikungunya
- `A92.8` = Zika

**Nível de Alerta (`tb_alertas`):**
- `1` = Verde (Seguro)
- `2` = Amarelo (Médio)
- `3` = Laranja (Alto)
- `4` = Vermelho (Crítico)

**Classificação Final (`tb_notificacoes`):**
- `10` = Confirmado (laboratorial ou clínico-laboratorial)
- `0` = Descartado / Outro

**Evolução (`tb_notificacoes`):**
- `1` = Cura esperada
- `2` = Cura com seqüela
- `3` = Óbito por doença

---

## Gold Layer (Aggregated for Analytics)

A camada Gold possui tabelas pré-agregadas para servir o Dashboard Streamlit e responder rapidamente às consultas da Inteligência Artificial. Elas unificam informações da Silver layer.

### 1. Resumo das Tabelas

| Tabela | Granularidade | Partition | Objetivo Principal no Dashboard |
|--------|---------------|-----------|----------------------------------|
| **tb_ft_alerta_semanal** | Município + Semana | `dt_semana` | Visão agregada do status de alerta e tendências semanais. (Aba Vigilância - Visão Geral). |
| **tb_ft_perfil_demografico**| Município + Faixa Etária + Sexo | `ano` | Distribuição do perfil demográfico dos casos. Usada para os gráficos populacionais e IA. |
| **tb_ft_ranking_anual** | Município + Ano | `ano` | Cálculo consolidado (score de risco ajustado, incidência média, tempo em alerta vermelho). (Aba Vigilância - Ranking). |

*(Para o DDL completo com comentários detalhados de cada coluna, consulte os arquivos dentro de `aws/tables/silver/` e `aws/tables/gold/` no repositório).*

---

## Performance Tips

1. **Sempre filtrar por data/partição primeiro:**
   - Use `WHERE dt_semana_epidemiologica >= ...` para `tb_alertas`
   - Use `WHERE dt_notific >= ...` para `tb_notificacoes`
2. **Usar agregações pré-calculadas quando possível:**
   - `tb_alertas` já tem `vl_incidencia`, `vl_rt` calculados.
   - Para o dashboard, consuma da Gold Layer que já uniu `tb_alertas` e `tb_municipios`.
3. **Partition Pruning:**
   - O Athena automaticamente pula partições que não estão na cláusula WHERE. Filtros como `WHERE nr_ano_epi = 2026` reduzem o custo massivamente.

---

## Consultas Frequentes (Cheat Sheet - Silver)

### 1. Alertas Críticos (Nível 4) na Última Semana
```sql
SELECT m.nm_municipio, a.ds_doenca, a.vl_incidencia, a.nr_nivel_alerta
FROM tb_alertas a
INNER JOIN tb_municipios m ON a.cd_geocode = m.cd_geocode
WHERE a.dt_semana_epidemiologica = (SELECT MAX(dt_semana_epidemiologica) FROM tb_alertas)
  AND a.nr_nivel_alerta = 4
ORDER BY a.vl_incidencia DESC;
```

### 2. Série Histórica - Dengue nos Últimos 3 Meses
```sql
SELECT
  DATE_TRUNC('week', a.dt_semana_epidemiologica) as semana,
  m.nm_municipio,
  SUM(a.vl_casos_estimados) as total_casos,
  AVG(a.vl_incidencia) as incidencia_media
FROM tb_alertas a
INNER JOIN tb_municipios m ON a.cd_geocode = m.cd_geocode
WHERE a.ds_doenca = 'dengue'
  AND a.dt_semana_epidemiologica >= CURRENT_DATE - INTERVAL 12 WEEK
GROUP BY DATE_TRUNC('week', a.dt_semana_epidemiologica), m.nm_municipio
ORDER BY semana DESC, total_casos DESC;
```

### 3. Comparar Temperaturas em Semanas com Alto Rt
```sql
SELECT
  m.nm_municipio,
  a.dt_semana_epidemiologica,
  a.vl_rt,
  a.vl_temp_min,
  a.vl_temp_max
FROM tb_alertas a
INNER JOIN tb_municipios m ON a.cd_geocode = m.cd_geocode
WHERE a.ds_doenca = 'dengue'
  AND a.vl_rt > 1.0
  AND a.nr_ano_epi = YEAR(CURRENT_DATE)
ORDER BY a.vl_rt DESC;
```
