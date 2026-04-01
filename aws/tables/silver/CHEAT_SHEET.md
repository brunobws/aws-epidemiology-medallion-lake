# Silver Tables - Cheat Sheet

## Comparação Rápida

| Tabela | Granularidade | Records | Partition | Source | Update |
|--------|---------------|---------|-----------|--------|--------|
| **tb_municipios** | Município | 645 | None | IBGE API | Static |
| **tb_alertas** | Mun + Doença + Week | ~1,935/semana | ano_epi + data_semana | InfoDengue API | Weekly |
| **tb_populacao** | Município + Ano | 645/ano | ano_referencia | IBGE SIDRA | Annual |
| **tb_notificacoes** | Caso Individual | ~50-500/semana | dt_notific | SINAN CSV | Weekly |

---

## Quando Usar Cada Tabela

### Para Análise de Alertas Epidemiológicos → **tb_alertas**
- Nível de alerta por município e semana
- Estimativas de casos e taxa Rt
- Variáveis ambientais (temperatura, umidade)
- Dados pré-agregados e modelados ✓ (mais rápido)

### Para Validação de Casos Confirmados → **tb_notificacoes**
- Casos individuais notificados ao SINAN
- Dados brutos por paciente (desidentificado)
- Desfechos clínicos (cura, óbito, seqüela)
- Dados não agregados (mais flexível mas mais lento)

### Para Cálculos de Incidência → **tb_populacao**
- Denominador para fazer: `(casos / população) × 100.000`
- Sempre fazer LEFT JOIN com tb_alertas
- Atualiza 1x/ano (usar cache em modelo de BI)

### Para Dimensão Geográfica → **tb_municipios**
- Lookup de nomes, microregiões, mesorregiões
- Sempre usar com LEFT JOIN em tb_alertas ou tb_populacao
- Nunca modificar (tabela de referência)

---

## Queries Frequentes

### 1. Alertas Críticos (Nível 4) na Última Semana
```sql
SELECT m.nm_municipio, a.ds_doenca, a.vl_incidencia, a.nr_nivel_alerta
FROM tb_alertas a
INNER JOIN tb_municipios m ON a.cd_geocode = m.cd_geocode
WHERE a.dt_semana_epidemiologica = (SELECT MAX(dt_semana_epidemiologica) FROM tb_alertas)
  AND a.nr_nivel_alerta = 4
ORDER BY a.vl_incidencia DESC;
```

### 2. Municípios com Maior Incidência de Dengue (Última Semana)
```sql
SELECT m.nm_municipio, m.nm_mesorregiao, a.vl_incidencia, a.vl_casos_estimados
FROM tb_alertas a
INNER JOIN tb_municipios m ON a.cd_geocode = m.cd_geocode
WHERE a.ds_doenca = 'dengue'
  AND a.dt_semana_epidemiologica = (SELECT MAX(dt_semana_epidemiologica) FROM tb_alertas)
ORDER BY a.vl_incidencia DESC
LIMIT 10;
```

### 3. Série Histórica - Dengue nos Últimos 3 Meses
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

### 4. Comparar Temperaturas em Semanas com Alto Rt
```sql
SELECT
  m.nm_municipio,
  a.dt_semana_epidemiologica,
  a.vl_rt,
  a.vl_temp_min,
  a.vl_temp_max,
  a.vl_umid_min,
  a.vl_umid_max,
  a.vl_receptividade
FROM tb_alertas a
INNER JOIN tb_municipios m ON a.cd_geocode = m.cd_geocode
WHERE a.ds_doenca = 'dengue'
  AND a.vl_rt > 1.0
  AND a.nr_ano_epi = YEAR(CURRENT_DATE)
ORDER BY a.vl_rt DESC;
```

### 5. Casos Confirmados SINAN - Últimas 2 Semanas
```sql
SELECT
  id_agravo,
  COUNT(*) as total_notificacoes,
  SUM(CASE WHEN classi_fin = '10' THEN 1 ELSE 0 END) as confirmados,
  SUM(CASE WHEN evolucao = '3' THEN 1 ELSE 0 END) as obitos
FROM tb_notificacoes
WHERE sg_uf = 35
  AND dt_notific >= CURRENT_DATE - INTERVAL 14 DAY
GROUP BY id_agravo;
```

### 6. Demografia de Casos (SINAN) - Dengue SP
```sql
SELECT
  CASE
    WHEN nu_idade_n / 365.25 < 15 THEN '0-14'
    WHEN nu_idade_n / 365.25 < 30 THEN '15-29'
    WHEN nu_idade_n / 365.25 < 45 THEN '30-44'
    ELSE '45+'
  END as faixa_etaria,
  cs_sexo,
  COUNT(*) as casos,
  SUM(CASE WHEN evolucao = '3' THEN 1 ELSE 0 END) as obitos
FROM tb_notificacoes
WHERE sg_uf = 35
  AND id_agravo = 'A90'
  AND YEAR(dt_sin_pri) = YEAR(CURRENT_DATE)
GROUP BY faixa_etaria, cs_sexo;
```

---

## Tipos de Dados Importantes

### Enums e Códigos Padronizados

**Doenças:**
- `A90` = Dengue
- `A92` = Chikungunya
- `A92.8` = Zika

**Nível de Alerta (tb_alertas):**
- `1` = Verde (Seguro)
- `2` = Amarelo (Médio)
- `3` = Laranja (Alto)
- `4` = Vermelho (Crítico)

**Classificação Final (tb_notificacoes):**
- `10` = Confirmado (laboratorial ou clínico-laboratorial)
- `0` = Descartado / Outro
- Vazio = Em investigação

**Evolução (tb_notificacoes):**
- `1` = Cura esperada
- `2` = Cura com seqüela
- `3` = Óbito por doença
- `4` = Óbito por outra causa
- `9` = Óbito sob investigação
- Vazio = Caso em evolução

**Sexo (tb_notificacoes):**
- `M` = Masculino
- `F` = Feminino
- Vazio = Ignorado

---

## Performance Tips

1. **Sempre filtrar por data/partição primeiro**
   - `WHERE dt_semana_epidemiologica >= ...` para tb_alertas
   - `WHERE dt_notific >= ...` para tb_notificacoes

2. **Usar agregações pré-calculadas quando possível**
   - tb_alertas já tem vl_incidencia, vl_rt calculados
   - Evite re-calcular em queries

3. **Prefixar cadeias com cd_geocode (sem LEFT JOIN)**
   - Se só precisa de cd_geocode, não faça LEFT JOIN em tb_municipios
   - Join só quando precisar de nm_municipio ou hierarquia geográfica

4. **Cuidado ao joinar tb_notificacoes com tb_municipios**
   - id_municip ≠ cd_geocode (mapeamento necessário)
   - Preferir filtrar por cd_geocode em tb_alertas (já normalizado)

5. **Partition Pruning**
   - Hive/Athena automaticamente pula partições
   - Filtro `WHERE nr_ano_epi = 2026` significa scan apenas de 1 ano
