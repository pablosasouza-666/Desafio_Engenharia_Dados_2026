# Qualidade do Ar × Clima nas Capitais Brasileiras

Pipeline de dados que extrai, transforma, valida e disponibiliza dados diários de **poluição do ar (PM2.5)** e **clima** de 6 capitais brasileiras, para responder: **o clima (chuva, vento, temperatura) influencia a poluição do ar?**

Projeto desenvolvido para o **Desafio Python – FT03 – Projeto 1** (AI/R – Agentic AI Engineering), em **Databricks Free Edition** com **PySpark + Spark SQL**.

---

## Sumário
1. [Arquitetura](#arquitetura)
2. [Como executar](#como-executar)
3. [Variáveis de ambiente](#variáveis-de-ambiente)
4. [APIs utilizadas](#apis-utilizadas)
5. [Estrutura do repositório](#estrutura-do-repositório)
6. [Transformações](#transformações)
7. [Data quality](#data-quality)
8. [Saída final](#saída-final)
9. [Resultados](#resultados)
10. [Limitações](#limitações)

---

## Arquitetura

Arquitetura **medalhão** em três camadas, gravadas num Volume do Unity Catalog (`/Volumes/workspace/desafio_ft03/dados`):

```
 OpenAQ (poluição) ──┐
                     ├─► BRONZE ──► SILVER ──► GOLD ──► DATA QUALITY ──► output/ (Parquet + relatório)
 NASA POWER (clima) ─┘   JSON bruto  limpo     cruzado    validações
```

| Camada | Conteúdo |
|---|---|
| **Bronze** | JSON exatamente como veio das APIs, um arquivo por estação/sensor |
| **Silver** | Dados desaninhados, tipados, sem nulos inválidos e sem duplicatas |
| **Gold** | Poluição e clima cruzados por estação + dia, com métricas derivadas |

---

## Como executar

**Pré-requisitos:** conta no Databricks (Free Edition ou superior, com Serverless e Unity Catalog) e uma chave gratuita do OpenAQ (cadastro em https://explore.openaq.org).

1. **Clone o repositório no Databricks:** Workspace → Create → Git folder → cole a URL deste repositório.
2. **Crie o arquivo de credenciais** `key.env` **na pasta acima do repositório** (ou seja, ao lado da pasta clonada, nunca dentro dela), usando o `.env.example` como modelo:
   ```
   OPENAQ_API_KEY=sua_chave_aqui
   ```
3. **Abra o notebook `main`** na raiz do repositório e clique em **Run all**.

O `main` executa todas as etapas em sequência (cria schema e Volume, extrai, transforma, valida e grava). Leva poucos minutos; a maior parte é a extração, que respeita o limite de requisições da API.

**Ambiente testado:** Databricks Serverless, Python 3.12, Spark 4.3. As bibliotecas extras estão em `requirements.txt`, com versões fixadas; o PySpark já vem no Databricks.

---

## Variáveis de ambiente

| Variável | Obrigatória | Descrição |
|---|---|---|
| `OPENAQ_API_KEY` | Sim | Chave da API do OpenAQ, enviada no cabeçalho `X-API-Key` |

As credenciais são lidas com `os.getenv` depois de carregar o `key.env` (`python-dotenv`). O arquivo fica **fora** da pasta do repositório, então não há como ser versionado por engano. O `.env.example` documenta as variáveis sem valores reais. A NASA POWER não exige chave.

---

## APIs utilizadas

### OpenAQ v3 (qualidade do ar)
Agregador de dados de estações de monitoramento do ar do mundo todo.

| Item | Valor |
|---|---|
| URL base | `https://api.openaq.org/v3` |
| Autenticação | cabeçalho `X-API-Key` |
| `GET /locations/{id}` | metadados da estação: coordenadas e lista de sensores (array aninhado) |
| `GET /sensors/{id}/days` | médias diárias de um sensor; parâmetros `date_from`, `date_to`, `limit`, `page` |
| Limite | plano gratuito com limite de requisições por minuto; o pipeline espaça as chamadas em 1,1 s |

### NASA POWER (clima)
Dados meteorológicos diários modelados a partir de satélite e reanálise (MERRA-2), em grade global.

| Item | Valor |
|---|---|
| URL | `https://power.larc.nasa.gov/api/temporal/daily/point` |
| Autenticação | nenhuma |
| Parâmetros | `parameters`, `community`, `latitude`, `longitude`, `start`, `end` (AAAAMMDD), `format` |
| Variáveis usadas | `T2M` (temperatura °C), `PRECTOTCORR` (chuva mm), `RH2M` (umidade %), `WS10M` (vento m/s) |
| Valor ausente | `-999`, convertido para nulo na silver |

### Resiliência das chamadas (`src/http_client.py`)
- **Rate limiting:** intervalo mínimo configurável entre chamadas.
- **Retentativas com backoff exponencial** (2 s, 4 s, 8 s...) para falhas temporárias: erros de rede, timeout e status 429/500/502/503/504.
- **Respeito ao `Retry-After`** enviado pela API, com **teto de 60 s**: acima disso, falha rápido em vez de travar o pipeline.
- **Erros definitivos** (401, 404) não são repetidos.
- **Falha isolada por estação:** se uma estação falha, o erro é registrado no relatório e as demais continuam.

---

## Estrutura do repositório

```
├── main                      # notebook orquestrador: executa o pipeline ponta a ponta
├── src/                      # código modular, um arquivo por etapa
│   ├── __init__.py           # transforma a pasta em pacote Python importável
│   ├── config.py             # configurações centrais: estações, período, URLs e caminhos
│   ├── http_client.py        # cliente HTTP com rate limit, retentativas e backoff
│   ├── extract.py            # extração das APIs e gravação do JSON bruto (bronze)
│   ├── transform.py          # desaninhamento, limpeza, cruzamento e enriquecimento (silver e gold)
│   ├── quality.py            # validações de data quality e relatório
│   └── load.py               # gravação em Parquet particionado e cópia para output/
├── output/
│   ├── qualidade_ar/         # dataset final em Parquet + Snappy, particionado por ano/mês
│   └── relatorio_qualidade.json
├── docs/
│   └── decisoes.md           # registro de todas as decisões técnicas e seus motivos
├── .env.example              # modelo das variáveis de ambiente (sem valores)
├── requirements.txt          # bibliotecas com versões fixadas
└── README.md
```

**Por que um arquivo por etapa:** cada módulo tem uma única responsabilidade. Se uma API muda, só o `extract.py` muda; se uma regra de qualidade muda, só o `quality.py`. Isso facilita ler, testar e manter. Tudo que pode variar (estações, período, caminhos) fica no `config.py`, sem precisar mexer na lógica.

---

## Transformações

As transformações combinam **PySpark** e **Spark SQL**. O desaninhamento e o cruzamento entre fontes são feitos em SQL; o restante, em PySpark.

| # | Transformação | Onde | Como |
|---|---|---|---|
| — | **Desaninhamento** | silver | `LATERAL VIEW EXPLODE` sobre o array de medições do OpenAQ e **duplo EXPLODE** sobre o mapa de mapas da NASA (`{variável: {data: valor}}`), lido com **schema explícito** (`MapType`) |
| 1 | **Padronização de tipos e datas** | silver | casts explícitos; data extraída do horário **local** de cada cidade (evita deslocar o dia ao converter para UTC); parâmetros em minúsculas |
| 2 | **Tratamento de nulos** | silver | descarta medições sem valor ou data; cobertura ausente vira 0; `-999` da NASA vira nulo; valores negativos descartados (exceto temperatura) |
| 3 | **Deduplicação** | silver | `ROW_NUMBER()` por estação + parâmetro + dia, mantendo a linha de maior cobertura |
| 4 | **Cruzamento de fontes (JOIN)** | gold | clima como base com `LEFT JOIN` da poluição: dias sem medição ficam visíveis como nulos em vez de sumirem |
| 5 | **Enriquecimento de métricas** | gold | categoria da qualidade do ar pela OMS, flag de dia chuvoso, diferença entre a temperatura do sensor e a da NASA, colunas de partição |

**Critérios do enriquecimento:**
- **Categoria do ar (PM2.5 diário, OMS 2021):** Boa ≤ 15 µg/m³ (diretriz da OMS); Moderada ≤ 25; Ruim ≤ 50; Muito ruim > 50 (metas intermediárias da OMS).
- **Dia chuvoso:** chuva ≥ 1 mm (convenção meteorológica).
- **Diferença de temperatura sensor − NASA:** compara a medição local com o modelo regional; indica ilha de calor urbana ou descalibração do sensor.

---

## Data quality

Executado automaticamente pelo `main` sobre a gold. O resultado é salvo em `output/relatorio_qualidade.json`.

| Verificação | O que testa | Reprova quando |
|---|---|---|
| **Schema** | colunas e tipos da gold contra um contrato declarado | falta coluna ou o tipo diverge |
| **Null ratio** | proporção de nulos de todas as colunas, numa única leitura | coluna obrigatória (estação, cidade, data, ano, mês) tem nulo |
| **Unicidade** | chave estação + dia | existe chave repetida |
| **Intervalos** | faixas fisicamente plausíveis (ex.: umidade 0–100%, PM2.5 0–500 µg/m³) | algum valor fora da faixa |
| **Cobertura de PM2.5** | % de dias com medição por cidade | **alerta** (não reprova) abaixo de 50% |

**Status:** cada verificação retorna `aprovado`, `alerta` ou `reprovado`; o relatório consolida um status geral.
- **Reprovado** indica defeito no pipeline, e a gravação é **cancelada** (portão de qualidade).
- **Alerta** indica limitação da fonte; os dados são gravados e a limitação fica registrada.

---

## Saída final

- **Formato:** Parquet com compressão Snappy.
- **Particionamento:** `ano=AAAA/mes=M`, um arquivo por mês (11 partições, de dez/2025 a out/2026).
- **Grão:** uma linha por capital + dia (2.010 linhas).

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_estacao` | int | id da estação no OpenAQ |
| `cidade` | string | capital |
| `data` | date | dia (horário local) |
| `pm25_ugm3` | double | média diária de PM2.5 (µg/m³) |
| `pm10_ugm3` | double | média diária de PM10 (só Rio de Janeiro) |
| `pct_completo_pm25` | double | % das horas do dia com medição de PM2.5 |
| `temperatura_sensor_c` | double | temperatura medida no sensor (°C) |
| `umidade_sensor_pct` | double | umidade medida no sensor (%) |
| `temperatura_nasa_c` | double | temperatura modelada pela NASA (°C) |
| `chuva_mm` | double | chuva acumulada no dia (mm) |
| `umidade_nasa_pct` | double | umidade modelada pela NASA (%) |
| `vento_ms` | double | velocidade média do vento (m/s) |
| `categoria_ar` | string | Boa / Moderada / Ruim / Muito ruim |
| `acima_diretriz_oms` | boolean | PM2.5 acima de 15 µg/m³ |
| `dia_chuvoso` | boolean | chuva ≥ 1 mm |
| `dif_temp_sensor_nasa_c` | double | temperatura do sensor − temperatura da NASA |
| `ano`, `mes` | int | colunas de partição |

---

## Resultados

| Capital | PM2.5 médio (µg/m³) | Dias acima da diretriz OMS | Dias com medição |
|---|---|---|---|
| São Paulo | 13,5 | 134 | 331 |
| Manaus | 12,2 | 66 | 310 |
| Porto Velho | 10,5 | 82 | 305 |
| Vitória | 9,3 | 2 | 53 ⚠️ |
| Rio de Janeiro | 6,5 | 14 | 331 |
| Salvador | 1,7 | 9 | 329 |

São Paulo concentra o maior número de dias acima da diretriz da OMS. Manaus e Porto Velho vêm em seguida, possivelmente pela temporada de queimadas na Amazônia. Rio e Salvador apresentam o ar mais limpo.

---

## Limitações

- **Cobertura geográfica:** apenas 6 das 27 capitais têm sensores de PM2.5 ativos no OpenAQ no período; as redes oficiais brasileiras pararam de enviar dados ao OpenAQ em 2023.
- **Salvador:** a estação fica em Lauro de Freitas, município vizinho na região metropolitana.
- **Vitória:** só 15,8% dos dias têm medição (sinalizado como alerta no data quality); conclusões sobre a cidade são frágeis.
- **Salvador com média muito baixa (1,7 µg/m³):** valor plausível para área litorânea com vento constante, mas também compatível com sensor descalibrado; não foi possível confirmar.
- **Representatividade:** cada estação mede o ar no seu entorno imediato, não a média da cidade.
- **Clima modelado:** a NASA POWER estima o clima numa grade de cerca de 50 km, não medido no local da estação.
- **Sensores de baixo custo:** 5 das 6 estações são sensores de baixo custo, menos precisos que estações oficiais.

---

## Decisões técnicas

Todas as decisões e seus motivos estão em [`docs/decisoes.md`](docs/decisoes.md).