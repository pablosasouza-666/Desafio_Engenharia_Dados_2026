# permite anotações de tipo modernas em qualquer Python 3
from __future__ import annotations

# DataFrame: tipo da "tabela" do Spark; SparkSession: a conexão com o motor Spark; Window: janelas (OVER)
from pyspark.sql import DataFrame, SparkSession, Window
# functions (apelidado de F): funções de coluna do Spark, como as funções do SQL (CAST, LOWER, ROW_NUMBER...)
from pyspark.sql import functions as F

# tipos do Spark para declarar o schema explicitamente (como as colunas de um CREATE TABLE)
from pyspark.sql.types import DoubleType, LongType, MapType, StringType, StructField, StructType

# config: caminhos e configurações centrais do projeto
from src import config

# pasta da camada bronze dentro do Volume
PASTA_BRONZE: str = f"{config.VOLUME}/bronze"


# ===== FUNÇÃO AUXILIAR: lê uma pasta de JSONs da bronze como uma tabela =====
def ler_json(spark: SparkSession, caminho: str) -> DataFrame:
    # multiLine=True: cada arquivo é UM objeto JSON com quebras de linha (não uma linha por registro)
    return spark.read.option("multiLine", True).json(caminho)


# ===== FUNÇÃO PRINCIPAL: gera a silver de medições do OpenAQ =====
def silver_medicoes(spark: SparkSession) -> DataFrame:
    # ===== BLOCO 1: lê os JSONs brutos dos sensores =====
    # lê os 29 arquivos da pasta de medições como uma tabela (1 linha por sensor)
    bruto = ler_json(spark, f"{PASTA_BRONZE}/openaq/medicoes/")
    # registra como view temporária para o SQL do próximo bloco enxergar
    bruto.createOrReplaceTempView("bronze_medicoes")

    # ===== BLOCO 2: desaninha o array de medições (EXPLODE em SQL) =====
    # cada linha de sensor tem um array "results" com ~330 dias; o EXPLODE vira 1 linha por dia
    explodido = spark.sql("""
        SELECT
          id_estacao,                                    -- estação (chave do JOIN)
          sensor.id                   AS id_sensor,      -- id do sensor
          sensor.parameter.name       AS parametro,      -- poluente/variável (pm25, temperature...)
          sensor.parameter.units      AS unidade,        -- unidade de medida (µg/m³, °C...)
          m.period.datetimeFrom.local AS inicio_local,   -- início do dia no horário local da cidade
          m.value                     AS valor,          -- média do dia
          m.coverage.percentComplete  AS pct_completo,   -- % do dia com medição
          _extraido_em                AS extraido_em     -- quando o dado foi baixado (rastreabilidade)
        FROM bronze_medicoes
        LATERAL VIEW EXPLODE(results) r AS m             -- 1 linha com array de N itens vira N linhas
    """)

    # ===== BLOCO 3: padroniza tipos e datas (transformação 1) =====
    # select com cast: equivale a um SELECT CAST(... AS ...) em SQL
    tipado = explodido.select(
        F.col("id_estacao").cast("int").alias("id_estacao"),  # garante número inteiro
        F.col("id_sensor").cast("long").alias("id_sensor"),  # id grande: usa long (BIGINT)
        F.lower(F.trim(F.col("parametro"))).alias("parametro"),  # tira espaços e põe em minúsculas
        F.col("unidade").alias("unidade"),  # mantém a unidade como veio
        F.to_date(F.substring(F.col("inicio_local"), 1, 10)).alias("data"),  # pega "AAAA-MM-DD" do texto e vira DATE
        F.col("valor").cast("double").alias("valor"),  # garante número decimal
        F.col("pct_completo").cast("double").alias("pct_completo"),  # garante número decimal
        F.to_timestamp(F.col("extraido_em")).alias("extraido_em"),  # texto ISO vira TIMESTAMP
    )

    # ===== BLOCO 4: trata nulos e valores inválidos (transformação 2) =====
    valido = (
        tipado
        # cobertura ausente vira 0 (sem informação = assume 0% do dia medido)
        .fillna({"pct_completo": 0.0})
        # descarta linhas sem valor ou sem data: não há o que analisar nelas
        .filter(F.col("valor").isNotNull() & F.col("data").isNotNull())
        # descarta valores negativos, exceto temperatura (que pode ser negativa de verdade)
        .filter((F.col("parametro") == "temperature") | (F.col("valor") >= 0))
    )

    # ===== BLOCO 5: remove duplicidades (transformação 3) =====
    # janela = PARTITION BY estação, poluente e dia; ORDER BY maior cobertura primeiro
    janela = Window.partitionBy("id_estacao", "parametro", "data").orderBy(
        F.col("pct_completo").desc(),  # prioriza a linha com mais horas medidas no dia
        F.col("id_sensor"),  # desempate fixo, para o resultado ser sempre o mesmo
    )
    # numera as linhas dentro de cada grupo e fica só com a primeira (o QUALIFY ROW_NUMBER() = 1)
    deduplicado = (
        valido
        .withColumn("ordem", F.row_number().over(janela))  # cria a coluna com o número da linha no grupo
        .filter(F.col("ordem") == 1)  # mantém só a melhor linha de cada grupo
        .drop("ordem")  # remove a coluna auxiliar
    )

    # devolve a tabela silver de medições
    return deduplicado

    # ===== SCHEMA EXPLÍCITO DO JSON DA NASA POWER =====
# declara só os campos que usamos; properties.parameter vira MAPA de mapas: {variável: {data: valor}}
SCHEMA_NASA = StructType([
    StructField("id_estacao", LongType()),  # estação a que o clima pertence
    StructField("properties", StructType([  # bloco "properties" do JSON
        StructField("parameter", MapType(StringType(), MapType(StringType(), DoubleType()))),  # mapa de mapas
    ])),
])

# códigos da NASA -> nomes de colunas legíveis no nosso projeto
VARIAVEIS_NASA: dict[str, str] = {
    "T2M": "temperatura_c",  # temperatura média do dia (°C)
    "PRECTOTCORR": "chuva_mm",  # chuva acumulada no dia (mm)
    "RH2M": "umidade_pct",  # umidade relativa média (%)
    "WS10M": "vento_ms",  # velocidade média do vento (m/s)
}

# valor que a NASA usa para "sem dado"
VALOR_AUSENTE_NASA: float = -999.0


# ===== FUNÇÃO PRINCIPAL: gera a silver de clima da NASA POWER =====
def silver_clima(spark: SparkSession) -> DataFrame:
    # ===== BLOCO 1: lê os JSONs brutos com schema explícito =====
    # .schema(...) impõe a estrutura declarada, em vez de deixar o Spark adivinhar
    bruto = spark.read.schema(SCHEMA_NASA).option("multiLine", True).json(f"{PASTA_BRONZE}/nasa_power/")
    # registra como view temporária para o SQL enxergar
    bruto.createOrReplaceTempView("bronze_clima")

    # ===== BLOCO 2: duplo EXPLODE de mapas em SQL =====
    # 1º EXPLODE: abre o mapa externo -> uma linha por variável (T2M, RH2M...) com sua série
    # 2º EXPLODE: abre a série -> uma linha por data com o valor
    explodido = spark.sql("""
        SELECT
          id_estacao,                                             -- estação (chave do JOIN)
          variavel,                                               -- código da NASA (T2M, PRECTOTCORR...)
          data_txt,                                               -- data como texto AAAAMMDD
          valor                                                   -- valor do dia
        FROM bronze_clima
        LATERAL VIEW EXPLODE(properties.parameter) t1 AS variavel, serie  -- mapa externo: chave e valor
        LATERAL VIEW EXPLODE(serie) t2 AS data_txt, valor                 -- mapa interno: chave e valor
    """)

    # ===== BLOCO 3: padroniza tipos, datas e trata o código de ausência =====
    tipado = explodido.select(
        F.col("id_estacao").cast("int").alias("id_estacao"),  # garante inteiro, igual à silver de medições
        F.upper(F.trim(F.col("variavel"))).alias("variavel"),  # padroniza o código em maiúsculas
        F.to_date(F.col("data_txt"), "yyyyMMdd").alias("data"),  # "20251201" vira DATE 2025-12-01
        # -999 significa "sem dado": vira NULL (como um CASE WHEN valor = -999 THEN NULL ELSE valor END)
        F.when(F.col("valor") == VALOR_AUSENTE_NASA, None).otherwise(F.col("valor")).alias("valor"),
    )

    # ===== BLOCO 4: pivota as variáveis em colunas (uma linha por estação + dia) =====
    # groupBy + pivot: equivale a um PIVOT em SQL; F.first pega o único valor de cada célula
    largo = (
        tipado
        .groupBy("id_estacao", "data")  # uma linha por estação + dia (isso também deduplica)
        .pivot("variavel", list(VARIAVEIS_NASA.keys()))  # cada código vira uma coluna
        .agg(F.first("valor"))  # valor da célula
    )

    # ===== BLOCO 5: renomeia as colunas para nomes legíveis =====
    # percorre o dicionário e troca T2M -> temperatura_c, etc.
    for codigo, nome in VARIAVEIS_NASA.items():
        largo = largo.withColumnRenamed(codigo, nome)

    # devolve a silver de clima
    return largo