# permite anotações de tipo modernas em qualquer Python 3
from __future__ import annotations

# DataFrame: tipo da "tabela" do Spark; SparkSession: a conexão com o motor Spark; Window: janelas (OVER)
from pyspark.sql import DataFrame, SparkSession, Window
# functions (apelidado de F): funções de coluna do Spark, como as funções do SQL (CAST, LOWER, ROW_NUMBER...)
from pyspark.sql import functions as F

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