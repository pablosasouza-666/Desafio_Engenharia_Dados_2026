# permite anotações de tipo modernas em qualquer Python 3
from __future__ import annotations

# logging: registra mensagens de execução
import logging
# os: percorre as pastas geradas pelo Spark
import os
# shutil: copia e apaga arquivos/pastas
import shutil
# Path: manipula caminhos de forma segura
from pathlib import Path

# DataFrame: tipo da tabela do Spark; SparkSession: conexão com o Spark (usada para os comandos SQL de descrição)
from pyspark.sql import DataFrame, SparkSession

# config: caminhos centrais do projeto
from src import config

# logger com o nome deste módulo
logger = logging.getLogger(__name__)

# pasta da gold no Volume (onde o Spark grava)
PASTA_GOLD: str = f"{config.VOLUME}/gold/qualidade_ar"
# pasta de destino dentro do repositório (entregável no GitHub)
PASTA_OUTPUT_GOLD: Path = config.PASTA_OUTPUT / "qualidade_ar"


# ===== GRAVAÇÃO: salva a gold em Parquet particionado =====
def gravar_gold(df: DataFrame, caminho: str = PASTA_GOLD) -> None:
    (
        df
        # agrupa os dados por ano/mês antes de gravar: um arquivo por partição
        .repartition("ano", "mes")
        .write
        # substitui a execução anterior em vez de acumular (idempotência)
        .mode("overwrite")
        # compressão Snappy: rápida de ler e escrever (exigência do desafio)
        .option("compression", "snappy")
        # cria pastas ano=AAAA/mes=M (particionamento temporal)
        .partitionBy("ano", "mes")
        # formato colunar Parquet
        .parquet(caminho)
    )
    # registra onde a gold foi gravada
    logger.info("Gold gravada em %s", caminho)


# ===== CÓPIA: leva os .parquet do Volume para a pasta output/ do repositório =====
def copiar_para_output(origem: str = PASTA_GOLD, destino: Path = PASTA_OUTPUT_GOLD) -> int:
    # apaga o conteúdo antigo do destino, para não misturar execuções
    if destino.exists():
        shutil.rmtree(destino)
    # contador de arquivos copiados
    copiados = 0
    # percorre todas as subpastas geradas pelo Spark (ano=.../mes=...)
    for raiz, _, arquivos in os.walk(origem):
        # para cada arquivo da subpasta atual
        for nome in arquivos:
            # ignora arquivos de controle do Spark; só interessa o .parquet
            if not nome.endswith(".parquet"):
                continue
            # caminho relativo da subpasta (ex.: "ano=2025/mes=12"), para manter a estrutura
            relativo = os.path.relpath(raiz, origem)
            # pasta correspondente no destino
            pasta_destino = destino / relativo
            # cria a pasta, se não existir
            pasta_destino.mkdir(parents=True, exist_ok=True)
            # copia o conteúdo do arquivo
            shutil.copyfile(os.path.join(raiz, nome), pasta_destino / nome)
            # conta mais um arquivo copiado
            copiados += 1
    # registra quantos arquivos foram para o output/
    logger.info("%d arquivos copiados para %s", copiados, destino)
    # devolve o total copiado
    return copiados

    # ===== DESCRIÇÕES QUE APARECEM NO CATALOG =====
# descrição de cada tabela (sem aspas simples no texto, porque vão dentro de um comando SQL)
DESCRICOES_TABELAS: dict[str, str] = {
    "silver_medicoes": "Medições diárias do OpenAQ desaninhadas, tipadas, sem nulos inválidos e sem duplicatas. Uma linha por estação, parâmetro e dia.",
    "silver_clima": "Clima diário da NASA POWER desaninhado e pivotado. Uma linha por estação e dia.",
    "gold_qualidade_ar": "Poluição do ar e clima cruzados por estação e dia, com categoria OMS, dia chuvoso e diferença de temperatura. Tabela final de análise.",
}

# descrição de cada coluna da gold
DESCRICOES_COLUNAS_GOLD: dict[str, str] = {
    "id_estacao": "Id da estação no OpenAQ",
    "cidade": "Capital da estação",
    "data": "Dia da medição, no horário local da cidade",
    "pm25_ugm3": "Média diária de PM2.5 em µg/m³ (OpenAQ)",
    "pm10_ugm3": "Média diária de PM10 em µg/m³ (só Rio de Janeiro)",
    "pct_completo_pm25": "Percentual das horas do dia com medição de PM2.5",
    "temperatura_sensor_c": "Temperatura medida no sensor em °C",
    "umidade_sensor_pct": "Umidade medida no sensor em %",
    "temperatura_nasa_c": "Temperatura média modelada pela NASA POWER em °C",
    "chuva_mm": "Chuva acumulada no dia em mm (NASA POWER)",
    "umidade_nasa_pct": "Umidade relativa média em % (NASA POWER)",
    "vento_ms": "Velocidade média do vento em m/s (NASA POWER)",
    "categoria_ar": "Categoria pela OMS 2021: Boa, Moderada, Ruim ou Muito ruim",
    "acima_diretriz_oms": "Verdadeiro quando o PM2.5 passa de 15 µg/m³",
    "dia_chuvoso": "Verdadeiro quando choveu 1 mm ou mais",
    "dif_temp_sensor_nasa_c": "Temperatura do sensor menos a da NASA em °C",
    "ano": "Ano da data",
    "mes": "Mês da data",
    "_processado_em": "Momento em que a linha foi processada",
    "_id_execucao": "Código da execução que gerou a linha; liga à tabela auditoria_execucoes",
    "_fonte": "API de origem do dado",
}


# ===== GRAVAÇÃO: salva um DataFrame como tabela no Unity Catalog =====
def gravar_tabela(
    spark: SparkSession,
    df: DataFrame,
    nome: str,
    descricoes_colunas: dict[str, str] | None = None,
) -> str:
    # nome completo em três partes: catálogo.schema.tabela
    tabela = f"{config.CATALOGO}.{config.SCHEMA}.{nome}"
    (
        df.write
        # substitui o conteúdo anterior (idempotência)
        .mode("overwrite")
        # aceita mudança de colunas entre execuções, em vez de dar erro
        .option("overwriteSchema", "true")
        # grava como tabela gerenciada (formato Delta, padrão do Unity Catalog)
        .saveAsTable(tabela)
    )
    # grava a descrição da tabela, que aparece na aba Overview do Catalog
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{DESCRICOES_TABELAS[nome]}'")
    # grava a descrição de cada coluna, que aparece na lista de colunas do Catalog
    for coluna, descricao in (descricoes_colunas or {}).items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{descricao}'")
    # registra no log a tabela gravada
    logger.info("Tabela gravada: %s", tabela)
    # devolve o nome completo da tabela
    return tabela