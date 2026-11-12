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

# DataFrame: tipo da tabela do Spark
from pyspark.sql import DataFrame

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