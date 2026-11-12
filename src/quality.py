# permite anotações de tipo modernas em qualquer Python 3
from __future__ import annotations

# json: grava o relatório em arquivo JSON
import json
# os: cria a pasta do relatório
import os
# datetime/timezone: carimbo de quando o relatório foi gerado
from datetime import datetime, timezone
# Any = qualquer tipo
from typing import Any

# DataFrame: tipo da tabela do Spark; F: funções de coluna (como funções SQL)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ===== CONTRATO DA GOLD: colunas e tipos esperados =====
# é o "CREATE TABLE" que a tabela final precisa respeitar
ESQUEMA_GOLD: dict[str, str] = {
    "id_estacao": "int",
    "cidade": "string",
    "data": "date",
    "pm25_ugm3": "double",
    "pm10_ugm3": "double",
    "pct_completo_pm25": "double",
    "temperatura_sensor_c": "double",
    "umidade_sensor_pct": "double",
    "temperatura_nasa_c": "double",
    "chuva_mm": "double",
    "umidade_nasa_pct": "double",
    "vento_ms": "double",
    "categoria_ar": "string",
    "acima_diretriz_oms": "boolean",
    "dia_chuvoso": "boolean",
    "dif_temp_sensor_nasa_c": "double",
    "ano": "int",
    "mes": "int",
}

# ===== COLUNAS QUE NÃO PODEM TER NULO =====
# se alguma tiver nulo, o pipeline está com defeito (reprova)
COLUNAS_OBRIGATORIAS: list[str] = ["id_estacao", "cidade", "data", "ano", "mes"]

# ===== CHAVE ÚNICA DA GOLD =====
# só pode existir uma linha por estação + dia (como uma PRIMARY KEY)
CHAVE_GOLD: list[str] = ["id_estacao", "data"]

# ===== FAIXAS FISICAMENTE PLAUSÍVEIS =====
# coluna -> (mínimo, máximo); valor fora disso indica erro de sensor ou de transformação
INTERVALOS: dict[str, tuple[float, float]] = {
    "pm25_ugm3": (0.0, 500.0),  # média diária acima de 500 µg/m³ é implausível
    "pm10_ugm3": (0.0, 600.0),
    "pct_completo_pm25": (0.0, 100.0),  # percentual
    "temperatura_sensor_c": (-10.0, 50.0),  # faixa realista para capitais brasileiras
    "temperatura_nasa_c": (-10.0, 50.0),
    "umidade_sensor_pct": (0.0, 100.0),  # percentual
    "umidade_nasa_pct": (0.0, 100.0),
    "chuva_mm": (0.0, 300.0),  # chuva diária acima de 300 mm é extrema
    "vento_ms": (0.0, 40.0),  # média diária acima de 40 m/s seria furacão
}

# cobertura mínima de PM2.5 por cidade; abaixo disso, o relatório emite alerta
COBERTURA_MINIMA_PM25: float = 0.5


# ===== VERIFICAÇÃO 1: schema =====
def verificar_schema(df: DataFrame) -> dict[str, Any]:
    # df.dtypes devolve pares (coluna, tipo); vira dicionário coluna -> tipo
    real = dict(df.dtypes)
    # colunas do contrato que não existem na tabela
    faltando = [c for c in ESQUEMA_GOLD if c not in real]
    # colunas que existem, mas com tipo diferente do contrato
    tipo_errado = {c: real[c] for c in ESQUEMA_GOLD if c in real and real[c] != ESQUEMA_GOLD[c]}
    # colunas que existem na tabela mas não estão no contrato
    sobrando = [c for c in real if c not in ESQUEMA_GOLD]
    # aprovado só se não faltar coluna nem houver tipo errado
    status = "aprovado" if not faltando and not tipo_errado else "reprovado"
    # devolve o resultado da verificação
    return {"status": status, "faltando": faltando, "tipo_errado": tipo_errado, "sobrando": sobrando}


# ===== VERIFICAÇÃO 2: null ratio =====
def verificar_nulos(df: DataFrame) -> dict[str, Any]:
    # para cada coluna, isNull vira 1/0 e a média dá a proporção de nulos; tudo numa única leitura
    proporcoes = df.select(
        [F.avg(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).first().asDict()
    # arredonda para 4 casas decimais (ex.: 0.1234 = 12,34%)
    proporcoes = {c: round(v, 4) for c, v in proporcoes.items()}
    # obrigatórias com qualquer nulo
    violacoes = [c for c in COLUNAS_OBRIGATORIAS if proporcoes.get(c, 0) > 0]
    # reprova se alguma obrigatória tiver nulo
    status = "aprovado" if not violacoes else "reprovado"
    # devolve status, proporção por coluna e as violações
    return {"status": status, "null_ratio": proporcoes, "obrigatorias_com_nulo": violacoes}


# ===== VERIFICAÇÃO 3: unicidade da chave =====
def verificar_unicidade(df: DataFrame) -> dict[str, Any]:
    # total de linhas da tabela
    total = df.count()
    # quantas combinações distintas de estação + dia existem
    distintas = df.select(CHAVE_GOLD).distinct().count()
    # duplicadas = linhas a mais que o número de chaves distintas
    duplicadas = total - distintas
    # aprovado se nenhuma chave se repete
    status = "aprovado" if duplicadas == 0 else "reprovado"
    # devolve o resultado
    return {"status": status, "chave": CHAVE_GOLD, "total": total, "distintas": distintas, "duplicadas": duplicadas}


# ===== VERIFICAÇÃO 4: testes de intervalo =====
def verificar_intervalos(df: DataFrame) -> dict[str, Any]:
    # conta, numa única leitura, quantos valores de cada coluna caem fora da faixa (nulos não contam)
    fora = df.select(
        [
            F.sum(F.when((F.col(c) < minimo) | (F.col(c) > maximo), 1).otherwise(0)).alias(c)
            for c, (minimo, maximo) in INTERVALOS.items()
        ]
    ).first().asDict()
    # mantém só as colunas com pelo menos uma violação
    violacoes = {c: n for c, n in fora.items() if n}
    # reprova se houver qualquer valor fora da faixa
    status = "aprovado" if not violacoes else "reprovado"
    # devolve o resultado, incluindo as faixas usadas (transparência)
    return {"status": status, "faixas": INTERVALOS, "fora_da_faixa": violacoes}


# ===== VERIFICAÇÃO 5: cobertura de PM2.5 por cidade =====
def verificar_cobertura(df: DataFrame) -> dict[str, Any]:
    # por cidade: dias com PM2.5 / total de dias (COUNT de coluna ignora nulos; COUNT(*) não)
    linhas = (
        df.groupBy("cidade")
        .agg(F.round(F.count("pm25_ugm3") / F.count(F.lit(1)), 4).alias("cobertura"))
        .orderBy("cidade")
        .collect()  # traz o resultado (6 linhas) do Spark para o Python
    )
    # vira dicionário cidade -> cobertura
    cobertura = {linha["cidade"]: linha["cobertura"] for linha in linhas}
    # cidades abaixo da cobertura mínima
    baixas = [c for c, v in cobertura.items() if v < COBERTURA_MINIMA_PM25]
    # cobertura baixa não invalida o pipeline: é um alerta sobre a fonte, não um defeito nosso
    status = "aprovado" if not baixas else "alerta"
    # devolve o resultado
    return {"status": status, "minimo": COBERTURA_MINIMA_PM25, "por_cidade": cobertura, "abaixo_do_minimo": baixas}


# ===== RELATÓRIO: executa todas as verificações e consolida =====
def gerar_relatorio(gold: DataFrame, resumo_extracao: dict[str, Any] | None = None) -> dict[str, Any]:
    # executa as cinco verificações
    verificacoes = {
        "schema": verificar_schema(gold),
        "nulos": verificar_nulos(gold),
        "unicidade": verificar_unicidade(gold),
        "intervalos": verificar_intervalos(gold),
        "cobertura_pm25": verificar_cobertura(gold),
    }
    # junta os status individuais para decidir o status geral
    status = [v["status"] for v in verificacoes.values()]
    # qualquer reprovação reprova tudo; senão, qualquer alerta vira alerta; senão, aprovado
    if "reprovado" in status:
        geral = "reprovado"
    elif "alerta" in status:
        geral = "alerta"
    else:
        geral = "aprovado"
    # monta o relatório final
    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),  # quando o relatório foi gerado
        "status_geral": geral,  # resultado consolidado
        "extracao": resumo_extracao,  # resumo da extração (estações, sensores, falhas)
        "linhas_gold": verificacoes["unicidade"]["total"],  # volume da tabela final
        "verificacoes": verificacoes,  # detalhe de cada verificação
    }


# ===== GRAVAÇÃO: salva o relatório como JSON =====
def salvar_relatorio(relatorio: dict[str, Any], caminho: str) -> None:
    # cria a pasta, se não existir
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    # grava com indentação (legível) e acentos preservados; default=str converte datas em texto
    with open(caminho, "w", encoding="utf-8") as arquivo:
        json.dump(relatorio, arquivo, ensure_ascii=False, indent=2, default=str)