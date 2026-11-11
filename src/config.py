# permite anotações de tipo modernas em qualquer Python 3
from __future__ import annotations

# Path manipula caminhos de arquivo de forma segura entre sistemas
from pathlib import Path

# ---------------------------------------------------------------
# Estações de qualidade do ar (IDs do OpenAQ) usadas no projeto
# ---------------------------------------------------------------
# dicionário: id da estação no OpenAQ -> nome da capital
ESTACOES: dict[int, str] = {
    820323: "Rio de Janeiro",  # estação Centro Kunak (Tijuca não mede PM2.5)
    6139516: "São Paulo",
    6126992: "Vitória",  # sensor Quality01
    3911519: "Salvador",  # fica em Lauro de Freitas, região metropolitana
    5040030: "Manaus",
    5616182: "Porto Velho",
}

# ---------------------------------------------------------------
# Período de análise (formato ISO: AAAA-MM-DD)
# ---------------------------------------------------------------
# período comum em que as 6 estações têm dados (11 meses)
DATA_INICIO: str = "2025-12-01"
DATA_FIM: str = "2026-10-31"

# ---------------------------------------------------------------
# Endereços base das APIs
# ---------------------------------------------------------------
# API de qualidade do ar (exige chave)
OPENAQ_BASE_URL: str = "https://api.openaq.org/v3"
# API de clima diário histórico da NASA POWER (sem chave; agência governamental)
NASA_POWER_BASE_URL: str = "https://power.larc.nasa.gov/api/temporal/daily"

# ---------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------
# raiz do repositório: a pasta acima de src/ (parents[1] sobe um nível a partir deste arquivo)
RAIZ_REPO: Path = Path(__file__).resolve().parents[1]
# arquivo de credenciais: fica uma pasta ACIMA do repositório, para nunca ir para o Git
ARQUIVO_ENV: Path = RAIZ_REPO.parent / "key.env"
# pasta de saída final versionada no GitHub (entregável obrigatório)
PASTA_OUTPUT: Path = RAIZ_REPO / "output"

# Volume do Databricks onde o Spark grava as camadas bronze/silver/gold
VOLUME: str = "/Volumes/workspace/desafio_ft03/dados"