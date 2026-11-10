# permite anotações de tipo modernas em qualquer Python 3
from __future__ import annotations

# json: converte dicionários Python em texto JSON para salvar em arquivo
import json
# logging: registra mensagens de execução
import logging
# os: cria pastas e lê variáveis de ambiente
import os
# datetime/timezone: geram o carimbo de data/hora da extração em UTC
from datetime import datetime, timezone
# Any = qualquer tipo (o JSON das APIs tem formato variável)
from typing import Any

# load_dotenv: carrega o arquivo de credenciais para as variáveis de ambiente
from dotenv import load_dotenv

# config: configurações centralizadas do projeto (estações, período, caminhos)
from src import config
# HttpClient: nosso cliente com retentativas e controle de taxa
from src.http_client import HttpClient

# logger com o nome deste módulo
logger = logging.getLogger(__name__)

# pasta da camada bronze dentro do Volume
PASTA_BRONZE: str = f"{config.VOLUME}/bronze"

# variáveis climáticas diárias pedidas à Open-Meteo
VARIAVEIS_CLIMA: list[str] = [
    "temperature_2m_mean",  # temperatura média do dia (°C)
    "precipitation_sum",  # chuva acumulada no dia (mm)
    "relative_humidity_2m_mean",  # umidade relativa média (%)
    "wind_speed_10m_max",  # rajada máxima de vento (km/h)
]


# cria os dois clientes HTTP; devolve uma tupla (openaq, openmeteo)
def criar_clientes() -> tuple[HttpClient, HttpClient]:
    # carrega o key.env; override=True garante o valor mais recente
    load_dotenv(config.ARQUIVO_ENV, override=True)
    # lê a chave do OpenAQ da variável de ambiente
    chave = os.getenv("OPENAQ_API_KEY")
    # se a chave não existir, para com uma mensagem clara (falhar cedo evita 401 lá na frente)
    if not chave:
        raise EnvironmentError("OPENAQ_API_KEY não encontrada. Veja o .env.example.")
    # cliente do OpenAQ: chave no cabeçalho e 1,1s entre chamadas (limite gratuito ~60/min)
    openaq = HttpClient(config.OPENAQ_BASE_URL, headers={"X-API-Key": chave}, intervalo_minimo=1.1)
    # cliente da Open-Meteo: sem chave, intervalo menor porque o limite é mais folgado
    openmeteo = HttpClient(config.OPENMETEO_BASE_URL, intervalo_minimo=0.5)
    # devolve os dois clientes
    return openaq, openmeteo


# salva qualquer estrutura Python como arquivo JSON no caminho indicado
def salvar_json(dados: Any, caminho: str) -> None:
    # cria as pastas do caminho, se ainda não existirem
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    # abre o arquivo para escrita com codificação UTF-8 (preserva acentos)
    with open(caminho, "w", encoding="utf-8") as arquivo:
        # grava o JSON; ensure_ascii=False mantém "São Paulo" legível, sem códigos \u00e3
        json.dump(dados, arquivo, ensure_ascii=False)
    # registra no log o arquivo salvo
    logger.info("Salvo: %s", caminho)


# busca os metadados de uma estação (coordenadas e lista aninhada de sensores)
def buscar_estacao(openaq: HttpClient, id_estacao: int) -> dict[str, Any]:
    # a API devolve uma lista "results" com um único item: a estação
    return openaq.get(f"locations/{id_estacao}")["results"][0]


# busca as médias diárias de um sensor no período configurado, percorrendo todas as páginas
def buscar_medicoes_diarias(openaq: HttpClient, id_sensor: int) -> list[dict[str, Any]]:
    # lista que acumula as medições de todas as páginas
    todas: list[dict[str, Any]] = []
    # página inicial
    pagina = 1
    # máximo de registros por página que a API aceita
    limite = 1000
    # repete até a última página
    while True:
        # pede uma página de agregados diários do sensor
        resposta = openaq.get(
            f"sensors/{id_sensor}/days",
            params={
                "datetime_from": config.DATA_INICIO,  # início do período
                "datetime_to": f"{config.DATA_FIM}T23:59:59Z",  # fim do período, incluindo o último dia inteiro
                "limit": limite,  # registros por página
                "page": pagina,  # número da página atual
            },
        )
        # pega a lista de medições desta página
        resultados = resposta["results"]
        # junta com as páginas anteriores
        todas.extend(resultados)
        # se veio menos que o limite, esta foi a última página: sai do loop
        if len(resultados) < limite:
            break
        # senão, avança para a próxima página
        pagina += 1
    # devolve todas as medições acumuladas
    return todas


# busca o clima diário histórico de uma coordenada no período configurado
def buscar_clima(openmeteo: HttpClient, latitude: float, longitude: float) -> dict[str, Any]:
    # chama o endpoint "archive" (dados históricos) da Open-Meteo
    return openmeteo.get(
        "archive",
        params={
            "latitude": latitude,  # latitude da estação
            "longitude": longitude,  # longitude da estação
            "start_date": config.DATA_INICIO,  # início do período
            "end_date": config.DATA_FIM,  # fim do período
            "daily": ",".join(VARIAVEIS_CLIMA),  # variáveis separadas por vírgula, como a API exige
            "timezone": "auto",  # usa o fuso local da coordenada, para o "dia" bater com o do OpenAQ
        },
    )


# função principal da extração: percorre as estações e grava tudo na bronze
def extrair() -> dict[str, Any]:
    # cria os clientes HTTP das duas APIs
    openaq, openmeteo = criar_clientes()
    # carimbo de data/hora da execução em UTC (rastreabilidade: quando o dado foi baixado)
    extraido_em = datetime.now(timezone.utc).isoformat()
    # resumo da execução, que alimenta o relatório de qualidade
    resumo: dict[str, Any] = {"extraido_em": extraido_em, "estacoes_ok": 0, "sensores_ok": 0, "falhas": []}

    # percorre cada estação configurada (id -> nome da capital)
    for id_estacao, cidade in config.ESTACOES.items():
        # try: se uma estação falhar, registra o erro e segue para a próxima
        try:
            # loga o início do processamento da estação
            logger.info("Extraindo %s (estação %d)", cidade, id_estacao)

            # busca os metadados da estação
            estacao = buscar_estacao(openaq, id_estacao)
            # acrescenta o nome da capital e o carimbo de extração ao JSON bruto
            estacao["cidade"] = cidade
            estacao["_extraido_em"] = extraido_em
            # salva os metadados da estação na bronze
            salvar_json(estacao, f"{PASTA_BRONZE}/openaq/estacoes/{id_estacao}.json")

            # percorre cada sensor da lista aninhada "sensors"
            for sensor in estacao["sensors"]:
                # busca as medições diárias desse sensor
                medicoes = buscar_medicoes_diarias(openaq, sensor["id"])
                # monta o JSON do sensor: de qual estação é, dados do sensor e medições
                saida = {"id_estacao": id_estacao, "sensor": sensor, "results": medicoes, "_extraido_em": extraido_em}
                # salva na bronze, um arquivo por sensor
                salvar_json(saida, f"{PASTA_BRONZE}/openaq/medicoes/{sensor['id']}.json")
                # conta mais um sensor extraído com sucesso
                resumo["sensores_ok"] += 1

            # pega as coordenadas da estação, para o clima ser do mesmo ponto
            coordenadas = estacao["coordinates"]
            # busca o clima diário dessa coordenada
            clima = buscar_clima(openmeteo, coordenadas["latitude"], coordenadas["longitude"])
            # identifica a qual estação esse clima pertence (chave do JOIN depois)
            clima["id_estacao"] = id_estacao
            clima["_extraido_em"] = extraido_em
            # salva o clima na bronze, um arquivo por estação
            salvar_json(clima, f"{PASTA_BRONZE}/openmeteo/{id_estacao}.json")

            # conta mais uma estação concluída
            resumo["estacoes_ok"] += 1

        # captura qualquer erro desta estação
        except Exception as erro:
            # registra o erro no log, sem interromper as demais
            logger.error("Falha em %s (estação %d): %s", cidade, id_estacao, erro)
            # guarda a falha no resumo, para o relatório de qualidade
            resumo["falhas"].append({"id_estacao": id_estacao, "cidade": cidade, "erro": str(erro)})

    # devolve o resumo da execução
    return resumo