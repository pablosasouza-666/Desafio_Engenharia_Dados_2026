# permite usar anotações de tipo modernas (ex.: dict | None) em qualquer Python 3
from __future__ import annotations

# módulo padrão para registrar mensagens de execução (info, avisos, erros)
import logging
# módulo padrão de tempo: usado nas pausas entre tentativas e no controle de taxa
import time
# Any = "qualquer tipo"; usado para descrever o JSON, que pode ter qualquer formato
from typing import Any

# biblioteca externa para fazer requisições HTTP
import requests

# cria um logger com o nome deste módulo, para identificar de onde vem cada mensagem
logger = logging.getLogger(__name__)

# códigos HTTP que indicam falha temporária: vale a pena tentar de novo
# 429 = muitas requisições; 500/502/503/504 = erro ou indisponibilidade do servidor
STATUS_RETENTAVEIS = {429, 500, 502, 503, 504}


# define a classe do cliente HTTP resiliente
class HttpClient:
    """Cliente HTTP com controle de taxa, retentativas e backoff exponencial."""

    # método construtor: roda quando criamos o cliente, ex.: HttpClient("https://...")
    def __init__(
        self,
        base_url: str,  # endereço base da API, ex.: "https://api.openaq.org/v3"
        headers: dict[str, str] | None = None,  # cabeçalhos fixos (ex.: chave de API); opcional
        max_tentativas: int = 5,  # quantas vezes tentar antes de desistir
        intervalo_minimo: float = 1.0,  # segundos mínimos entre uma chamada e outra (rate limit)
        timeout: int = 30,  # segundos máximos esperando cada resposta
    ) -> None:  # -> None indica que o construtor não devolve valor
        # guarda a URL base sem barra no final, para montar endpoints sem "//"
        self.base_url = base_url.rstrip("/")
        # guarda os cabeçalhos; se não vier nenhum, usa dicionário vazio
        self.headers = headers or {}
        # guarda o limite de tentativas
        self.max_tentativas = max_tentativas
        # guarda o intervalo mínimo entre chamadas
        self.intervalo_minimo = intervalo_minimo
        # guarda o tempo limite de cada requisição
        self.timeout = timeout
        # horário da última chamada; começa em 0 para a primeira não esperar
        self._ultima_chamada = 0.0
        # sessão reaproveita a mesma conexão entre chamadas, o que é mais rápido
        self.sessao = requests.Session()

    # garante o intervalo mínimo entre chamadas (controle de taxa / rate limiting)
    def _respeitar_taxa(self) -> None:
        # quanto tempo passou desde a última chamada (monotonic = relógio que não volta atrás)
        decorrido = time.monotonic() - self._ultima_chamada
        # quanto ainda falta esperar para completar o intervalo mínimo
        espera = self.intervalo_minimo - decorrido
        # se ainda falta tempo, pausa o programa por esse período
        if espera > 0:
            time.sleep(espera)
        # registra o horário desta chamada para a próxima comparação
        self._ultima_chamada = time.monotonic()

    # calcula e aplica a pausa antes de uma nova tentativa
    def _aguardar(self, tentativa: int, retry_after: str | None = None) -> None:
        # se a API informou quanto esperar (cabeçalho Retry-After em segundos), obedece
        if retry_after and retry_after.isdigit():
            espera = float(retry_after)
        else:
            # senão, usa backoff exponencial: 2s, 4s, 8s, 16s...
            espera = 2.0 ** tentativa
        # registra no log quanto tempo vai esperar
        logger.info("Aguardando %.0fs antes da tentativa %d", espera, tentativa + 1)
        # pausa o programa
        time.sleep(espera)

    # faz uma requisição GET e devolve o JSON da resposta como dicionário
    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        # monta a URL completa: base + "/" + endpoint (sem barra duplicada)
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # repete de 1 até max_tentativas (range não inclui o último número, por isso o +1)
        for tentativa in range(1, self.max_tentativas + 1):
            # antes de cada chamada, respeita o intervalo mínimo
            self._respeitar_taxa()

            try:
                # faz a chamada HTTP com parâmetros, cabeçalhos e tempo limite
                resposta = self.sessao.get(
                    url, params=params, headers=self.headers, timeout=self.timeout
                )
            # captura falhas de rede (conexão caiu, DNS) e tempo esgotado
            except (requests.ConnectionError, requests.Timeout) as erro:
                # loga o aviso; só a URL, nunca os cabeçalhos, para não vazar a chave
                logger.warning("Erro de rede em %s (tentativa %d): %s", url, tentativa, erro)
                # espera antes de tentar de novo
                self._aguardar(tentativa)
                # volta para o início do loop (próxima tentativa)
                continue

            # se o status indica falha temporária, tenta de novo
            if resposta.status_code in STATUS_RETENTAVEIS:
                # loga o status recebido
                logger.warning("Status %d em %s (tentativa %d)", resposta.status_code, url, tentativa)
                # espera, usando o Retry-After da API se ele vier
                self._aguardar(tentativa, resposta.headers.get("Retry-After"))
                # próxima tentativa
                continue

            # erros definitivos (ex.: 401 chave inválida, 404 não encontrado): não adianta repetir,
            # então lança exceção imediatamente
            resposta.raise_for_status()
            # sucesso: converte o corpo da resposta de JSON para dicionário Python e devolve
            return resposta.json()

        # se saiu do loop sem retornar, todas as tentativas falharam
        raise RuntimeError(f"Falha após {self.max_tentativas} tentativas: {url}")