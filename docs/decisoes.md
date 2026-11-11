# Registro de decisões

| # | Decisão | Motivo |
|---|---------|--------|
| 1 | Plataforma: Databricks Free Edition | Ambiente gerenciado com Spark e integração nativa com Git |
| 2 | Motor: PySpark + Spark SQL | Nativo do Databricks; EXPLODE atende o desaninhamento exigido |
| 3 | APIs: OpenAQ (qualidade do ar) e NASA POWER (clima) | Domínios distintos, JSON aninhado, cruzáveis por estação + data |
| 4 | Arquitetura medalhão (bronze, silver, gold) | Separa dado bruto, tratado e final; facilita auditoria |
| 5 | Credenciais em `key.env` fora da Git folder, lidas com `os.getenv` | Atende o requisito de segurança; nenhuma chave no repositório |
| 6 | `.env.example` versionado sem valores | Documenta as variáveis necessárias para quem for reproduzir |
| 7 | Código modular em `src/` (um módulo por etapa) | Requisito do desafio; cada etapa fica isolada e testável |
| 8 | Saída final em Parquet + Snappy, particionado por ano/mês | Formato citado no entregável; Snappy é o padrão do Spark |
| 9 | Recorte: 6 capitais com PM2.5 ativo no OpenAQ (RJ, SP, Vitória, Salvador, Manaus, Porto Velho) | São as únicas capitais com monitoramento ativo; as redes oficiais pararam de enviar dados em 2023 |
| 10 | Período: dez/2025 a out/2026 | Janela comum em que as 6 estações têm dados; gera 11 partições mensais |
| 11 | Rio: estação Centro Kunak em vez de Tijuca | A Tijuca não mede PM2.5 |
| 12 | Open-Meteo substituída pela NASA POWER | A cota gratuita da Open-Meteo é por IP, e o Databricks Free compartilha IPs entre usuários (erro 429 com espera de 7h) |
| 13 | Teto de 60s na espera entre retentativas | Obedecer um Retry-After de horas travaria o pipeline; acima do teto, falha rápido e registra |
| 14 | Coordenadas do clima vêm da própria estação do OpenAQ | Garante que as duas fontes olham para o mesmo ponto geográfico |
| 15 | Bronze guarda o JSON bruto, um arquivo por estação/sensor | Dado fiel à fonte; falhas isoladas e reprocessamento barato |