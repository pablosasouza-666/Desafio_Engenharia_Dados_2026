# Registro de decisões

| # | Decisão | Motivo |
|---|---------|--------|
| 1 | Plataforma: Databricks Free Edition | Ambiente gerenciado com Spark e integração nativa com Git |
| 2 | Motor: PySpark + Spark SQL | Nativo do Databricks; EXPLODE atende o desaninhamento exigido |
| 3 | APIs: Open-Meteo (clima) e OpenAQ (qualidade do ar) | Domínios distintos, JSON aninhado, cruzáveis por cidade + data |
| 4 | Arquitetura medalhão (bronze, silver, gold) | Separa dado bruto, tratado e final; facilita auditoria |
| 5 | Credenciais em `.env` fora da Git folder, lidas com `os.getenv` | Atende o requisito de segurança; nenhuma chave no repositório |
| 6 | `.env.example` versionado sem valores | Documenta as variáveis necessárias para quem for reproduzir |
| 7 | Código modular em `src/` (um módulo por etapa) | Requisito do desafio; cada etapa fica isolada e testável |
| 8 | Saída final em Parquet + Snappy, particionado por ano/mês | Formato citado no entregável; Snappy é o padrão do Spark |
