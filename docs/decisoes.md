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
| 16 | OpenAQ: filtro de período com `date_from`/`date_to` | O endpoint de médias diárias ignorava `datetime_from`/`datetime_to` e devolvia todo o histórico; detectado validando as datas recebidas, não só o status 200 |
| 17 | PySpark em geral; Spark SQL no EXPLODE e no JOIN | Atende literalmente o requisito de desaninhamento "via SQL" e mostra domínio das duas formas |
| 18 | Data local da cidade, extraída do texto (`AAAA-MM-DD`) | Converter para timestamp levaria para UTC e poderia deslocar a medição para o dia anterior |
| 19 | Schema explícito (`MapType`) para o JSON da NASA | Sem ele, o Spark criaria uma coluna por data (335 colunas); como mapa, o EXPLODE funciona |
| 20 | Deduplicação por `ROW_NUMBER`, mantendo a maior cobertura | Em caso de repetição, fica a medição mais completa do dia; desempate fixo garante resultado determinístico |
| 21 | Gold com clima como base e `LEFT JOIN` da poluição | Dias sem medição ficam visíveis como nulos, permitindo medir a cobertura real |
| 22 | Categorias de qualidade do ar pelas faixas da OMS (2021) | Referência oficial em vez de faixas arbitrárias |
| 23 | Dia chuvoso = chuva ≥ 1 mm | Convenção meteorológica; abaixo disso é garoa |
| 24 | Data quality com três status (aprovado, alerta, reprovado) | Separa defeito do pipeline (reprova e bloqueia a gravação) de limitação da fonte (alerta e grava) |
| 25 | Cobertura mínima de PM2.5 de 50% por cidade | Critério simples: dados em pelo menos metade dos dias para a média ser representativa |
| 26 | Vitória mantida no projeto, com alerta | Expor a limitação é mais transparente do que remover o dado |
| 27 | Um arquivo Parquet por partição (`repartition` antes de gravar) | Evita muitos arquivos pequenos, que são lentos de ler e poluem o repositório |
| 28 | Notebook `main` cria schema e Volume (`IF NOT EXISTS`) | Qualquer pessoa consegue rodar do zero sem setup manual; idempotente |
| 29 | PySpark fora do `requirements.txt` | A versão do Databricks é uma build própria; instalar outra por cima quebraria o ambiente |