# Instruções do projeto para agentes de IA

Este bundle (Databricks Asset Bundle) é mantido com apoio do Claude Code. Este arquivo guarda as
convenções e os números de referência que qualquer agente (ou pessoa) precisa seguir ao mudar o projeto.
O `CLAUDE.md` importa este arquivo.

Antes de qualquer ação no Databricks, carregue a skill `databricks-core` do plugin Databricks
(autenticação, perfil da CLI e fluxo de deploy do bundle). Sem o plugin: `databricks aitools install`.

---

## Projeto

Pipeline de e-commerce brasileiro (Lakeflow Declarative Pipeline) com as camadas bronze → silver → gold.

### Ambiente
- Perfil da Databricks CLI: sempre `--profile ecommerce_profile`.
- Um catálogo por ambiente, na variável `catalog` do bundle: `ecommerce` no target `dev` e
  `ecommerce_prod` no target `prod`. Nunca use outro catálogo. Schemas: `bronze`, `silver` e `gold`.
- A bronze (`bronze.vendas`, `bronze.produtos`, `bronze.clientes`, `bronze.preco_competidores` e
  `bronze.estados_ibge`) é escrita SÓ pela ingestão (`src/ingestao/ingestao_bronze.py`), com carga full
  (`overwrite`). O pipeline nunca escreve na bronze.
- Credenciais do Storage do Supabase ficam no secret scope `ecommerce` (`s3_endpoint`, `s3_access_key`,
  `s3_secret_key`), lidas com `dbutils.secrets.get`. Nunca escreva endpoint, chave ou segredo no código.
- `CREATE CATALOG` roda por SQL (a ingestão cria o catálogo e os schemas se não existirem); na Free
  Edition ele não funciona pela API REST.

### Convenções
- Nomes de tabelas e colunas em português, snake_case, sem acento.
- Silver em Python (`from pyspark import pipelines as dp`), gold em SQL.
- Um arquivo por tabela: `src/ecommerce_etl/transformations/silver/<tabela>.py` e
  `src/ecommerce_etl/transformations/gold/<tabela>.sql`.
- Todas as tabelas são materialized views com leitura batch (`spark.read.table`). Nunca use
  streaming table, porque a bronze é sobrescrita a cada execução.
- Pipeline serverless, catálogo `${var.catalog}`, schema padrão `silver`. As golds são publicadas como
  `gold.<tabela>`. No código, os nomes são sempre `schema.tabela`, sem o catálogo.
- Cada arquivo começa com comentários, em português, explicando o PORQUÊ das regras.
- Dinheiro sempre em `DECIMAL(10,2)`.
- Um problema de qualidade conhecido é MARCADO em uma coluna e medido com `@dp.expect` (warn).
  Nunca descarte linhas: apagar vendas mudaria a receita.
- `@dp.expect_all_or_fail` só para o que nunca pode acontecer.
- Sempre rode `databricks bundle validate --strict --profile ecommerce_profile` antes do deploy.

### Regras para toda gold (valem para todas as diretorias)
- SQL, um arquivo por tabela em `src/ecommerce_etl/transformations/gold/`, com
  `CREATE OR REFRESH MATERIALIZED VIEW gold.<tabela>`.
- Declare TODAS as colunas entre parênteses, com tipo e COMMENT (sem o tipo, o comentário é
  ignorado). Declare também o COMMENT da tabela, dizendo quando usar a tabela. Os comentários são em
  português, com unidade (R$), regra de cálculo e avisos que evitem erro do Genie. Faça CAST na
  consulta para o tipo declarado (dinheiro em DECIMAL(10,2)).
- Inclua TODAS as vendas, inclusive as de produto não cadastrado: dinheiro que entrou é receita.
- Período dos dados: 13/12/2025 a 11/01/2026. O dataset é uma foto fixa: o período está escrito nos
  comentários da gold, nas instruções do Genie e nos subtítulos dos dashboards, e o teste de período
  (constantes `PERIODO_INICIO`/`PERIODO_FIM` no notebook de testes) falha se o dado mudar. Mudou o
  período? Atualize todos esses textos junto com as constantes.
- Toda gold nova ganha testes no notebook `testes_qualidade.py`.

### Testes e Job
- Notebook `src/ecommerce_etl/testes/testes_qualidade.py` (widget `catalogo`, preenchido pelo parâmetro
  do Job).
  Cada teste é uma consulta que conta as linhas com problema. Se alguma contagem for maior que zero,
  o notebook falha com `AssertionError`.
- Job "Pipeline E-commerce" (`resources/pipeline_ecommerce.job.yml`): `ingestao_bronze` →
  `atualizar_pipeline` → `testes_qualidade`. Agendado todo dia às 6h (`America/Sao_Paulo`), com e-mail
  na falha. Não declare `pause_status`: o modo development pausa o agendamento em dev sozinho, e um
  `UNPAUSED` explícito faria o Job de dev rodar todo dia.
  `databricks bundle run pipeline_ecommerce -t dev --profile ecommerce_profile`.

### Tabelas do pipeline
| Camada | Tabela | Grão | Diretoria |
|---|---|---|---|
| silver | `silver.produtos` | id_produto (com `marca_divergente_do_nome`) | - |
| silver | `silver.clientes` | id_cliente (região de `bronze.estados_ibge`) | - |
| silver | `silver.preco_competidores` | id_produto + nome_concorrente | - |
| silver | `silver.vendas` | id_venda | - |
| gold | `gold.clientes_segmentacao` | id_cliente (inclusive quem nunca comprou) | Customer Success |
| gold | `gold.vendas_temporais` | data × hora × canal_venda | Comercial |
| gold | `gold.vendas_produtos` | id_produto vendido | Comercial |
| gold | `gold.vendas_detalhadas` | id_venda | Comercial (cruza diretorias) |
| gold | `gold.precos_competitividade` | id_produto com preço de concorrente | Pricing |
| gold | `gold.qualidade_dados` | regra de qualidade | Todas (placar de qualidade) |

A gold Comercial (`vendas_detalhadas`) depende do segmento de `gold.clientes_segmentacao`.

### Números de referência (bronze de 13/12/2025 a 11/01/2026)
Se algum número mudar sem que a bronze tenha mudado, investigue antes de corrigir.
- Bronze: 3.020 vendas, 215 produtos, 50 clientes, 728 preços de concorrente (4 concorrentes), 27 UFs
  em `estados_ibge`. Sem duplicatas.
- Receita total: **R$ 974.077,28 (3.020 vendas)** em `silver.vendas`, `gold.clientes_segmentacao`,
  `gold.vendas_temporais`, `gold.vendas_produtos` e `gold.vendas_detalhadas`. 2.155 vendas no ecommerce.
- Expectations (warn) na silver: 20 vendas de produto não cadastrado (R$ 4.240,01), 5 vendas antes do
  cadastro (R$ 325,88), 55 preços de concorrente suspeitos, 12 produtos com marca divergente do nome.
- `gold.qualidade_dados` (7 regras): produto não cadastrado 20 (R$ 4.240,01), venda antes do cadastro 5
  (R$ 325,88), preço suspeito 55, marca divergente 12 (ALERTA); nome repetido 137, menos de 4
  concorrentes 109 (INFORMATIVO); pronome de tratamento 11 (CORRIGIDO).
- 11 clientes com pronome de tratamento. Clientes por região: Norte 17, Nordeste 12, Centro-Oeste 9,
  Sudeste 8, Sul 4.
- Segmentos: 10 VIP, 25 TOP_TIER, 15 REGULAR. Maior cliente: Ana Sophia Pereira (MG, R$ 30.716,63).
- Pricing: 215 produtos; 35 MAIS_CARO_QUE_TODOS, 92 ACIMA_DA_MEDIA, 76 ABAIXO_DA_MEDIA,
  6 MAIS_BARATO_QUE_TODOS, 6 NA_MEDIA; 15 com preço suspeito; 30 nunca venderam. A receita em
  `gold.precos_competitividade` é R$ 969.837,27 (total menos os R$ 4.240,01 de produto não cadastrado,
  que não têm preço de concorrente).
- Nenhuma coluna gold sem comentário. Notebook de testes: 22 testes, todos com 0 problemas.

### Dashboards AI/BI (um por diretoria)
- Um arquivo por dashboard: `src/dashboards/<nome>.lvdash.json`, com o recurso em
  `resources/<nome>.dashboard.yml`: `warehouse_id: ${var.warehouse_id}` (variável do bundle com lookup
  pelo nome "Serverless Starter Warehouse"), `dataset_catalog: ${var.catalog}` e `dataset_schema: gold`.
- Dashboards: `diretoria_comercial`, `diretoria_customer_success` e `diretoria_pricing`.
- Consultas com o nome da tabela sem catálogo nem schema (`FROM vendas_temporais`), para o mesmo
  dashboard funcionar em dev e prod.
- Teste no warehouse TODA consulta que vira dataset antes do deploy, e confira os KPIs contra SQL direto
  na gold (números de referência acima).
- Tudo em português: título, subtítulo com período e fonte dos dados, nomes de gráficos e eixos.
  Canais exibidos como "E-commerce" e "Loja física". Dinheiro em R$ (`currencyCode: BRL`).
- Layout de leitura rápida: título (com filtros ao lado), uma linha de KPIs, gráficos e uma tabela de
  detalhe para agir.
- Regras que nenhum gráfico pode quebrar:
  - Ticket médio = receita total ÷ número de vendas, nunca média de médias: `SUM(receita) /
    SUM(total_vendas)` em vendas_temporais, `COUNT(*)` em vendas_detalhadas, `SUM(total_compras)` em
    clientes_segmentacao.
  - Nunca somar `clientes_unicos` entre linhas. Produto se conta por `id_produto` (há nomes repetidos).
  - Dia da semana se compara pela receita MÉDIA por dia (`SUM(receita) / COUNT(DISTINCT data)`): o
    período tem 5 sábados e 5 domingos e só 4 de cada dia útil.
  - Data e hora estão em UTC: diga isso no eixo e no filtro de período.
  - `diferenca_pct_*` está em pontos percentuais (10 = 10%): divida por 100 para usar o formato de %.
  - Pricing separa sempre o confirmado do preço suspeito (`possui_preco_suspeito`): o diretor não
    reage a preço suspeito antes de conferir a coleta.
- Filtros de dataset agregado (ex.: top 10, que agrega antes do LIMIT) usam parâmetros (`:periodo`
  RANGE e `:canal` MULTI com `size(:canal) = 0 OR array_contains(...)`) ligados ao MESMO widget de filtro
  que filtra o campo do dataset principal.
- Mudou um dashboard? Edite o JSON e faça deploy. Ajuste feito na interface se perde no próximo deploy.
- O botão "Ask Genie" dos 3 dashboards aponta para o space "Diretoria E-commerce" com o id FIXO no JSON
  (`uiSettings.genieSpace.overrideId`, hoje o id do space de dev). O JSON do dashboard não passa por
  variáveis do bundle: em prod o space tem outro id (veja com `databricks bundle summary -t prod`),
  então troque o `overrideId` antes de publicar em prod.

### Genie space "Diretoria E-commerce" (um agente para as três diretorias)
- O serialized space fica DENTRO de `resources/diretoria.genie_space.yml` (campo `serialized_space`, um
  JSON num bloco `|`), e não num arquivo separado: assim os identificadores das tabelas usam
  `${var.catalog}.gold.<tabela>` (também nos SQLs de exemplo e nos joins) e o mesmo space vale em dev e
  prod. Recurso com `warehouse_id: ${var.warehouse_id}` (lookup pelo nome) e
  `parent_path: ${workspace.root_path}`, para não colidir com outro space de mesmo nome na sua pasta.
- Mudou uma instrução? Edite o YAML e faça deploy. Nada de ajustar o space pela interface: o ajuste se
  perde no próximo deploy.
- Só as 6 golds entram no space (nenhuma bronze ou silver). O Genie só sabe o que está nas tabelas, nos
  comentários das colunas e no space: não repita nas instruções o que o comentário já diz.
- Instruções gerais curtas (até ~2.500 caracteres), só com regra de negócio que não cabe em comentário.
  Não coloque exemplo de valor nas instruções (o Genie já citou "R$ 1.234,56" como se fosse dado real).
- Regra que falha em texto costuma passar com um SQL de exemplo no formato certo. Os SQLs de exemplo
  NÃO podem repetir as perguntas do teste de aceitação (senão o teste vira cola). Teste cada SQL no
  warehouse antes de colocar no JSON.
- Todo ajuste passa pelo teste: 10 perguntas pela API de conversa (`databricks genie start-conversation`)
  comparadas com SQL direto na gold, mais "Qual foi o nosso lucro?" e "Quanto vendemos ontem?", que
  devem ser recusadas sem inventar número. Só conta acerto se o TEXTO da resposta trouxer todos os
  números esperados. As 6 perguntas da tela inicial não podem voltar vazias.
