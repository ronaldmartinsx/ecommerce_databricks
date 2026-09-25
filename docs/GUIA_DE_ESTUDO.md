# Guia de estudo: pipeline de e-commerce no Databricks

Este guia explica a solução de ponta a ponta, da ingestão ao Genie, para quem vem do Power BI e do Microsoft Fabric e está começando no Databricks.

**Como ler as marcações**

| Marca | Significado |
|---|---|
| **[V]** | Verificado: conferido no código deste repositório ou no workspace (via Databricks CLI, só leitura) |
| **[I]** | Inferido: conclusão a partir de indícios, não confirmada diretamente |
| **≈ Fabric** | Ponte com o Microsoft Fabric / Power BI. Quando a equivalência não é exata, o texto avisa |

Nos caminhos do workspace, `<seu_usuario>` é o e-mail do usuário dono do bundle e `[dev <seu_usuario>]` é o prefixo que o modo *development* coloca nos recursos.

---

## Sumário

1. [Problema de negócio](#1-problema-de-negócio)
2. [Visão geral](#2-visão-geral)
3. [Camada a camada](#3-camada-a-camada)
   - [3.1 Ingestão](#31-ingestão)
   - [3.2 Bronze](#32-bronze)
   - [3.3 Silver](#33-silver)
   - [3.4 Gold](#34-gold)
   - [3.5 Consumo: dashboards AI/BI e Genie](#35-consumo-dashboards-aibi-e-genie)
4. [Plataforma](#4-plataforma)
5. [Papel da IA na construção](#5-papel-da-ia-na-construção)
6. [Mapa: onde encontro cada coisa](#6-mapa-onde-encontro-cada-coisa)
7. [Gaps para produção](#7-gaps-para-produção)
8. [Glossário](#8-glossário)

---

## 1. Problema de negócio

### A dor

Um e-commerce brasileiro acabou de abrir a operação digital e vende em dois canais: loja online (`ecommerce`) e loja física (`loja_fisica`). Três diretores querem respostas, e cada um tem a sua pergunta:

| Diretoria | Pergunta | Tabela gold que responde |
|---|---|---|
| **Comercial** | Quanto vendemos, quando, em qual canal e com quais produtos? | `vendas_temporais`, `vendas_produtos`, `vendas_detalhadas` |
| **Customer Success** | Quem são os melhores clientes e onde eles estão? | `clientes_segmentacao` |
| **Pricing** | Estamos mais caros que a concorrência (Mercado Livre, Amazon, Magalu e Shopee)? Em quais produtos agir? | `precos_competitividade` |

Os dados são sintéticos (gerados com Faker) e têm **defeitos colocados de propósito** [V, README do repositório oficial]: vendas de produtos que não existem no catálogo, produtos com o mesmo nome, nomes de clientes com "Sr." ou "Dra.", preços de concorrente pela metade do nosso.

### Como era antes

A resposta dependia de alguém:

- baixar os arquivos, subir na mão (CSV) e escrever SQL solto;
- lembrar das armadilhas (por exemplo, um `INNER JOIN` com produtos descarta em silêncio as 20 vendas sem cadastro, e a receita cai de R$ 974.077,28 para R$ 969.837,27 [V]);
- refazer tudo quando chegasse dado novo, sem saber se o número de hoje bate com o de ontem.

**≈ Fabric:** é o cenário do "relatório em Excel alimentado à mão" que vira um `.pbix` com Power Query apontando para arquivos locais: funciona uma vez, não escala e ninguém consegue auditar.

### O que muda

| Antes | Depois |
|---|---|
| Arquivo arrastado à mão | Ingestão por código a partir do data lake (Storage do Supabase, protocolo S3) |
| SQL solto, sem regra escrita | Pipeline declarativo: cada tabela é um arquivo com as regras e o porquê em comentário |
| Problema de qualidade descoberto por acaso | Problema **marcado em coluna e medido** a cada execução (expectations) |
| "Confia em mim" | 22 testes automáticos que deixam o Job vermelho se um número não bater [V] |
| Um relatório para todo mundo | Um dashboard por diretoria, com os 4 KPIs que cada diretor acompanha |
| O diretor pede, o analista responde | O diretor pergunta em português ao **Genie**, que responde em cima da gold |

---

## 2. Visão geral

Os arquivos Parquet saem do data lake (Storage do Supabase) e de uma API pública (IBGE) e são gravados sem alteração na camada **bronze** do Unity Catalog. Um **pipeline declarativo** (Lakeflow) lê a bronze, limpa e marca os problemas de qualidade na **silver** (Python) e monta na **gold** (SQL) uma tabela para cada pergunta de negócio, com todas as colunas comentadas. Um **Job** diário roda a ingestão, atualiza o pipeline e em seguida roda um notebook de testes. Em cima da gold ficam **três dashboards AI/BI**, um por diretoria, e um **Genie space** que transforma perguntas em português em SQL. Tudo o que é recurso (pipeline, job, dashboards e Genie) está descrito como código num **Databricks Asset Bundle** e é implantado com `databricks bundle deploy`.

### Analogia: a cozinha de um restaurante

| Etapa | Na cozinha | No projeto |
|---|---|---|
| Ingestão | O fornecedor entrega na doca | Notebook baixa os Parquet e chama a API do IBGE |
| Bronze | A câmara fria guarda os ingredientes como chegaram, com a nota fiscal | Tabelas Delta idênticas à origem |
| Silver | O pré-preparo: lavar, cortar, etiquetar ("este tomate está machucado", sem jogar fora) | Tipos corrigidos, receita calculada, problemas marcados |
| Gold | O prato montado para cada pedido | Uma tabela por pergunta de diretoria |
| Testes | O chef prova antes de sair da cozinha | Notebook de testes: se falhar, o Job fica vermelho |
| Dashboard | O cardápio fixo | As perguntas que já sabemos que os diretores fazem |
| Genie | O garçom que anota pedidos fora do cardápio, mas só sabe o que está no caderno de receitas | Responde em português com base em tabelas, comentários e instruções |

```
Supabase (S3) ─┐
               ├─► bronze ──► silver ──► gold ──┬─► 3 dashboards AI/BI
API do IBGE ───┘   (Delta)    (Python)   (SQL)  └─► Genie space
                          └── Lakeflow pipeline ──┘
                   Job: ingestão → pipeline → testes de qualidade
```

---

## 3. Camada a camada

### 3.1 Ingestão

**O que faz.** Conecta no Storage do Supabase pelo protocolo S3 (biblioteca `boto3`), baixa `vendas`, `produtos`, `clientes` e `preco_competidores` em Parquet, converte em DataFrame e grava como tabela Delta na bronze. Em seguida chama a API do IBGE (lista de estados em JSON) e grava `bronze.estados_ibge` [V].

**Objetos**

| Objeto | Onde no repo | Onde no workspace |
|---|---|---|
| Notebook de ingestão | `ecommerce/src/ingestao/ingestao_bronze.py` | `.bundle/ecommerce/dev/files/src/ingestao/ingestao_bronze`, 1ª tarefa do Job [V] |
| Credenciais | — (nunca no código) | Secret scope `ecommerce`: `s3_endpoint`, `s3_access_key`, `s3_secret_key` [V] |
| Bucket de origem | Widget `bucket` (padrão `datalake_ecommerce`) | Supabase Storage [V] |

**Como foi modelada.** Carga **full com sobrescrita** (`mode("overwrite")` + `overwriteSchema`) a cada execução [V]. O caminho do dado é `S3 → bytes → pandas → Spark → Delta`. O catálogo vem do parâmetro `catalogo` do Job, então dev grava em `ecommerce` e prod em `ecommerce_prod`. No fim, o notebook falha se alguma tabela chegar vazia.

**Decisões e porquês**

| Decisão | Por quê |
|---|---|
| `boto3` com `endpoint_url` do Supabase | O Supabase fala o protocolo S3; o mesmo código funcionaria na AWS |
| pandas no meio do caminho | Os arquivos são pequenos (3.020 vendas); pandas lê Parquet de bytes em uma linha |
| Sobrescrever tudo | Idempotente e simples: rodar duas vezes dá o mesmo resultado |
| `overwriteSchema` em vez de `DROP TABLE` | A primeira versão apagava a tabela antes de gravar. Sobrescrever mantém o histórico Delta (`DESCRIBE HISTORY`) para auditoria |
| Credenciais no secret scope | O notebook vai para o Git; `dbutils.secrets.get` mantém endpoint e chaves fora dele e mostra `[REDACTED]` se alguém imprimir |
| `CREATE CATALOG` por SQL no próprio notebook | Na Free Edition a criação de catálogo não funciona pela API REST; assim o primeiro run de um ambiente novo cria catálogo e schemas |

**Como era a primeira versão (e o que mudou) [V]**

A primeira ingestão foi um notebook solto no workspace (`01_ingestao_bronze`), fora do Git e do Job, com `ACCESS_KEY` e `SECRET_KEY` escritos no código e sem os imports de `pandas` e `io` (rodado do zero como tarefa, daria `NameError`). A versão atual resolve os três pontos: está versionada, lê as credenciais do secret scope e é a primeira tarefa do Job. O notebook antigo deve ser apagado e as chaves antigas, trocadas.

**Alternativas e trade-offs [I]** (não há registro de discussão; é a análise das opções que existiam)

| Alternativa | Ganho | Custo / motivo de não usar agora |
|---|---|---|
| **Auto Loader** (`cloudFiles`) lendo de um volume ou external location | Carga incremental, detecta arquivo novo, evolução de schema | Precisa do arquivo num storage que o Databricks leia direto; para um endpoint S3-compatível de terceiros, o caminho simples é o `boto3` |
| Copiar o arquivo bruto para um **Volume** antes de virar tabela | Reprocessar sem baixar de novo da origem; auditoria do arquivo | Mais um passo. O material da imersão cita o volume `bronze.arquivos`, mas ele não foi criado aqui [V] |
| **Lakeflow Connect** (conectores gerenciados) | Sem código, com CDC | Não há conector para Storage do Supabase |
| Append com colunas de controle (`_ingerido_em`, `_arquivo_origem`) | Histórico e rastreabilidade | Exigiria MV ou lógica de "última versão" na silver. Hoje a bronze não tem colunas de metadado [V] |

**≈ Fabric.** O equivalente mais direto é um **notebook do Fabric** gravando no Lakehouse, ou uma **Copy activity** num Data pipeline. No Fabric existe ainda a opção de um **shortcut do OneLake para storage S3-compatível**, que evitaria a cópia [I: não testado com o Supabase]. O secret scope ≈ **Azure Key Vault** lido com `notebookutils.credentials.getSecret`.

---

### 3.2 Bronze

**O que faz.** Guarda o dado **exatamente como chegou**. A regra da camada é não limpar nada: qualquer correção aqui apagaria a evidência do que a origem mandou.

**Objetos [V]**

| Tabela | Linhas | Tipo |
|---|---:|---|
| `ecommerce.bronze.vendas` | 3.020 | Delta gerenciada |
| `ecommerce.bronze.produtos` | 215 | Delta gerenciada |
| `ecommerce.bronze.clientes` | 50 | Delta gerenciada |
| `ecommerce.bronze.preco_competidores` | 728 | Delta gerenciada |
| `ecommerce.bronze.estados_ibge` | 27 | Delta gerenciada, lida por `silver.clientes` |

A bronze é escrita **só** pela ingestão (`ecommerce/src/ingestao/ingestao_bronze.py`); o pipeline nunca escreve nela [V, `AGENTS.md`].

**Como foi modelada.** Uma tabela por arquivo de origem, com o schema que veio do Parquet. Exemplo `bronze.vendas` [V]: `id_venda STRING, data_venda TIMESTAMP, id_cliente STRING, id_produto STRING, canal_venda STRING, quantidade LONG, preco_unitario DOUBLE`. O preço está em `DOUBLE` (ponto flutuante) e a silver corrige isso.

**Decisões e porquês**

| Decisão | Por quê |
|---|---|
| Tabela Delta, e não arquivo solto | Schema imposto, histórico de versões (`DESCRIBE HISTORY`) e *time travel* |
| Tabelas gerenciadas (*managed*) | O Unity Catalog cuida do storage; apagar a tabela apaga o dado, sem arquivo órfão |
| Sobrescrita a cada carga | Por isso **toda a pipeline usa materialized view, e não streaming table** (ver 3.3) |

**≈ Fabric.** É a pasta **Tables** de um Lakehouse bronze. Aqui a equivalência é quase exata: o formato é o mesmo (Delta Lake). A diferença é o endereço: no Databricks é `catalogo.schema.tabela`; no Fabric é *workspace → lakehouse → (schema) → tabela*.

---

### 3.3 Silver

**O que faz.** Transforma o dado cru em dado confiável: tipos certos, chaves sem duplicata, receita calculada, atributos de tempo prontos e **cada problema de qualidade marcado numa coluna e medido**.

**Objetos [V]**

| Tabela (materialized view) | Arquivo | Chave | Regras principais |
|---|---|---|---|
| `silver.produtos` | `ecommerce/src/ecommerce_etl/transformations/silver/produtos.py` | `id_produto` | `trim` no nome, preço em `DECIMAL(10,2)`, `faixa_preco` (PREMIUM > 1000, MEDIO > 500, BASICO), `marca_divergente_do_nome` |
| `silver.clientes` | `.../silver/clientes.py` | `id_cliente` | Remove "Sr./Sra./Srta./Dr./Dra.", formato título com preposições minúsculas, guarda `nome_original`, UF → nome do estado e região via `bronze.estados_ibge` |
| `silver.preco_competidores` | `.../silver/preco_competidores.py` | `id_produto` + `nome_concorrente` | Preço em `DECIMAL`, `data_coleta` de texto para timestamp, `preco_suspeito` quando o concorrente cobra menos de 60% do nosso preço |
| `silver.vendas` | `.../silver/vendas.py` | `id_venda` | `receita = quantidade × preco_unitario`, `data`, `hora`, `dia_semana` em português, `produto_cadastrado`, `venda_antes_do_cadastro` |

No workspace: `ecommerce.silver.*` (em prod, `ecommerce_prod.silver.*`), com tipo *Materialized view* no Catalog Explorer, e o grafo no pipeline `[dev <seu_usuario>] ecommerce_etl`.

**Como foi modelada.** Um arquivo Python por tabela, com o decorator `@dp.materialized_view` (`from pyspark import pipelines as dp`) e leitura **batch** (`spark.read.table`). O pipeline descobre sozinho a ordem: `silver.produtos` roda antes de `silver.vendas` e de `silver.preco_competidores`, porque as duas leem produtos.

**Expectations: a qualidade declarada junto da tabela [V]**

| Tipo | Uso no projeto | Resultado da última execução |
|---|---|---|
| **Fail** (`@dp.expect_all_or_fail`) | O que nunca pode acontecer: chave vazia, quantidade ou preço ≤ 0, canal desconhecido, cliente sem região | 15 regras, 0 falhas |
| **Warn** (`@dp.expect`, `@dp.expect_all`) | Problema conhecido e tolerado | `produto_cadastrado`: 20 falhas · `venda_depois_do_cadastro`: 5 · `preco_plausivel`: 55 · `marca_consistente_com_nome`: 12 |

**Decisões e porquês**

| Decisão | Por quê |
|---|---|
| **Marcar e não apagar** | As 20 vendas de produto não cadastrado somam R$ 4.240,01. Dinheiro que entrou é receita: apagar mudaria o faturamento sem ninguém saber por quê |
| Materialized view, e não streaming table | A bronze é sobrescrita. Uma streaming table só aceita linhas novas e quebraria; a MV relê a fonte e recalcula |
| Dinheiro em `DECIMAL(10,2)` | `DOUBLE` acumula erro de arredondamento; a soma precisa bater centavo a centavo |
| Silver em Python | Regex de pronomes, mapa de UFs e lista de preposições ficam mais claros em código do que em SQL |
| `dropDuplicates` mesmo sem duplicata hoje | Defesa: a bronze é sobrescrita por outra ingestão e pode passar a trazer repetidos |
| Região a partir de `bronze.estados_ibge` | A ingestão já traz a lista oficial do IBGE. A primeira versão usava um dicionário fixo de 27 UFs (o prompt dizia que "não existe tabela de estados na bronze") e deixava `estados_ibge` sem uso. Trocar não mudou nenhum cliente: 0 divergências [V] |
| Marca divergente por palavra inteira | "Tênis Nike Revolution" com marca Adidas é erro de cadastro. A comparação por palavra (`\b`) evita que "LG" case com "algodão". Resultado: 12 produtos [V] |
| Dia da semana por índice (`dayofweek` + array) | Não depende do idioma configurado no Spark. O implementação de referência da imersão traduz o nome em inglês com `date_format('EEEE')` |
| Limite de 60% para preço suspeito | Separa promoção ou erro de coleta dos preços normais. O implementação de referência da imersão registra que os preços legítimos ficam entre 92% e 110% do nosso [V, comentário do código oficial] |

**Alternativas descartadas**

| Alternativa | Trade-off |
|---|---|
| `expect_or_drop` para vendas sem cadastro | Mais "limpo", mas muda a receita. Descartada pela regra "nunca descarte linhas" [V, `AGENTS.md`] |
| Tabela de quarentena | Isola o problema, mas duplica a lógica e o diretor não vê a receita cheia |
| Silver em SQL | Possível; o projeto escolheu Python na silver e SQL na gold [V, convenção do `AGENTS.md`] |
| Manter o dicionário fixo de UFs | Sem dependência da API na ingestão, mas duplica uma lista que a ingestão já traz. Foi a primeira versão; trocada pela leitura de `bronze.estados_ibge` |

**≈ Fabric.** O pipeline declarativo ≈ **Materialized lake views** do Fabric (SQL com `CONSTRAINT ... ON MISMATCH DROP/FAIL`), ou um notebook Spark agendado. A equivalência não é exata: no Databricks as expectations do tipo *warn* publicam métricas por regra no event log, e o grafo de dependências é montado a partir do código Python e SQL. Em termos de Power Query: é como se cada consulta fosse materializada e o motor descobrisse sozinho a ordem de atualização, com uma "coluna de erro" em vez de "remover linhas com erro".

---

### 3.4 Gold

**O que faz.** Entrega uma tabela pronta para cada pergunta de diretoria, **autoexplicativa**: toda coluna tem tipo e `COMMENT` em português, com unidade (R$), regra de cálculo e avisos que evitam erro do Genie.

**Objetos [V]**

| Tabela (MV) | Grão | Diretoria | Arquivo |
|---|---|---|---|
| `gold.vendas_temporais` | data × hora × canal | Comercial | `ecommerce/src/ecommerce_etl/transformations/gold/vendas_temporais.sql` |
| `gold.vendas_produtos` | produto vendido | Comercial | `.../gold/vendas_produtos.sql` |
| `gold.vendas_detalhadas` | venda (3.020 linhas) | Comercial, cruza as três | `.../gold/vendas_detalhadas.sql` |
| `gold.clientes_segmentacao` | cliente (50, inclusive quem nunca comprou) | Customer Success | `.../gold/clientes_segmentacao.sql` |
| `gold.precos_competitividade` | produto com preço de concorrente (215) | Pricing | `.../gold/precos_competitividade.sql` |
| `gold.qualidade_dados` | regra de qualidade (7) | Todas | `.../gold/qualidade_dados.sql` |

No workspace: `ecommerce.gold.*`. Dependência entre golds [V]: `vendas_detalhadas` lê o segmento de `gold.clientes_segmentacao`, para usar exatamente a mesma regra de segmentação.

**Como foi modelada**

Não é um *star schema*. São **tabelas agregadas por pergunta** mais **uma tabela larga no grão da venda** (`vendas_detalhadas`, o padrão *one big table*) para as perguntas que cruzam dimensões.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_temporais (
  receita DECIMAL(10,2) COMMENT 'Receita em R$ ... Pode ser somada entre linhas.',
  clientes_unicos BIGINT COMMENT '... ATENÇÃO: NÃO somar entre linhas ...'
)
COMMENT 'Vendas agregadas por data, hora e canal ... Use para perguntas sobre quanto vendemos e quando ...'
AS SELECT ...
```

Sem o tipo na lista de colunas, o comentário é ignorado em silêncio [V, `AGENTS.md`]. Por isso toda coluna declara as duas coisas, e um teste garante que **nenhuma coluna da gold fica sem comentário** (0 hoje [V]).

**Regras de negócio [V]**

| Regra | Onde | Resultado |
|---|---|---|
| Todas as vendas entram, inclusive de produto não cadastrado (rótulo "Produto não cadastrado") | Todas as golds de vendas | Receita de R$ 974.077,28 igual em silver e nas 4 golds [V] |
| Segmentos: VIP ≥ R$ 22.000; TOP_TIER ≥ R$ 17.000; REGULAR abaixo | `clientes_segmentacao` | 10 VIP, 25 TOP_TIER, 15 REGULAR [V] |
| Limites antigos (R$ 10.000 e R$ 5.000) abandonados | idem | Com eles quase todos viravam VIP, e um segmento com todo mundo não prioriza nada |
| Classificação de preço, nesta ordem: extremos primeiro, depois a média | `precos_competitividade` | 35 mais caros que todos, 92 acima da média, 6 na média, 76 abaixo, 6 mais baratos que todos [V] |
| Preço suspeito continua nas contas, com alerta `possui_preco_suspeito` | idem | Dos 35 "mais caros que todos", 20 confirmados e 15 suspeitos [V] |
| Diferenças em pontos percentuais (10 = 10%) | idem | O dashboard divide por 100 para usar o formato de % |
| `ROW_NUMBER` com desempate por id | rankings | Mesmo resultado em toda execução |
| `CLUSTER BY (data)` | `vendas_detalhadas` | *Liquid clustering* pela coluna mais filtrada |

**Placar de qualidade [V]** (`gold.qualidade_dados`)

| Regra | Severidade | Linhas | Receita afetada |
|---|---|---:|---:|
| Venda de produto não cadastrado | ALERTA | 20 | R$ 4.240,01 |
| Venda anterior à criação do produto | ALERTA | 5 | R$ 325,88 |
| Preço de concorrente abaixo de 60% do nosso | ALERTA | 55 | |
| Marca do produto diferente da marca citada no nome | ALERTA | 12 | |
| Produto com nome igual ao de outro produto | INFORMATIVO | 137 | |
| Produto monitorado em menos de 4 concorrentes | INFORMATIVO | 109 | |
| Nome de cliente com pronome de tratamento | CORRIGIDO | 11 | |

A receita afetada **continua** nas outras golds: o placar explica o número, não o altera.

**A armadilha do Tênis [V].** A categoria Tênis aparece **+100% acima do mercado**. Os 15 produtos suspeitos são todos de Tênis, nenhum vendeu, e o concorrente cobra a metade do nosso preço. Sem os suspeitos, a categoria mais cara é **Beleza, com +1,24%**. A conclusão para o diretor de Pricing: não há problema de preço generalizado. A ação no Tênis é **conferir a coleta**, não baixar o preço. Os 20 confirmados somam R$ 161.375,09 de receita e são onde vale agir.

**Decisões e porquês**

| Decisão | Por quê |
|---|---|
| Gold em SQL | Quem mantém gold costuma ser analista; SQL é a língua comum com dashboards e Genie |
| Tabela por pergunta, e não star schema | Os consumidores são dashboards (cada dataset é um SQL) e o Genie, que erra menos com poucas tabelas largas e bem comentadas do que com muitos joins |
| Comentários na definição da MV | Sobrevivem a cada refresh. Um `COMMENT ON` feito à parte se perde quando a tabela é recriada |
| `vendas_detalhadas` no grão da venda | Permite filtro cruzado no dashboard e perguntas como "receita por região e categoria" |
| `DECIMAL(10,2)` também nas somas | Padronização. O limite é R$ 99.999.999,99 por valor, folgado para este volume [I] |

**Alternativas descartadas ou não implementadas**

| Alternativa | Trade-off |
|---|---|
| **Star schema** (fato vendas + dimensões produto, cliente, calendário) | É o padrão para Power BI com DAX. Aqui não há modelo semântico: cada widget é um SQL, e o Genie escreve SQL. As agregadas respondem mais rápido e com menos risco de join errado |
| Views comuns em vez de MV | Não ocupam storage, mas recalculam a cada consulta de dashboard ou Genie |
| **Metric views** do Unity Catalog | Definiriam "ticket médio" uma vez só, com governança, para dashboard e Genie. Mais robusto do que repetir a regra em comentário, instrução e SQL de exemplo [I] |
| Deixar a qualidade só no event log | O diretor não abre o event log. Por isso existe `gold.qualidade_dados`: leva o placar ao dashboard e ao Genie. Ela nasceu de uma revisão: na primeira versão, 12 produtos com marca diferente da citada no nome (ex.: "Tênis Nike Revolution" com marca Adidas) ficavam sem medição. Além desses, a marca parece aleatória em boa parte do catálogo sintético ("Camisa Social" da Brastemp); isso não é detectável por regra e deve ser lido como limitação do dado [I] |

**≈ Power BI / DAX.** Três regras da gold são velhas conhecidas de quem escreve DAX:

| Regra aqui | Em DAX |
|---|---|
| Ticket médio = `SUM(receita) / SUM(total_vendas)`, nunca média da coluna `ticket_medio` | `DIVIDE(SUM(receita), SUM(total_vendas))`, e não `AVERAGE` de uma média pré-calculada (média de médias) |
| Não somar `clientes_unicos` entre linhas | `DISTINCTCOUNT` é não aditivo: somar o resultado por dia conta o mesmo cliente várias vezes |
| Dia da semana pela **receita média por dia** (5 sábados × 4 quartas no período) | `AVERAGEX(VALUES('Calendário'[Data]), [Receita])` em vez de `[Receita]` total por dia da semana |
| `ROW_NUMBER` com desempate | `RANKX` com critério de desempate (o `RANKX` sozinho gera empates) |

A diferença de fundo: no Power BI essas regras moram **uma vez** numa medida DAX do modelo semântico. Aqui elas estão repetidas no SQL de cada widget, no comentário da coluna e nas instruções do Genie. É a consequência de não ter uma camada semântica (ver *Metric views* acima).

---

### 3.5 Consumo: dashboards AI/BI e Genie

#### Dashboards

**Objetos [V]**

| Dashboard | Dataset principal | KPIs | Gráficos e tabela | Filtros |
|---|---|---|---|---|
| **Dashboard Comercial** | `vendas_detalhadas` + "Top 10 produtos" com parâmetros | Receita, vendas, ticket médio, itens | Receita por dia e canal, por canal, média por dia da semana, por hora, por categoria; top 10 produtos | Período (UTC) e canal |
| **Dashboard de Customer Success** | `clientes_segmentacao` | Clientes, VIP, % da receita VIP, ticket médio | Clientes e receita por segmento, receita por região; ranking de clientes | Segmento e região |
| **Dashboard de Pricing** | `precos_competitividade` | Monitorados, mais caros que todos (confirmados), receita deles, suspeitos a conferir | Classificação confirmados × a confirmar; diferença média por categoria sem suspeitos; tabela "Onde agir" | Categoria e classificação |

Arquivos: `ecommerce/src/dashboards/<nome>.lvdash.json` e `ecommerce/resources/<nome>.dashboard.yml`. No workspace: menu **Dashboards**, `[dev <seu_usuario>] Dashboard ...`. O arquivo publicado fica em `.bundle/ecommerce/dev/resources/`. Os 3 publicados são idênticos aos JSON do repositório [V].

**Decisões e porquês**

| Decisão | Por quê |
|---|---|
| `FROM vendas_temporais`, sem catálogo nem schema | O recurso injeta `dataset_catalog: ${var.catalog}` e `dataset_schema: gold`; o mesmo JSON serve em dev e prod |
| `warehouse_id` por *lookup* do nome "Serverless Starter Warehouse" | O ID muda entre workspaces; o nome não |
| Top 10 num dataset próprio, com parâmetros `:periodo` e `:canal` ligados aos mesmos filtros | O top 10 precisa agregar **antes** do `LIMIT`; um filtro normal agiria depois |
| Canal exibido como "E-commerce" e "Loja física"; "(UTC)" no eixo e no filtro | Leitura do diretor e honestidade sobre o fuso |
| Botão **Ask Genie** apontando para o space `Diretoria E-commerce` | Do gráfico para a pergunta que o gráfico não previu. O ID do space está fixo no JSON (`overrideId`) [V]: em prod precisa ser trocado pelo id do space de prod |

**≈ Power BI.** Um dashboard AI/BI ≈ um relatório do Power BI em **DirectQuery** contra um SQL endpoint: cada widget dispara SQL no warehouse. **Não é equivalente** em modelagem: não há modelo semântico, relacionamento nem DAX. As medidas são expressões SQL no widget. Os parâmetros ligados a filtros lembram os **dynamic M query parameters** do DirectQuery.

#### Genie

O Genie space `Diretoria E-commerce` atende as três diretorias em português. A configuração e as limitações estão em [4.7](#47-genie-como-funciona-configuração-e-limitações).

---

## 4. Plataforma

### 4.1 Organização do workspace [V]

```
/Workspace/Users/<seu_usuario>/
├── .bundle/ecommerce/dev/          ← tudo o que o `bundle deploy` publica
│   ├── files/                      ← cópia do repositório (databricks.yml, src/, resources/, AGENTS.md…)
│   ├── resources/                  ← os 3 dashboards (.lvdash.json) publicados
│   ├── state/                      ← estado do deploy (o que o bundle criou)
│   └── artifacts/
├── 01_ingestao_bronze              ← primeira versão da ingestão, com chaves no código: apagar
├── Drafts/New Notebook …           ← rascunho
└── ecommerce/                      ← pasta vazia
```

O Genie space também fica em `.bundle/ecommerce/dev`, porque o recurso usa `parent_path: ${workspace.root_path}` [V]. Em prod, tudo vai para `.bundle/ecommerce/prod`. Existe um secret scope (`ecommerce`); não há Git folder conectado [V].

**≈ Fabric (não exato).** Um *workspace* do Databricks é o ambiente inteiro: pastas, compute, jobs e acesso ao catálogo. Um *workspace* do Fabric está mais perto de uma "pasta de projeto" com itens (lakehouse, notebook, relatório). O que organiza os **dados** no Databricks é o Unity Catalog, e não a pasta.

### 4.2 Unity Catalog

```
metastore
└── catálogo  ecommerce            (gerenciado; dono: <seu_usuario>)
    ├── schema bronze   → 5 tabelas Delta
    ├── schema silver   → 4 materialized views
    ├── schema gold     → 6 materialized views
    └── information_schema (automático)

catálogo  ecommerce_prod           (target prod; criado pela ingestão no primeiro run)
```

Um catálogo por ambiente, na variável `catalog` do bundle: `ecommerce` em dev e `ecommerce_prod` em prod. Na primeira versão os dois targets apontavam para `ecommerce`, e um deploy em prod criaria um segundo pipeline disputando as mesmas tabelas.

Outros catálogos no workspace [V]: `workspace` (padrão), `samples` (exemplos) e `system` (tabelas de sistema: billing, audit, lineage). **Volumes:** nenhum [V].

**Permissões hoje [V]**

| Objeto | Quem | Privilégio |
|---|---|---|
| Catálogo `ecommerce` | `account users` | `BROWSE` (vê que existe, não lê dados) |
| Schemas bronze/silver/gold | — | Nenhum grant explícito |
| Tabelas | dono = `<seu_usuario>` | Tudo, por ser dono |

O dashboard Comercial publicado tem `embed_credentials: false` [V]: cada pessoa que abre vê só o que o Unity Catalog permite a ela. Para um diretor usar, ele precisaria de: `USE CATALOG` em `ecommerce`, `USE SCHEMA` e `SELECT` em `gold`, `CAN USE` no warehouse, `CAN VIEW` no dashboard e `CAN RUN` no Genie space.

**≈ Fabric (não exato).** Unity Catalog ≈ **OneLake catalog + permissões de item + OneLake security + Purview**, num lugar só. A hierarquia de 3 níveis `catalogo.schema.tabela` não tem espelho direto no Fabric (lá é *workspace → item → schema → tabela*). Volume ≈ a seção **Files** de um Lakehouse. Uma diferença que pesa: no UC as permissões são SQL (`GRANT SELECT ON SCHEMA ...`) e valem igual para notebook, dashboard e Genie.

### 4.3 Compute [V]

| Onde roda | Compute | Detalhe |
|---|---|---|
| Pipeline (silver e gold) | Serverless | `serverless: true`, modo development |
| Tarefas de ingestão e de testes do Job (notebooks) | Serverless | O Job não declara cluster |
| Dashboards, Genie e consultas SQL | SQL warehouse **Serverless Starter Warehouse** | PRO, serverless, 2X-Small, 1 cluster no máximo, desliga após 10 min parado |
| Clusters clássicos | — | Nenhum |

[I] O conjunto (warehouse "Starter", catálogo `workspace`, nenhum cluster) é o padrão da **Databricks Free Edition**.

**≈ Fabric (não exato).** No Fabric tudo consome uma **capacidade** (F SKU) compartilhada. No Databricks cada tipo de compute é cobrado à parte em DBUs: serverless de pipeline e job, SQL warehouse. O "desliga após 10 min" do warehouse é o controle de custo mais visível.

### 4.4 Jobs / workflows [V]

`[dev <seu_usuario>] Pipeline E-commerce` (`ecommerce/resources/pipeline_ecommerce.job.yml`):

```
ingestao_bronze (notebook_task) ──► atualizar_pipeline (pipeline_task) ──► testes_qualidade (notebook_task)
```

- Parâmetro de Job `catalogo` (`${var.catalog}`), que chega como widget em todos os notebooks.
- Agendado todo dia às 6h (`America/Sao_Paulo`), com e-mail na falha. Em dev o modo development pausa o agendamento sozinho; por isso o YAML **não** declara `pause_status` (um `UNPAUSED` explícito faria o Job de dev rodar todo dia — o `bundle validate -o json` mostrou isso antes do deploy).
- Última execução com as 3 tarefas: `SUCCESS` (ingestão 83 s, pipeline 162 s, testes 44 s) [V].

**≈ Fabric.** Job ≈ **Data pipeline** do Fabric (orquestração com dependências). A tarefa `pipeline_task` ≈ uma atividade que dispara a atualização de materialized lake views; a `notebook_task` ≈ uma atividade de notebook.

### 4.5 Notebooks vs. arquivos

| Artefato | Formato | Por quê |
|---|---|---|
| Silver (`.py`) e gold (`.sql`) | **Arquivos** comuns, sem o cabeçalho `# Databricks notebook source` | São código-fonte do pipeline: o motor lê a definição e monta o grafo. Não têm células nem `display` |
| Testes (`testes_qualidade.py`) | **Notebook** em formato *source* (`# Databricks notebook source`, células `# COMMAND ----------`, markdown em `# MAGIC %md`) | Roda como `notebook_task`: usa `dbutils.widgets`, `display` e mostra a tabela de resultados na execução do Job |
| Ingestão (`ingestao_bronze.py`) | **Notebook** em formato *source* | Roda como `notebook_task`: usa `%pip`, widgets, `dbutils.secrets` e `display` na conferência final |

O formato *source* é texto puro: fica legível no Git e no diff, ao contrário do `.ipynb`.

**≈ Fabric.** Notebook ≈ notebook do Fabric (bem próximo). Arquivo `.py` de pipeline ≈ a definição SQL de uma materialized lake view; não é um notebook.

### 4.6 Databricks CLI: comandos usados e para quê

**Na construção** (documentados no `AGENTS.md` e nos prompts [V]; execução [I], confirmada indiretamente pelo estado do deploy e pelo histórico)

| Comando | Para quê |
|---|---|
| `databricks pipelines init` | Gera o esqueleto do bundle (template *lakeflow-pipelines*) [I: os READMEs do template ficaram no repo] |
| `databricks auth login --host … --profile ecommerce_profile` | Autentica por OAuth e grava o perfil em `~/.databrickscfg` |
| `databricks auth token -p ecommerce_profile` | Usado por `.claude/databricks-mcp-headers.sh` para entregar ao MCP um token sempre renovado [V] |
| `databricks bundle validate --strict -t dev -p …` | Valida o YAML antes do deploy |
| `databricks bundle validate -t prod -p … -o json` | Mostra a configuração já resolvida; foi assim que se confirmou que `${var.catalog}` vira `ecommerce_prod` dentro do Genie |
| `databricks secrets create-scope ecommerce` · `secrets put-secret ecommerce <chave>` | Cria o cofre e grava endpoint e chaves do Supabase (o valor é pedido no terminal, fora do histórico do shell) |
| `databricks bundle deploy -t dev -p …` | Publica pipeline, job, dashboards e Genie (o estado local registra 11 deploys [V]) |
| `databricks bundle run pipeline_ecommerce -t dev -p …` | Roda o Job (5 execuções `ONE_TIME` [V]) |
| `databricks experimental aitools tools query -p … -- "SELECT …"` | Testa no warehouse cada consulta que vira dataset |
| `databricks genie start-conversation` | Faz as perguntas de teste ao Genie pela API de conversa (6 rodadas) |

**No levantamento deste guia** (só leitura [V])

| Comando | Para quê |
|---|---|
| `databricks auth profiles`, `current-user me` | Perfil válido e usuário |
| `catalogs list`, `schemas list`, `tables list`, `volumes list`, `grants get` | Inventário do Unity Catalog e permissões |
| `pipelines list-pipelines / get / list-updates` | Configuração e histórico do pipeline |
| `jobs list / get / list-runs` | Tarefas, parâmetros e execuções do Job |
| `warehouses list`, `clusters list` | Compute |
| `lakeview list / get / get-published` | Dashboards e comparação com o JSON do repositório |
| `genie list-spaces / get-space --include-serialized-space` | Genie space e comparação com o JSON do repositório |
| `genie list-conversations / list-conversation-messages` | Placar das rodadas de teste |
| `workspace list / export` | Pastas e notebook de ingestão |
| `experimental aitools tools query` | Conferir os números de referência |

Lembrete prático: todo comando leva `-p ecommerce_profile`. Numa `zsh`, `P="--profile x"; databricks ... $P` **não funciona**, porque a variável não é quebrada em palavras.

**≈ Fabric.** Databricks CLI ≈ **Fabric CLI** (`fab`), mais o `az` e o PowerShell. O Asset Bundle ≈ **Git integration + deployment pipelines** (ou a biblioteca `fabric-cicd`). A equivalência não é exata: o bundle é declarativo (um YAML descreve o estado desejado e o CLI converge o workspace para ele).

### 4.7 Genie: como funciona, configuração e limitações

#### Como funciona

O Genie é um agente *text-to-SQL*:

```
pergunta em português
   → lê o contexto do space (tabelas, comentários, instruções, joins, exemplos)
   → um modelo de linguagem escreve o SQL
   → o SQL roda no SQL warehouse com as permissões do Unity Catalog de quem perguntou
   → devolve tabela, gráfico e um resumo em texto (com "Show code" para ver o SQL)
```

Ele **não sabe nada da empresa** além do que está no space. Por isso a qualidade da gold (comentários, nomes, uma tabela por pergunta) vale mais do que qualquer instrução.

#### Como foi configurado [V]

Arquivo: `ecommerce/resources/diretoria.genie_space.yml`. O *serialized space* (o JSON do space) fica **dentro** do YAML, no campo `serialized_space`, para os identificadores das tabelas usarem `${var.catalog}.gold.<tabela>`: o mesmo space vale para `ecommerce` em dev e `ecommerce_prod` em prod. Na primeira versão ele era um arquivo JSON separado, com `ecommerce.gold` escrito à mão, porque arquivo não passa por variáveis do bundle. O space publicado é idêntico ao que o bundle resolve [V].

| Camada de contexto | Conteúdo |
|---|---|
| **Tabelas** | As 6 golds (`${var.catalog}.gold.*`); nenhuma bronze ou silver |
| **Comentários** | Vêm das próprias MVs da gold (Unity Catalog) |
| **Sinônimos** | faturamento → `receita`, UF → `estado`, canal → `canal_venda`, perfil → `segmento_cliente`, posição de preço → `classificacao_preco` |
| **Entity matching / format assistance** | Nas colunas categóricas (região, segmento, categoria, marca, regra de qualidade…), para o Genie casar "sudeste" com "Sudeste" e "marca diferente" com a regra certa |
| **Instruções gerais** | 15 regras, cerca de 2.770 caracteres: idioma e formato de R$, "não existe lucro", período fixo, recusar "hoje/ontem", qual tabela usar, qualidade dos dados pela contagem pronta, ticket médio, top N antes do filtro, dia da semana pela média, segmentos, "mais caro que o mercado", separar preço suspeito, o que trazer em "qual X vende mais", nomes dos canais, rankings com 10 linhas |
| **Joins** | `vendas_produtos` × `precos_competitividade` (1:1) e `vendas_detalhadas` × `clientes_segmentacao` (N:1) |
| **SQL de exemplo** | 6 pares pergunta → SQL: ticket por segmento, participação TOP_TIER, receita por região e categoria, produtos acima da média do mercado (confirmado × a confirmar), clientes e receita por segmento em cada região, problemas de qualidade e receita afetada |
| **Medida (SQL snippet)** | `ticket_medio = SUM(vendas_temporais.receita) / SUM(vendas_temporais.total_vendas)` |
| **Perguntas da tela inicial** | 6, duas por diretoria |

#### Como foi testado [V]

12 perguntas pela API de conversa: 10 com resposta conhecida mais 2 de limite ("Qual foi o nosso lucro?" e "Quanto vendemos ontem?"). Só conta acerto se o **texto** trouxer todos os números esperados. A partir da rodada 5 entram também as 6 perguntas da tela inicial (não podem voltar vazias) e 2 perguntas sobre qualidade dos dados. Foram 6 rodadas:

| Rodada | Placar | O que falhou |
|---|---|---|
| 1 | 9/10 · 2/2 | "Qual região gera mais receita?" veio sem o número de clientes (17) |
| 2 | 8/10 · 2/2 | A mesma de região, e "Dos 10 produtos que mais faturam, quais estão mais caros que a média?" respondeu "todos os 10": filtrou antes de montar o ranking |
| 3 | 10/10 · 2/2 | — |
| 4 | 10/10 · 2/2 | — |
| 5 | 10/10 · 2/2 · tela inicial 6/6 · qualidade 1/2 | Depois de entrar `qualidade_dados`: "Quantos produtos têm a marca diferente da citada no nome?" respondeu **183**. O Genie ignorou o placar e inventou a regra "a marca não aparece no nome" |
| 6 | 10/10 · 2/2 · tela inicial 6/6 · qualidade 2/2 | — |

As duas correções aparecem nas instruções locais e **não existem no implementação de referência da imersão** [V]:

- "pegue primeiro o top N por `ranking_receita <= N` e só depois aplique o filtro; nunca filtre antes de ranquear";
- o SQL de exemplo "Qual estado vende mais?", que traz `COUNT(DISTINCT id_cliente)`.

[I] A ordem dos acontecimentos sugere que as duas foram acrescentadas entre as rodadas 2 e 3.

Na revisão para publicação, o SQL de exemplo "Qual estado vende mais?" foi trocado por "Quantos clientes e quanta receita cada segmento tem em cada região?": ele tinha a mesma forma da pergunta de teste "Qual região gera mais receita?" e inflava o placar. A pergunta de região continuou certa nas rodadas 5 e 6. A falha da rodada 5 foi corrigida com **SQL de exemplo** genérico ("Quais problemas de qualidade temos nos dados…"), *entity matching* na coluna `regra` e a instrução "use a contagem pronta de `qualidade_dados`; nunca recalcule a regra com outra tabela".

Nas duas perguntas de limite, o Genie não gerou SQL e respondeu em texto. Exemplo real [V]: "Os dados vão de 13/12/2025 a 11/01/2026, e 11/01/2026 não é ontem. Você quer ver as vendas do dia 11/01/2026?"

#### Limitações

| Limitação | Consequência | Mitigação |
|---|---|---|
| **Não determinístico** | A pergunta dos "10 produtos" acertou na rodada 1 e errou na 2 [V] | Refazer as 12 perguntas a cada mudança; usar os **benchmarks** do Genie (o CLI tem `genie-create-eval-run`, em Beta; nenhuma rodada criada neste space [V]) |
| Regra em texto é frágil | Regra que falha como instrução costuma passar como SQL de exemplo [V, `AGENTS.md`] | Preferir SQL de exemplo e comentário de coluna |
| **Exemplo de valor vira "dado"** | O Genie citou "R$ 1.234,56" como se fosse real quando havia um exemplo nas instruções [V, `AGENTS.md` deste projeto] | Não colocar valores de exemplo nas instruções |
| Risco de "cola" no teste | SQL de exemplo parecido demais com uma pergunta de teste infla o placar | Manter os exemplos longe das perguntas de aceitação (foi preciso trocar um, ver acima) |
| Recalcula em vez de consultar | Com uma tabela pronta disponível, o Genie ainda inventou uma regra própria (183 no lugar de 12) [V] | SQL de exemplo + entity matching + instrução explícita |
| **Tabela precisa existir antes do deploy** | O deploy falhou com `Table 'ecommerce.gold.qualidade_dados' does not exist`: a API do Genie valida as tabelas, e a MV só nasce quando o pipeline roda [V] | Em ambiente novo (ex.: o primeiro deploy de prod): deploy → rodar o Job → deploy de novo |
| Ajuste pela interface se perde | O próximo `bundle deploy` sobrescreve o space | Editar só o JSON |
| Período escrito à mão | "13/12/2025 a 11/01/2026" está nas instruções, nos comentários e nos subtítulos dos dashboards [V]. Com dado novo, tudo fica errado | Ver [Gaps](#7-gaps-para-produção) |
| Não é camada semântica | O snippet de ticket médio é uma dica para o modelo, não uma medida governada | Metric views do Unity Catalog |
| Depende do warehouse | Primeira pergunta do dia espera o warehouse subir; exige warehouse Pro ou Serverless [V, ajuda do CLI] | Aceitar o *cold start* ou ajustar o auto-stop |
| Permissões | Quem não tem `SELECT` na gold não recebe resposta | Grants para o grupo dos diretores |

**≈ Fabric (não exato).** Genie space ≈ **Fabric Data Agent**: fontes de dados escolhidas, instruções do agente e *example queries*. Também lembra o **Copilot do Power BI** com o recurso **Prep data for AI** (esquema para IA, *AI instructions* e *verified answers* ≈ SQL de exemplo). Diferenças: o Data Agent pode responder com **DAX sobre um modelo semântico**; o Genie sempre escreve **SQL** sobre tabelas do Unity Catalog. No Power BI, a regra "ticket médio" viveria numa medida DAX; aqui ela é repetida como comentário, instrução e exemplo.

---

## 5. Papel da IA na construção

O projeto foi escrito pelo **Claude Code** com o plugin Databricks, a partir de 6 prompts do material da imersão: 4 para silver e golds e 2 para dashboards e Genie. Os prompts não são republicados aqui; estão no [repositório da imersão](https://github.com/lvgalvao/Imersao-Jornada-Databricks). As convenções que os prompts mandaram gravar ficaram no `ecommerce/AGENTS.md`, que o `CLAUDE.md` importa [V].

Linha do tempo [V, pelos timestamps do workspace]: rascunho de ingestão em 22/09; 5 execuções do Job em 23/09; Genie criado, testado em 4 rodadas e ligado aos dashboards em 24/09. Na revisão para publicação (24/09), a ingestão entrou no Job, os ambientes foram separados, nasceu `gold.qualidade_dados` e o Genie passou por mais 2 rodadas.

### Onde acelerou

| Tarefa | Por quê |
|---|---|
| Esqueleto completo do bundle (pipeline, job, 3 dashboards, Genie) | O JSON de um dashboard tem cerca de 750 a 1.000 linhas [V]; ninguém escreve isso à mão com prazer |
| Comentários de todas as colunas gold | Tarefa longa e repetitiva, que é exatamente o que o Genie precisa |
| Exploração dos dados antes de codar | Os prompts exigem mostrar os números antes de escrever código |
| Loop de teste do Genie | 48 conversas de teste (12 perguntas × 4 rodadas) em poucos minutos, pela API [V] |
| Consistência | As mesmas regras (DECIMAL, marcar e não apagar, um arquivo por tabela) aplicadas em todos os arquivos |

### Onde errou [V, salvo indicação]

1. **Genie, rodada 1:** resposta de região sem o número de clientes.
2. **Genie, rodada 2:** filtrou antes do ranking e disse que "todos os 10" estavam acima da média.
3. **Genie citando exemplo como dado real** ("R$ 1.234,56"), registrado no `AGENTS.md`.
4. **Restos do template:** o prompt mandou apagar os exemplos. Os arquivos saíram, mas os READMEs do template (que citavam `sample_trips_ecommerce.py` e `sample_job.job.yml`, inexistentes) ficaram até a revisão para publicação.
5. **Divergência silenciosa:** seguiu o prompt ("não existe tabela de estados na bronze") sem avisar que `bronze.estados_ibge` existe no workspace.
6. **Defeito não detectado:** 12 produtos têm no nome uma marca diferente da cadastrada ("Tênis Nike Revolution" com marca Adidas). Os prompts não pediram, e a IA não sinalizou, nem quando o próprio Genie respondeu com "Notebook Inspiron 15 (Adidas)".
7. **Configuração de ambientes:** na primeira versão, dev e prod usavam o **mesmo catálogo** `ecommerce` no `databricks.yml`, e o target prod tinha host e e-mail fixos. Veio do template e não foi questionado até a revisão.
8. [I] **Notebook de ingestão sem os imports de `pandas` e `io`.** Esse ponto é humano, não da IA: o notebook foi montado à mão no workspace.
9. **Genie recalculando em vez de consultar** (rodada 5): 183 produtos com "marca diferente" no lugar de 12.
10. **Ordem de deploy:** a revisão incluiu uma tabela nova no Genie antes de ela existir, e o deploy do space falhou. O resto do deploy foi aplicado; bastou rodar o Job e implantar de novo.

A revisão também **pegou erros antes do deploy**: o `bundle validate -o json` mostrou que um `pause_status: UNPAUSED` explícito faria o Job de dev rodar todo dia, e foi removido.

### Onde exigiu revisão humana

| Decisão | Por que não dá para delegar |
|---|---|
| Limites de segmentação (R$ 22.000 e R$ 17.000) | Regra de negócio: vem da diretora, não do dado |
| "Marcar e não apagar" | Escolha contábil: receita é receita, mesmo com cadastro ruim |
| Limite de 60% para preço suspeito | Critério de negócio para separar promoção ou erro de preço real |
| Leitura do SQL gerado pelo Genie ("Show code") | Único jeito de saber se o número está certo pelo motivo certo |
| A conclusão do Tênis | A IA mostra os números; decidir "conferir a coleta, não baixar preço" é do diretor |
| Os números de referência | Aqui vieram do material da imersão, como resposta conhecida. **Num projeto real não há resposta pronta**: a proteção passa a ser a reconciliação entre camadas (os testes de receita silver × gold), que independe da IA |

---

## 6. Mapa: onde encontro cada coisa

| Item | Tipo | Caminho no repo | Caminho no workspace |
|---|---|---|---|
| Definição do projeto | Bundle (YAML) | `ecommerce/databricks.yml` | `/Workspace/Users/<seu_usuario>/.bundle/ecommerce/dev/files/databricks.yml` |
| Convenções para IA | Markdown | `ecommerce/AGENTS.md` (importado por `ecommerce/CLAUDE.md`) | `.bundle/ecommerce/dev/files/AGENTS.md` |
| Ingestão | Notebook | `ecommerce/src/ingestao/ingestao_bronze.py` | `.bundle/ecommerce/dev/files/src/ingestao/ingestao_bronze` (1ª tarefa do Job) |
| Credenciais do data lake | Secret scope | — | Secret scope `ecommerce` (`databricks secrets list-secrets ecommerce`) |
| Tabelas bronze (5) | Delta gerenciada | — (escritas pela ingestão) | Catalog Explorer › `ecommerce` › `bronze` |
| Pipeline | Lakeflow pipeline (YAML) | `ecommerce/resources/ecommerce_etl.pipeline.yml` | Jobs & Pipelines › `[dev <seu_usuario>] ecommerce_etl` |
| Silver (4 tabelas) | Arquivos Python | `ecommerce/src/ecommerce_etl/transformations/silver/*.py` | `ecommerce.silver.*` (MV) · código em `.bundle/ecommerce/dev/files/src/ecommerce_etl/transformations/silver/` |
| Gold (6 tabelas) | Arquivos SQL | `ecommerce/src/ecommerce_etl/transformations/gold/*.sql` | `ecommerce.gold.*` (MV) · código em `.bundle/…/transformations/gold/` |
| Métricas das expectations | Event log | — | Pipeline › tabela › aba *Data quality*; SQL `event_log(TABLE(ecommerce.silver.vendas))` |
| Testes de qualidade (22) | Notebook | `ecommerce/src/ecommerce_etl/testes/testes_qualidade.py` | `.bundle/ecommerce/dev/files/src/ecommerce_etl/testes/testes_qualidade` |
| Job | Job (YAML) | `ecommerce/resources/pipeline_ecommerce.job.yml` | Jobs & Pipelines › `[dev <seu_usuario>] Pipeline E-commerce` |
| Dashboard Comercial | AI/BI (JSON + YAML) | `ecommerce/src/dashboards/diretoria_comercial.lvdash.json` · `ecommerce/resources/diretoria_comercial.dashboard.yml` | Dashboards › `[dev <seu_usuario>] Dashboard Comercial` |
| Dashboard de Customer Success | AI/BI (JSON + YAML) | `ecommerce/src/dashboards/diretoria_customer_success.lvdash.json` · `resources/diretoria_customer_success.dashboard.yml` | Dashboards › `[dev <seu_usuario>] Dashboard de Customer Success` |
| Dashboard de Pricing | AI/BI (JSON + YAML) | `ecommerce/src/dashboards/diretoria_pricing.lvdash.json` · `resources/diretoria_pricing.dashboard.yml` | Dashboards › `[dev <seu_usuario>] Dashboard de Pricing` |
| Placar de qualidade | MV (SQL) | `ecommerce/src/ecommerce_etl/transformations/gold/qualidade_dados.sql` | `ecommerce.gold.qualidade_dados` |
| Genie space | Serialized space dentro do YAML | `ecommerce/resources/diretoria.genie_space.yml` | Genie › `[dev <seu_usuario>] Diretoria E-commerce` (em `.bundle/ecommerce/dev`) |
| Histórico de testes do Genie | Conversas | — | Genie space › histórico (6 rodadas) |
| SQL warehouse | Compute | Lookup pelo nome em `ecommerce/databricks.yml` | SQL Warehouses › `Serverless Starter Warehouse` |
| Estado do deploy | JSON | `ecommerce/.databricks/` (ignorado pelo Git) | `.bundle/ecommerce/dev/state/` |
| MCP de SQL para o Claude Code | Config | `.mcp.json.example` (copiar para `.mcp.json`, que fica fora do Git) · `.claude/databricks-mcp-headers.sh` | Endpoint `/api/2.0/mcp/sql` do workspace |
| Recomendações do VS Code | Config | `ecommerce/.vscode/` | — |

---

## 7. Gaps para produção

O projeto está pronto para **demonstração em dev** e preparado para um primeiro deploy em prod. A revisão para publicação já resolveu parte dos gaps (marcados com ✅); o resto está em ordem aproximada de prioridade.

### Já resolvidos na revisão

- ✅ Ingestão versionada, lendo credenciais do secret scope e rodando como 1ª tarefa do Job.
- ✅ Agendamento diário (6h) e e-mail na falha; pausado automaticamente em dev.
- ✅ Um catálogo por ambiente (`ecommerce` × `ecommerce_prod`); host e e-mail fora do `databricks.yml`.
- ✅ Genie sem catálogo escrito à mão (`${var.catalog}` no `serialized_space`).
- ✅ Placar de qualidade (`gold.qualidade_dados`) no Genie, com a regra de marca divergente.
- ✅ Teste de período: se o dado mudar, o Job fica vermelho até os textos serem atualizados.
- ✅ SQL de exemplo que funcionava como "cola" do teste trocado.

### Testes

| Existe hoje [V] | Falta |
|---|---|
| 22 testes de dados entre tabelas (chaves, reconciliação de receita, limites, período, placar, comentários) | Testes unitários das funções Python (ex.: `_nome_titulo` com "Sra. Maria da Silva"), com `pytest` local ou Databricks Connect |
| 19 expectations na silver | Teste das consultas dos dashboards (hoje foram testadas uma vez, na construção) |
| 12 perguntas de aceitação + 8 de apoio no Genie, rodadas por script | Transformar as perguntas num **benchmark** do Genie, reexecutado a cada mudança |

### CI/CD (Databricks Asset Bundles)

- O bundle existe, mas não há pipeline de CI. Próximo passo: GitHub Actions com `bundle validate --strict` em todo PR e `bundle deploy -t prod` no merge, autenticando com **service principal** (OAuth M2M) e `run_as`, nunca com usuário.
- O `overrideId` do botão "Ask Genie" nos 3 dashboards ainda aponta para o space de dev [V]. Em prod, trocar pelo id do space de prod (`databricks bundle summary -t prod`) ou gerar o JSON por ambiente no CI.
- Primeiro deploy de um ambiente novo exige a ordem **deploy → Job → deploy**, por causa da validação de tabelas do Genie.

### Orquestração e dados

- **Período escrito à mão** em comentários da gold, instruções do Genie e subtítulos dos dashboards. O teste de período impede que isso fique errado em silêncio, mas atualizar ainda é manual. Se o dado passar a ser vivo, calcular o período por SQL (`MIN/MAX(data)`) nos dashboards e tirar a data das instruções.
- Carga full [V] → avaliar Auto Loader, volume de arquivos brutos e colunas de controle (`_ingerido_em`, `_arquivo_origem`).

### Monitoramento

- Alertas sobre as expectations: ex.: alerta SQL se `produto_cadastrado` passar de 1% (hoje só o teste pega) ou se `preco_plausivel` crescer.
- Frescor: teste que falha se `MAX(data_venda)` não avançar depois de uma ingestão (hoje o teste de período faz o contrário, porque o dataset é fixo).
- Placar de qualidade num dashboard para o time de dados.

### Custo

- Acompanhar consumo pela tabela de sistema `system.billing.usage` (serverless de pipeline, de job e de warehouse).
- Warehouse com auto-stop de 10 min e 1 cluster no máximo [V]: adequado. Numa conta paga, usar **budget policies** e tags de custo.
- O refresh das MVs pode ser incremental no serverless; em volume maior, medir antes de trocar por outra estratégia.

### Governança

- Apagar o notebook antigo `01_ingestao_bronze` do workspace e **trocar as chaves do Supabase** que estavam nele.
- Grants mínimos para um grupo `diretoria`: `USE CATALOG`, `USE SCHEMA` e `SELECT` só em `gold`; `CAN USE` no warehouse; `CAN RUN` no Genie; `CAN VIEW` nos dashboards.
- Dono das tabelas é um usuário [V] → passar para um grupo ou service principal (se a pessoa sair da empresa, nada fica órfão).
- Dado pessoal: `nome_cliente` está na gold e no Genie. Avaliar *column mask* ou remover o nome das golds que não precisam dele.
- Decidir `embed_credentials` dos dashboards (hoje `false` [V]): com `true`, o diretor não precisa de acesso às tabelas, mas as consultas rodam com a permissão de quem publicou.

---

## 8. Glossário

| Termo | Em uma frase | ≈ Fabric / Power BI |
|---|---|---|
| **Lakehouse** | Arquitetura que junta storage barato de data lake com tabelas, SQL e governança de warehouse | Lakehouse do Fabric (o conceito é o mesmo) |
| **Delta Lake / tabela Delta** | Formato de tabela com schema, transações ACID, histórico e *time travel* | Tabelas do Lakehouse no OneLake (mesmo formato; equivalência exata) |
| **Unity Catalog (UC)** | Catálogo central: organiza `catalogo.schema.tabela` e controla acesso para tudo | OneLake catalog + permissões + Purview (não exato) |
| **Catálogo** | Nível mais alto da hierarquia do UC; aqui, `ecommerce` | Mais perto de um workspace ou item do Fabric (não exato) |
| **Schema** | Agrupamento dentro do catálogo; aqui, um por camada | Schema de um Lakehouse |
| **Volume** | Pasta governada pelo UC para arquivos (CSV, Parquet, JSON) | Seção *Files* do Lakehouse |
| **Tabela gerenciada** | O UC controla onde o dado mora; apagar a tabela apaga o dado | Tabela gerenciada do Lakehouse |
| **Arquitetura medalhão** | Camadas bronze (como chegou), silver (confiável) e gold (pronto para o negócio) | Mesmo padrão recomendado no Fabric |
| **Lakeflow Declarative Pipelines** (antigo DLT) | Você declara *o que* cada tabela é; o Databricks descobre a ordem, roda e mede a qualidade | Materialized lake views (não exato) |
| **Materialized view (MV)** | View cujo resultado fica gravado e é recalculado pelo pipeline | Materialized lake view; em Power BI, lembra uma agregação importada |
| **Streaming table** | Tabela que só acrescenta linhas novas; não serve para fonte sobrescrita | Eventstream / tabela incremental (não exato) |
| **Expectation** | Regra de qualidade por linha: *warn* (mede), *drop* (descarta) ou *fail* (para o pipeline) | `CONSTRAINT … ON MISMATCH` das materialized lake views |
| **Event log** | Registro do pipeline com execuções e métricas de qualidade, consultável em SQL | Monitoring hub (não exato) |
| **Liquid clustering** (`CLUSTER BY`) | Organiza os arquivos pela coluna mais filtrada, sem partição fixa | V-Order / otimização de tabela (não exato) |
| **Job** | Tarefas com dependências (DAG), agendamento e alertas | Data pipeline do Fabric |
| **Serverless** | O Databricks aloca a máquina quando precisa; você não liga nem desliga cluster | Capacidade do Fabric (modelo de cobrança diferente) |
| **SQL warehouse** | Compute que executa SQL para dashboards, Genie e editor SQL | Warehouse / SQL analytics endpoint |
| **DBU** | Unidade de cobrança de compute do Databricks | CU (capacity unit) do Fabric (não exato) |
| **Notebook (formato source)** | Notebook salvo como `.py` ou `.sql` com marcadores de célula; legível no Git | Notebook do Fabric |
| **Databricks Asset Bundle (DAB)** | O projeto descrito em YAML e implantado com um comando; alvos `dev` e `prod` | Git integration + deployment pipelines / `fabric-cicd` (não exato) |
| **Target** (`dev`, `prod`) | Ambiente de deploy do bundle; `dev` prefixa nomes e pausa agendamentos | Estágio de um deployment pipeline |
| **Databricks CLI** | O Databricks pelo terminal | Fabric CLI (`fab`) |
| **Secret scope** | Cofre de segredos lido com `dbutils.secrets.get`; mascara o valor na saída | Azure Key Vault + `notebookutils.credentials` |
| **Dashboard AI/BI** (Lakeview) | Dashboard de datasets SQL e widgets, publicado com link | Relatório do Power BI em DirectQuery, sem modelo semântico (não exato) |
| **Genie space** (Genie Agent) | Agente que transforma pergunta em SQL sobre tabelas escolhidas, com instruções e exemplos | Fabric Data Agent / Copilot com *Prep data for AI* (não exato) |
| **Serialized space** | O JSON que descreve um Genie space: tabelas, instruções, joins, exemplos | Definição de um Data Agent |
| **Entity matching** | O Genie conhece os valores reais de uma coluna para casar o que o usuário escreveu | Sinônimos do Q&A / *linguistic schema* |
| **SQL snippet (medida)** | Expressão SQL nomeada que o Genie reutiliza, como "ticket médio" | Medida DAX, porém sem governança de camada semântica |
| **Metric view** | Métrica de negócio governada no UC, definida em YAML, usada por dashboards e Genie | Medida do modelo semântico (a equivalência mais próxima) |
| **MCP (Model Context Protocol)** | Padrão para conectar uma IA a sistemas; aqui, o Claude Code ao SQL do workspace | — |
| **Text-to-SQL** | Gerar SQL a partir de linguagem natural | O que o Copilot faz com DAX |
