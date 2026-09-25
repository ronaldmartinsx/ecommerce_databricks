# Databricks notebook source
# MAGIC %md
# MAGIC # Testes de qualidade do pipeline e-commerce
# MAGIC
# MAGIC Roda depois do pipeline, dentro do Job "Pipeline E-commerce". Cada teste é uma consulta que
# MAGIC **conta as linhas com problema**: o esperado é sempre 0. Se algum teste achar problema, o
# MAGIC notebook mostra a tabela de resultados e falha com `AssertionError`, e o Job fica vermelho.
# MAGIC
# MAGIC As expectations do pipeline medem a qualidade de cada tabela isoladamente. Estes testes
# MAGIC conferem regras que cruzam tabelas (chaves, receita batendo entre camadas, limites).

# COMMAND ----------

dbutils.widgets.text("catalogo", "ecommerce")
catalogo = dbutils.widgets.get("catalogo")
spark.sql(f"USE CATALOG `{catalogo}`")

# COMMAND ----------

# Período que os comentários da gold, as instruções do Genie e os subtítulos dos dashboards citam.
# O dataset é uma foto fixa: se a bronze trouxer outro período, o teste de período falha e obriga a
# atualizar esses textos junto com estas duas constantes.
PERIODO_INICIO = "2025-12-13"
PERIODO_FIM = "2026-01-11"

# (nome do teste, consulta que retorna uma única coluna com a quantidade de problemas)
TESTES = [
    # Período documentado = período real dos dados.
    (
        f"silver.vendas: período de {PERIODO_INICIO} a {PERIODO_FIM}",
        f"""SELECT CASE WHEN min(data) = DATE'{PERIODO_INICIO}' AND max(data) = DATE'{PERIODO_FIM}'
                        THEN 0 ELSE 1 END FROM silver.vendas""",
    ),
    # Chaves únicas das silver: duplicata multiplica linhas nos joins e infla a receita.
    (
        "silver.produtos: id_produto único",
        "SELECT count(*) - count(DISTINCT id_produto) FROM silver.produtos",
    ),
    (
        "silver.clientes: id_cliente único",
        "SELECT count(*) - count(DISTINCT id_cliente) FROM silver.clientes",
    ),
    (
        "silver.preco_competidores: id_produto + nome_concorrente único",
        """SELECT count(*) FROM (
             SELECT id_produto, nome_concorrente FROM silver.preco_competidores
             GROUP BY ALL HAVING count(*) > 1)""",
    ),
    (
        "silver.vendas: id_venda único",
        "SELECT count(*) - count(DISTINCT id_venda) FROM silver.vendas",
    ),
    # Receita tem que ser exatamente quantidade × preco_unitario.
    (
        "silver.vendas: receita = quantidade × preco_unitario",
        "SELECT count(*) FROM silver.vendas WHERE receita <> CAST(quantidade * preco_unitario AS DECIMAL(10,2))",
    ),
    # Produto não cadastrado é tolerado (é marcado), mas acima de 1% indica falha no cadastro.
    (
        "silver.vendas: produto não cadastrado abaixo de 1%",
        "SELECT CASE WHEN avg(CASE WHEN produto_cadastrado THEN 0 ELSE 1 END) >= 0.01 THEN 1 ELSE 0 END FROM silver.vendas",
    ),
    # gold.clientes_segmentacao (Customer Success)
    (
        "gold.clientes_segmentacao: receita total igual à de silver.vendas",
        """SELECT CASE WHEN (SELECT sum(receita) FROM gold.clientes_segmentacao)
                          = (SELECT sum(receita) FROM silver.vendas) THEN 0 ELSE 1 END""",
    ),
    (
        "gold.clientes_segmentacao: id_cliente único",
        "SELECT count(*) - count(DISTINCT id_cliente) FROM gold.clientes_segmentacao",
    ),
    (
        "gold.clientes_segmentacao: segmento só VIP, TOP_TIER ou REGULAR",
        """SELECT count(*) FROM gold.clientes_segmentacao
           WHERE segmento_cliente IS NULL OR segmento_cliente NOT IN ('VIP', 'TOP_TIER', 'REGULAR')""",
    ),
    (
        "gold.clientes_segmentacao: nenhum VIP com receita abaixo de R$ 22.000",
        "SELECT count(*) FROM gold.clientes_segmentacao WHERE segmento_cliente = 'VIP' AND receita < 22000",
    ),
    # golds da Diretoria Comercial: a receita tem que bater com a silver em todas.
    *[
        (
            f"gold.{tabela}: receita total igual à de silver.vendas",
            f"""SELECT CASE WHEN (SELECT sum(receita) FROM gold.{tabela})
                               = (SELECT sum(receita) FROM silver.vendas) THEN 0 ELSE 1 END""",
        )
        for tabela in ["vendas_temporais", "vendas_produtos", "vendas_detalhadas"]
    ],
    (
        "gold.vendas_detalhadas: mesmo número de linhas de silver.vendas",
        "SELECT abs((SELECT count(*) FROM gold.vendas_detalhadas) - (SELECT count(*) FROM silver.vendas))",
    ),
    (
        "gold.vendas_detalhadas: id_venda único",
        "SELECT count(*) - count(DISTINCT id_venda) FROM gold.vendas_detalhadas",
    ),
    (
        "gold.vendas_detalhadas: toda venda com segmento e região",
        "SELECT count(*) FROM gold.vendas_detalhadas WHERE segmento_cliente IS NULL OR regiao IS NULL",
    ),
    # gold.precos_competitividade (Pricing)
    (
        "gold.precos_competitividade: id_produto único",
        "SELECT count(*) - count(DISTINCT id_produto) FROM gold.precos_competitividade",
    ),
    # Marca divergente do nome é tolerada (é marcada), mas acima de 10% do catálogo indica cadastro quebrado.
    (
        "silver.produtos: marca divergente do nome abaixo de 10%",
        "SELECT CASE WHEN avg(CASE WHEN marca_divergente_do_nome THEN 1 ELSE 0 END) >= 0.10 THEN 1 ELSE 0 END FROM silver.produtos",
    ),
    # gold.qualidade_dados (placar de qualidade)
    (
        "gold.qualidade_dados: uma linha por regra",
        "SELECT count(*) - count(DISTINCT regra) FROM gold.qualidade_dados",
    ),
    (
        "gold.qualidade_dados: receita afetada igual à das vendas marcadas na silver",
        """SELECT CASE WHEN (SELECT receita_afetada FROM gold.qualidade_dados
                             WHERE regra = 'Venda de produto não cadastrado')
                          = (SELECT sum(receita) FROM silver.vendas WHERE NOT produto_cadastrado)
                        THEN 0 ELSE 1 END""",
    ),
    # Comentários: o Genie depende deles para escrever SQL certo. As tabelas __materialization*
    # são internas do pipeline e ficam de fora.
    (
        "gold: toda coluna com comentário",
        """SELECT count(*) FROM information_schema.columns
           WHERE table_schema = 'gold'
             AND table_name NOT LIKE '\\_\\_materialization%'
             AND (comment IS NULL OR trim(comment) = '')""",
    ),
]

# COMMAND ----------

resultados = [
    (nome, int(spark.sql(consulta).collect()[0][0] or 0)) for nome, consulta in TESTES
]
df = spark.createDataFrame(resultados, "teste STRING, problemas BIGINT").selectExpr(
    "teste", "problemas", "CASE WHEN problemas = 0 THEN 'OK' ELSE 'FALHOU' END AS status"
)
display(df)

# COMMAND ----------

falhas = [nome for nome, problemas in resultados if problemas > 0]
assert not falhas, f"{len(falhas)} teste(s) falharam: {falhas}"
print(f"Todos os {len(resultados)} testes passaram.")
