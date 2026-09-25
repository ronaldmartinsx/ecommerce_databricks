# silver.vendas: uma linha por venda (id_venda), com receita e atributos de tempo.
#
# Por que estas regras:
# - Remover duplicatas por id_venda: venda repetida inflaria a receita.
# - receita = quantidade × preco_unitario em DECIMAL(10,2), para a soma bater centavo a centavo.
# - data, hora, dia_semana_num (1 = domingo ... 7 = sábado, igual ao dayofweek do Spark) e
#   dia_semana em português ficam prontos para as análises temporais, sem cada gold recalcular.
# - Existem vendas de produtos que não estão em silver.produtos e vendas registradas antes da
#   criação do produto. As duas são MARCADAS (produto_cadastrado, venda_antes_do_cadastro) e medidas
#   com expectations de warn, mas NUNCA descartadas: o dinheiro entrou, e apagar a venda mudaria a
#   receita.
# - Campos obrigatórios vazios, quantidade ou preço <= 0 e canal desconhecido indicam ingestão
#   quebrada. Isso nunca pode acontecer, então o pipeline falha.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

DIAS_SEMANA = ["Domingo", "Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado"]


@dp.materialized_view(
    name="silver.vendas",
    comment="Vendas sem duplicatas, com receita em DECIMAL(10,2), atributos de tempo e marcações de qualidade.",
)
@dp.expect_all_or_fail(
    {
        "id_venda_preenchido": "id_venda IS NOT NULL",
        "data_venda_preenchida": "data_venda IS NOT NULL",
        "id_cliente_preenchido": "id_cliente IS NOT NULL",
        "id_produto_preenchido": "id_produto IS NOT NULL",
        "quantidade_preenchida": "quantidade IS NOT NULL",
        "preco_unitario_preenchido": "preco_unitario IS NOT NULL",
        "quantidade_positiva": "quantidade > 0",
        "preco_unitario_positivo": "preco_unitario > 0",
        "canal_venda_valido": "canal_venda IN ('ecommerce', 'loja_fisica')",
    }
)
@dp.expect_all(
    {
        "produto_cadastrado": "produto_cadastrado",
        "venda_depois_do_cadastro": "NOT venda_antes_do_cadastro",
    }
)
def vendas():
    produtos = spark.read.table("silver.produtos").select(
        F.col("id_produto").alias("id_produto_cadastro"), "data_criacao"
    )
    preco = F.col("preco_unitario").cast("decimal(10,2)")
    dia_semana_num = F.dayofweek("data_venda")
    return (
        spark.read.table("bronze.vendas")
        .dropDuplicates(["id_venda"])
        .join(produtos, F.col("id_produto") == F.col("id_produto_cadastro"), "left")
        .select(
            "id_venda",
            "data_venda",
            F.to_date("data_venda").alias("data"),
            F.hour("data_venda").alias("hora"),
            dia_semana_num.alias("dia_semana_num"),
            F.element_at(F.array(*[F.lit(d) for d in DIAS_SEMANA]), dia_semana_num).alias(
                "dia_semana"
            ),
            "id_cliente",
            "id_produto",
            "canal_venda",
            "quantidade",
            preco.alias("preco_unitario"),
            (F.col("quantidade") * preco).cast("decimal(10,2)").alias("receita"),
            F.col("id_produto_cadastro").isNotNull().alias("produto_cadastrado"),
            F.coalesce(F.col("data_venda") < F.col("data_criacao"), F.lit(False)).alias(
                "venda_antes_do_cadastro"
            ),
        )
    )
