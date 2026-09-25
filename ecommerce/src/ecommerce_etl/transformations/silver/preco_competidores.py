# silver.preco_competidores: preço de cada concorrente por produto, uma linha por
# id_produto + nome_concorrente.
#
# Por que estas regras:
# - Remover duplicatas pela chave evita que um concorrente pese mais que os outros nas médias.
# - Dinheiro é DECIMAL(10,2), igual ao nosso preco_atual, para comparar sem erro de arredondamento.
# - data_coleta chega como texto; como timestamp dá para filtrar e ordenar por data.
# - Um preço de concorrente abaixo de 60% do nosso é marcado como preco_suspeito: pode ser erro de
#   coleta ou uma promoção relâmpago. A linha NÃO é descartada (a decisão fica para a gold de
#   Pricing); a expectation preco_plausivel só mede quantas existem.
# - Linha sem produto ou com preço <= 0 não tem uso nenhum e indica ingestão quebrada: falha o pipeline.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LIMITE_PRECO_SUSPEITO = 0.6


@dp.materialized_view(
    name="silver.preco_competidores",
    comment="Preços de concorrentes sem duplicatas, com data_coleta em timestamp e marcação de preço suspeito.",
)
@dp.expect_all_or_fail(
    {
        "id_produto_preenchido": "id_produto IS NOT NULL",
        "preco_concorrente_positivo": "preco_concorrente > 0",
    }
)
@dp.expect("preco_plausivel", "NOT preco_suspeito")
def preco_competidores():
    produtos = spark.read.table("silver.produtos").select("id_produto", "preco_atual")
    preco = F.col("preco_concorrente").cast("decimal(10,2)")
    return (
        spark.read.table("bronze.preco_competidores")
        .dropDuplicates(["id_produto", "nome_concorrente"])
        .join(produtos, "id_produto", "left")
        .select(
            "id_produto",
            "nome_concorrente",
            preco.alias("preco_concorrente"),
            F.to_timestamp("data_coleta").alias("data_coleta"),
            F.coalesce(preco < F.col("preco_atual") * LIMITE_PRECO_SUSPEITO, F.lit(False)).alias(
                "preco_suspeito"
            ),
        )
    )
