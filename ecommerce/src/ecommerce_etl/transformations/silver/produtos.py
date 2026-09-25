# silver.produtos: catálogo de produtos limpo, uma linha por id_produto.
#
# Por que estas regras:
# - A bronze é sobrescrita pela ingestão e pode trazer o mesmo produto mais de uma vez;
#   duplicata aqui multiplicaria as vendas em qualquer join, então removemos por id_produto.
# - nome_produto recebe trim porque espaços sobrando fazem o mesmo produto parecer outro em
#   filtros e agrupamentos.
# - Dinheiro é DECIMAL(10,2): double acumula erro de arredondamento nas somas de receita.
# - faixa_preco agrupa o catálogo para análises (PREMIUM > 1000, MEDIO > 500, BASICO o resto).
# - Alguns produtos citam no nome uma marca do catálogo diferente da cadastrada ("Tênis Nike
#   Revolution" com marca Adidas). Isso é erro de cadastro, mas não muda receita: a linha é MARCADA
#   em marca_divergente_do_nome e medida com a expectation marca_consistente_com_nome (warn).
#   A comparação é por palavra inteira, para "LG" não casar com "algodão".
# - Produto sem id ou com preço <= 0 não é um problema de qualidade tolerável: quebraria as
#   comparações de preço e os joins. Nesse caso o pipeline falha (expect_all_or_fail).

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver.produtos",
    comment="Produtos sem duplicatas, com preço em DECIMAL(10,2), faixa de preço e marcação de marca divergente do nome.",
)
@dp.expect_all_or_fail(
    {
        "id_produto_preenchido": "id_produto IS NOT NULL",
        "preco_atual_positivo": "preco_atual > 0",
    }
)
@dp.expect("marca_consistente_com_nome", "NOT marca_divergente_do_nome")
def produtos():
    base = (
        spark.read.table("bronze.produtos")
        .dropDuplicates(["id_produto"])
        .withColumn("nome_produto", F.trim("nome_produto"))
    )
    outras_marcas = base.select(F.col("marca").alias("outra_marca")).distinct()
    divergentes = (
        base.join(
            F.broadcast(outras_marcas),
            (F.col("outra_marca") != F.col("marca"))
            & F.expr("lower(nome_produto) RLIKE concat('\\\\b', lower(outra_marca), '\\\\b')"),
        )
        .select("id_produto")
        .distinct()
        .withColumn("marca_divergente_do_nome", F.lit(True))
    )
    preco = F.col("preco_atual").cast("decimal(10,2)")
    return base.join(divergentes, "id_produto", "left").select(
        "id_produto",
        "nome_produto",
        "categoria",
        "marca",
        preco.alias("preco_atual"),
        F.when(preco > 1000, "PREMIUM")
        .when(preco > 500, "MEDIO")
        .otherwise("BASICO")
        .alias("faixa_preco"),
        "data_criacao",
        F.coalesce(F.col("marca_divergente_do_nome"), F.lit(False)).alias("marca_divergente_do_nome"),
    )
