# silver.clientes: cadastro de clientes limpo, uma linha por id_cliente.
#
# Por que estas regras:
# - Remover duplicatas por id_cliente evita contar o mesmo cliente duas vezes (e duplicar vendas
#   nos joins).
# - Alguns nomes chegam com pronome de tratamento ("Sr.", "Sra.", "Srta.", "Dr.", "Dra."). Isso
#   atrapalha buscas por nome e deixa relatórios inconsistentes, então nome_cliente fica sem o
#   pronome e em formato título. O nome original é preservado em nome_original para auditoria.
# - estado (UF) vai para maiúsculas para casar com a sigla do IBGE.
# - nome_estado e regiao vêm de bronze.estados_ibge, gravada pela ingestão a partir da API de
#   localidades do IBGE (27 UFs). Usar a fonte oficial evita manter uma lista de UFs no código.
# - Cliente sem id ou sem região (UF desconhecida) quebraria as análises regionais: falha o pipeline.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

PRONOMES_TRATAMENTO = r"^\s*(Sr|Sra|Srta|Dr|Dra)\.\s*"

# O formato título (initcap) deixaria "Henrique Da Conceição"; em português as preposições dos
# sobrenomes ficam em minúsculas ("Henrique da Conceição").
PREPOSICOES = ["da", "de", "do", "das", "dos", "e"]


def _nome_titulo(coluna):
    nome = F.initcap(F.trim(F.regexp_replace(coluna, PRONOMES_TRATAMENTO, "")))
    for p in PREPOSICOES:
        nome = F.regexp_replace(nome, rf" {p.capitalize()} ", f" {p} ")
    return nome


@dp.materialized_view(
    name="silver.clientes",
    comment="Clientes sem duplicatas, nome sem pronome de tratamento, com nome do estado e região (IBGE).",
)
@dp.expect_all_or_fail(
    {
        "id_cliente_preenchido": "id_cliente IS NOT NULL",
        "regiao_preenchida": "regiao IS NOT NULL",
    }
)
def clientes():
    estados = spark.read.table("bronze.estados_ibge").select(
        F.col("sigla").alias("estado"),
        F.col("nome").alias("nome_estado"),
        F.col("regiao_nome").alias("regiao"),
    )
    return (
        spark.read.table("bronze.clientes")
        .dropDuplicates(["id_cliente"])
        .select(
            "id_cliente",
            _nome_titulo("nome_cliente").alias("nome_cliente"),
            F.col("nome_cliente").alias("nome_original"),
            F.upper(F.trim("estado")).alias("estado"),
            "pais",
            "data_cadastro",
        )
        .join(estados, "estado", "left")
        .select(
            "id_cliente",
            "nome_cliente",
            "nome_original",
            "estado",
            "nome_estado",
            "regiao",
            "pais",
            "data_cadastro",
        )
    )
