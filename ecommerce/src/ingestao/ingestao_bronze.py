# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão: data lake e API do IBGE → bronze
# MAGIC
# MAGIC Primeira tarefa do Job "Pipeline E-commerce". Baixa os 4 arquivos Parquet do data lake (Storage do
# MAGIC Supabase, que fala o protocolo S3) e a lista de UFs da API do IBGE, e grava tudo na bronze **sem
# MAGIC alterar nada**: a bronze é a evidência do que a origem mandou. Limpeza e regras ficam na silver.
# MAGIC
# MAGIC | Origem | Tabela |
# MAGIC |---|---|
# MAGIC | `vendas.parquet`, `produtos.parquet`, `clientes.parquet`, `preco_competidores.parquet` | `<catalogo>.bronze.<tabela>` |
# MAGIC | API de localidades do IBGE | `<catalogo>.bronze.estados_ibge` |
# MAGIC
# MAGIC Por que assim:
# MAGIC - **Credenciais no secret scope**, nunca no código: endpoint e chaves são lidos com
# MAGIC   `dbutils.secrets.get`, e o Databricks mostra `[REDACTED]` se alguém imprimir o valor.
# MAGIC - **Carga full com `overwrite`**: os arquivos são pequenos e rodar duas vezes dá o mesmo resultado
# MAGIC   (idempotente). `overwriteSchema` aceita mudança de schema na origem sem apagar a tabela, o que
# MAGIC   preserva o histórico Delta (`DESCRIBE HISTORY`) para auditoria.
# MAGIC - **pandas no meio do caminho**: o `boto3` devolve bytes, e o pandas lê Parquet de bytes numa linha.
# MAGIC   Para arquivos grandes, o caminho seria Auto Loader lendo de um volume.
# MAGIC - O catálogo vem do parâmetro `catalogo` do Job: dev e prod gravam em catálogos diferentes.

# COMMAND ----------

# MAGIC %pip install boto3 -q

# COMMAND ----------

import io

import boto3
import pandas as pd
import requests

dbutils.widgets.text("catalogo", "ecommerce")
dbutils.widgets.text("secret_scope", "ecommerce")
dbutils.widgets.text("bucket", "datalake_ecommerce")
dbutils.widgets.text("s3_region", "ca-central-1")

catalogo = dbutils.widgets.get("catalogo")
scope = dbutils.widgets.get("secret_scope")
bucket = dbutils.widgets.get("bucket")

TABELAS = ["vendas", "produtos", "clientes", "preco_competidores"]
URL_IBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Estrutura
# MAGIC
# MAGIC `CREATE CATALOG` roda por SQL (na Free Edition ele não funciona pela API REST). As silver e gold são
# MAGIC preenchidas pelo pipeline, que precisa encontrar os schemas prontos.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalogo}`")
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalogo}`.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data lake → bronze

# COMMAND ----------

s3 = boto3.client(
    "s3",
    endpoint_url=dbutils.secrets.get(scope, "s3_endpoint"),
    region_name=dbutils.widgets.get("s3_region"),
    aws_access_key_id=dbutils.secrets.get(scope, "s3_access_key"),
    aws_secret_access_key=dbutils.secrets.get(scope, "s3_secret_key"),
)


def gravar_bronze(pdf, tabela):
    (
        spark.createDataFrame(pdf)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"`{catalogo}`.bronze.{tabela}")
    )


for tabela in TABELAS:
    objeto = s3.get_object(Bucket=bucket, Key=f"{tabela}.parquet")
    gravar_bronze(pd.read_parquet(io.BytesIO(objeto["Body"].read())), tabela)
    print(f"bronze.{tabela} gravada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. API do IBGE → bronze
# MAGIC
# MAGIC O cadastro de clientes só tem a UF; a região vem do IBGE. O JSON tem a região aninhada, e o
# MAGIC `json_normalize` transforma `regiao.nome` na coluna `regiao_nome`.

# COMMAND ----------

resposta = requests.get(URL_IBGE, timeout=60)
resposta.raise_for_status()
estados = pd.json_normalize(resposta.json())
estados.columns = [coluna.replace(".", "_") for coluna in estados.columns]
gravar_bronze(estados, "estados_ibge")
print(f"bronze.estados_ibge gravada ({len(estados)} UFs)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Conferência
# MAGIC
# MAGIC Uma bronze vazia faria o pipeline publicar golds vazias sem erro nenhum. Por isso a ingestão falha
# MAGIC aqui se alguma tabela chegar sem linhas.

# COMMAND ----------

contagens = {t: spark.table(f"`{catalogo}`.bronze.{t}").count() for t in TABELAS + ["estados_ibge"]}
display(spark.createDataFrame(list(contagens.items()), "tabela STRING, linhas BIGINT"))
vazias = [t for t, n in contagens.items() if n == 0]
assert not vazias, f"Tabelas bronze vazias: {vazias}"
