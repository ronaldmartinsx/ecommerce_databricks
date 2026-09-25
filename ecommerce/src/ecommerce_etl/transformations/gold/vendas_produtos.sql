-- gold.vendas_produtos: Diretoria Comercial, visão de O QUE vendemos.
--
-- Por que estas regras:
-- - Uma linha por produto vendido: LEFT JOIN de silver.vendas com silver.produtos. O LEFT JOIN é de
--   propósito: vendas de produto não cadastrado continuam aqui (o dinheiro entrou), com nome
--   "Produto não cadastrado" e categoria, marca e faixa "Não cadastrado", para a receita bater com
--   silver.vendas e o problema ficar visível no dashboard.
-- - Produtos diferentes podem ter o mesmo nome; por isso a chave é id_produto, e qualquer contagem
--   de produtos deve usar id_produto, nunca nome_produto.
-- - Os rankings usam ROW_NUMBER (sem empates), com id_produto como desempate para o resultado ser
--   sempre o mesmo entre execuções.

CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_produtos (
  id_produto STRING COMMENT 'Identificador único do produto. Chave da tabela: uma linha por produto vendido. Use para contar produtos.',
  nome_produto STRING COMMENT 'Nome do produto, ou "Produto não cadastrado" quando o id_produto não existe no cadastro. ATENÇÃO: produtos diferentes têm o mesmo nome; conte produtos por id_produto, nunca por nome_produto.',
  categoria STRING COMMENT 'Categoria do produto, ou "Não cadastrado" quando o produto não existe no cadastro.',
  marca STRING COMMENT 'Marca do produto, ou "Não cadastrado" quando o produto não existe no cadastro.',
  faixa_preco STRING COMMENT 'Faixa pelo preço atual do produto: PREMIUM (acima de R$ 1.000), MEDIO (acima de R$ 500), BASICO (até R$ 500) ou "Não cadastrado".',
  produto_cadastrado BOOLEAN COMMENT 'true quando o produto existe no cadastro de produtos; false para vendas de produto não cadastrado (a receita é contada mesmo assim).',
  total_vendas BIGINT COMMENT 'Quantidade de vendas (pedidos) do produto no período.',
  itens_vendidos BIGINT COMMENT 'Soma das unidades vendidas (quantidade) do produto no período.',
  receita DECIMAL(10,2) COMMENT 'Receita do produto no período, em R$: soma de quantidade × preço unitário.',
  ticket_medio DECIMAL(10,2) COMMENT 'Receita média por venda do produto, em R$ (média da receita das vendas, arredondada em 2 casas).',
  ranking_receita INT COMMENT 'Posição do produto pela receita entre todos os produtos, da maior para a menor (1 = maior receita). Sem empates.',
  ranking_na_categoria INT COMMENT 'Posição do produto pela receita dentro da sua categoria (1 = maior receita da categoria). Sem empates.'
)
COMMENT 'Uma linha por produto vendido, com receita, unidades, ticket médio e rankings geral e por categoria, inclusive vendas de produtos não cadastrados. Use para perguntas sobre quais produtos, categorias, marcas e faixas de preço mais vendem. Período dos dados: 13/12/2025 a 11/01/2026. Valores em R$. Conte produtos por id_produto (há nomes repetidos).'
AS
WITH por_produto AS (
  SELECT
    v.id_produto,
    COALESCE(p.nome_produto, 'Produto não cadastrado') AS nome_produto,
    COALESCE(p.categoria, 'Não cadastrado') AS categoria,
    COALESCE(p.marca, 'Não cadastrado') AS marca,
    COALESCE(p.faixa_preco, 'Não cadastrado') AS faixa_preco,
    p.id_produto IS NOT NULL AS produto_cadastrado,
    COUNT(*) AS total_vendas,
    CAST(SUM(v.quantidade) AS BIGINT) AS itens_vendidos,
    CAST(SUM(v.receita) AS DECIMAL(10,2)) AS receita,
    CAST(ROUND(AVG(v.receita), 2) AS DECIMAL(10,2)) AS ticket_medio
  FROM silver.vendas v
  LEFT JOIN silver.produtos p ON p.id_produto = v.id_produto
  GROUP BY ALL
)
SELECT
  *,
  CAST(ROW_NUMBER() OVER (ORDER BY receita DESC, id_produto) AS INT) AS ranking_receita,
  CAST(ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY receita DESC, id_produto) AS INT) AS ranking_na_categoria
FROM por_produto
