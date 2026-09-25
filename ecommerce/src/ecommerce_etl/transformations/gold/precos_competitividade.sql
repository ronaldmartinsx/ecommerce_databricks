-- gold.precos_competitividade: Diretoria de Pricing. Estamos mais caros que a concorrência
-- (Mercado Livre, Amazon, Magalu e Shopee)? Em quais produtos agir?
--
-- Por que estas regras:
-- - Uma linha por produto que tem preço de concorrente: JOIN de silver.produtos com a agregação de
--   silver.preco_competidores. Produto sem preço de concorrente não tem o que comparar.
-- - receita e itens_vendidos vêm de silver.vendas com LEFT JOIN, porque um produto pode nunca ter
--   vendido (fica 0). Assim o Pricing prioriza pelo impacto: produto caro que vende muito primeiro.
-- - As diferenças estão em pontos percentuais: 10 = nosso preço 10% acima da referência;
--   -10 = 10% abaixo.
-- - A classificação segue esta ordem: primeiro os extremos (MAIS_CARO_QUE_TODOS, acima do maior
--   preço; MAIS_BARATO_QUE_TODOS, abaixo do menor), depois a comparação com a média (ACIMA_DA_MEDIA,
--   ABAIXO_DA_MEDIA ou NA_MEDIA).
-- - O produto com preço suspeito (algum concorrente abaixo de 60% do nosso preço, marcado na silver)
--   CONTINUA em todas as contas: promoção relâmpago existe, e descartar o preço esconderia um
--   concorrente agressivo. A coluna possui_preco_suspeito só alerta que o preço precisa ser
--   confirmado antes de reagir (por exemplo, antes de baixar o nosso preço).

CREATE OR REFRESH MATERIALIZED VIEW gold.precos_competitividade (
  id_produto STRING COMMENT 'Identificador único do produto. Chave da tabela: uma linha por produto com preço de concorrente. Use para contar produtos (há nomes repetidos).',
  nome_produto STRING COMMENT 'Nome do produto. Produtos diferentes podem ter o mesmo nome: conte por id_produto.',
  categoria STRING COMMENT 'Categoria do produto.',
  marca STRING COMMENT 'Marca do produto.',
  nosso_preco DECIMAL(10,2) COMMENT 'Nosso preço atual do produto, em R$.',
  preco_medio_concorrentes DECIMAL(10,2) COMMENT 'Média dos preços dos concorrentes para o produto, em R$, arredondada em 2 casas. Inclui preços suspeitos.',
  preco_minimo_concorrentes DECIMAL(10,2) COMMENT 'Menor preço entre os concorrentes para o produto, em R$. Pode ser um preço suspeito (ver possui_preco_suspeito).',
  preco_maximo_concorrentes DECIMAL(10,2) COMMENT 'Maior preço entre os concorrentes para o produto, em R$.',
  total_concorrentes BIGINT COMMENT 'Quantidade de concorrentes com preço coletado para o produto (entre Mercado Livre, Amazon, Magalu e Shopee).',
  diferenca_pct_vs_media DECIMAL(10,2) COMMENT 'Diferença do nosso preço em relação à média dos concorrentes, em pontos percentuais: (nosso_preco - preco_medio_concorrentes) / preco_medio_concorrentes × 100, arredondada em 2 casas. 10 = estamos 10% mais caros; -10 = 10% mais baratos.',
  diferenca_pct_vs_minimo DECIMAL(10,2) COMMENT 'Diferença do nosso preço em relação ao menor preço dos concorrentes, em pontos percentuais: (nosso_preco - preco_minimo_concorrentes) / preco_minimo_concorrentes × 100, arredondada em 2 casas. 10 = estamos 10% mais caros que o mais barato.',
  classificacao_preco STRING COMMENT 'Posição do nosso preço, avaliada nesta ordem: MAIS_CARO_QUE_TODOS (acima do maior preço dos concorrentes), MAIS_BARATO_QUE_TODOS (abaixo do menor), ACIMA_DA_MEDIA, ABAIXO_DA_MEDIA ou NA_MEDIA (igual à média).',
  possui_preco_suspeito BOOLEAN COMMENT 'true quando algum concorrente tem preço abaixo de 60% do nosso (possível erro de coleta ou promoção relâmpago). O preço continua em todas as contas; o alerta só indica que ele precisa ser confirmado antes de reagir.',
  receita DECIMAL(10,2) COMMENT 'Receita do produto no período (13/12/2025 a 11/01/2026), em R$: soma de quantidade × preço unitário das vendas. 0 se nunca vendeu.',
  itens_vendidos BIGINT COMMENT 'Unidades vendidas do produto no período (13/12/2025 a 11/01/2026). 0 se nunca vendeu.'
)
COMMENT 'Uma linha por produto com preço de concorrente (Mercado Livre, Amazon, Magalu e Shopee): nosso preço, preços médio, mínimo e máximo dos concorrentes, diferenças em pontos percentuais, classificação do preço, alerta de preço suspeito e receita do produto. Use para perguntas sobre competitividade de preço e em quais produtos agir. Vendas do período de 13/12/2025 a 11/01/2026. Valores em R$. Preços suspeitos continuam nas contas: confira possui_preco_suspeito antes de reagir.'
AS
WITH concorrentes AS (
  SELECT
    id_produto,
    CAST(ROUND(AVG(preco_concorrente), 2) AS DECIMAL(10,2)) AS preco_medio_concorrentes,
    MIN(preco_concorrente) AS preco_minimo_concorrentes,
    MAX(preco_concorrente) AS preco_maximo_concorrentes,
    COUNT(DISTINCT nome_concorrente) AS total_concorrentes,
    BOOL_OR(preco_suspeito) AS possui_preco_suspeito
  FROM silver.preco_competidores
  GROUP BY id_produto
),
vendas AS (
  SELECT id_produto, SUM(receita) AS receita, SUM(quantidade) AS itens_vendidos
  FROM silver.vendas
  GROUP BY id_produto
)
SELECT
  p.id_produto,
  p.nome_produto,
  p.categoria,
  p.marca,
  p.preco_atual AS nosso_preco,
  c.preco_medio_concorrentes,
  c.preco_minimo_concorrentes,
  c.preco_maximo_concorrentes,
  c.total_concorrentes,
  CAST(ROUND((p.preco_atual - c.preco_medio_concorrentes) / c.preco_medio_concorrentes * 100, 2) AS DECIMAL(10,2)) AS diferenca_pct_vs_media,
  CAST(ROUND((p.preco_atual - c.preco_minimo_concorrentes) / c.preco_minimo_concorrentes * 100, 2) AS DECIMAL(10,2)) AS diferenca_pct_vs_minimo,
  CASE
    WHEN p.preco_atual > c.preco_maximo_concorrentes THEN 'MAIS_CARO_QUE_TODOS'
    WHEN p.preco_atual < c.preco_minimo_concorrentes THEN 'MAIS_BARATO_QUE_TODOS'
    WHEN p.preco_atual > c.preco_medio_concorrentes THEN 'ACIMA_DA_MEDIA'
    WHEN p.preco_atual < c.preco_medio_concorrentes THEN 'ABAIXO_DA_MEDIA'
    ELSE 'NA_MEDIA'
  END AS classificacao_preco,
  c.possui_preco_suspeito,
  CAST(COALESCE(v.receita, 0) AS DECIMAL(10,2)) AS receita,
  CAST(COALESCE(v.itens_vendidos, 0) AS BIGINT) AS itens_vendidos
FROM silver.produtos p
JOIN concorrentes c ON c.id_produto = p.id_produto
LEFT JOIN vendas v ON v.id_produto = p.id_produto
