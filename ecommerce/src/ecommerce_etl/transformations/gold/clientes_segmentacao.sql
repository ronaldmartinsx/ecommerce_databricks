-- gold.clientes_segmentacao: Diretoria de Customer Success.
--
-- Por que estas regras:
-- - Uma linha por cliente, INCLUSIVE quem nunca comprou (LEFT JOIN a partir de silver.clientes, com
--   receita 0). É justamente esse cliente que o time de CS precisa ativar; um INNER JOIN o esconderia.
-- - Entram TODAS as vendas, inclusive as de produto não cadastrado: o dinheiro entrou, então é receita
--   do cliente. Assim a receita total desta tabela bate com silver.vendas.
-- - Segmentação definida com a diretora a partir da distribuição real da receita no período
--   (13/12/2025 a 11/01/2026):
--     VIP      receita >= R$ 22.000,00
--     TOP_TIER receita >= R$ 17.000,00 (até R$ 21.999,99)
--     REGULAR  receita <  R$ 17.000,00 (inclui quem nunca comprou)
--   Os limites antigos (R$ 10.000 para VIP e R$ 5.000 para TOP_TIER) não serviam: com eles quase
--   todos os clientes viravam VIP, e um segmento que contém todo mundo não ajuda a priorizar a carteira.
-- - ranking_receita usa ROW_NUMBER (sem empates), com id_cliente como desempate para o resultado ser
--   sempre o mesmo entre execuções.

CREATE OR REFRESH MATERIALIZED VIEW gold.clientes_segmentacao (
  id_cliente STRING COMMENT 'Identificador único do cliente. Chave da tabela: uma linha por cliente.',
  nome_cliente STRING COMMENT 'Nome do cliente sem pronome de tratamento (Sr., Sra., Srta., Dr., Dra.), em formato título.',
  estado STRING COMMENT 'Sigla da UF do cliente, em maiúsculas (ex.: MG, SP).',
  nome_estado STRING COMMENT 'Nome do estado do cliente por extenso (ex.: Minas Gerais), conforme o IBGE.',
  regiao STRING COMMENT 'Região do Brasil do cliente, conforme o IBGE: Norte, Nordeste, Centro-Oeste, Sudeste ou Sul.',
  total_compras BIGINT COMMENT 'Quantidade de vendas (pedidos) do cliente no período. 0 para quem nunca comprou.',
  receita DECIMAL(10,2) COMMENT 'Receita total do cliente no período, em R$: soma de quantidade × preço unitário de todas as vendas, inclusive de produtos não cadastrados. 0 para quem nunca comprou.',
  ticket_medio DECIMAL(10,2) COMMENT 'Receita média por venda do cliente, em R$ (média da receita das vendas, arredondada em 2 casas). Nulo para quem nunca comprou.',
  primeira_compra TIMESTAMP COMMENT 'Data e hora da primeira venda do cliente no período. Nulo para quem nunca comprou.',
  ultima_compra TIMESTAMP COMMENT 'Data e hora da venda mais recente do cliente no período. Nulo para quem nunca comprou.',
  segmento_cliente STRING COMMENT 'Segmento pela receita no período: VIP (a partir de R$ 22.000), TOP_TIER (de R$ 17.000 até R$ 21.999,99) ou REGULAR (abaixo de R$ 17.000, inclusive quem nunca comprou).',
  ranking_receita INT COMMENT 'Posição do cliente pela receita, da maior para a menor (1 = maior receita). Sem empates.'
)
COMMENT 'Um cliente por linha, inclusive quem nunca comprou, com receita, ticket médio, datas de compra, segmento (VIP, TOP_TIER, REGULAR) e ranking. Use para perguntas sobre melhores clientes, carteira por segmento, estado ou região e clientes a ativar. Período dos dados: 13/12/2025 a 11/01/2026. Valores em R$.'
AS
WITH compras AS (
  SELECT
    id_cliente,
    COUNT(*) AS total_compras,
    SUM(receita) AS receita,
    ROUND(AVG(receita), 2) AS ticket_medio,
    MIN(data_venda) AS primeira_compra,
    MAX(data_venda) AS ultima_compra
  FROM silver.vendas
  GROUP BY id_cliente
),
base AS (
  SELECT
    c.id_cliente,
    c.nome_cliente,
    c.estado,
    c.nome_estado,
    c.regiao,
    COALESCE(v.total_compras, 0) AS total_compras,
    CAST(COALESCE(v.receita, 0) AS DECIMAL(10,2)) AS receita,
    CAST(v.ticket_medio AS DECIMAL(10,2)) AS ticket_medio,
    v.primeira_compra,
    v.ultima_compra
  FROM silver.clientes c
  LEFT JOIN compras v ON v.id_cliente = c.id_cliente
)
SELECT
  *,
  CASE
    WHEN receita >= 22000 THEN 'VIP'
    WHEN receita >= 17000 THEN 'TOP_TIER'
    ELSE 'REGULAR'
  END AS segmento_cliente,
  CAST(ROW_NUMBER() OVER (ORDER BY receita DESC, id_cliente) AS INT) AS ranking_receita
FROM base
