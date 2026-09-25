-- gold.vendas_detalhadas: uma linha por venda, com produto e cliente já juntos.
--
-- Por que estas regras:
-- - As outras golds são agregadas e não respondem perguntas que cruzam diretorias ("receita por
--   região e categoria", "canal preferido dos VIPs"), nem alimentam os filtros cruzados do dashboard.
--   Esta tabela mantém o grão da venda com todas as dimensões lado a lado.
-- - Entram TODAS as vendas (mesmo número de linhas de silver.vendas). Produto não cadastrado recebe
--   os mesmos rótulos de gold.vendas_produtos ("Produto não cadastrado" / "Não cadastrado").
-- - O segmento do cliente vem de gold.clientes_segmentacao, para usar exatamente a mesma regra de
--   segmentação da Diretoria de Customer Success.
-- - CLUSTER BY (data): a maioria das perguntas e dos filtros do dashboard é por período.

CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_detalhadas (
  id_venda STRING COMMENT 'Identificador único da venda. Chave da tabela: uma linha por venda.',
  data_venda TIMESTAMP COMMENT 'Data e hora da venda.',
  data DATE COMMENT 'Data da venda (sem horário).',
  dia_semana STRING COMMENT 'Dia da semana da venda, em português: Domingo, Segunda, Terça, Quarta, Quinta, Sexta ou Sábado.',
  dia_semana_num INT COMMENT 'Número do dia da semana: 1 = Domingo, 2 = Segunda ... 7 = Sábado. Use para ordenar os dias.',
  hora INT COMMENT 'Hora da venda, de 0 a 23.',
  canal_venda STRING COMMENT 'Canal da venda: ecommerce (loja online) ou loja_fisica.',
  id_produto STRING COMMENT 'Identificador do produto vendido. Use para contar produtos (há nomes repetidos).',
  nome_produto STRING COMMENT 'Nome do produto, ou "Produto não cadastrado" quando o id_produto não existe no cadastro. Produtos diferentes podem ter o mesmo nome: conte por id_produto.',
  categoria STRING COMMENT 'Categoria do produto, ou "Não cadastrado".',
  marca STRING COMMENT 'Marca do produto, ou "Não cadastrado".',
  faixa_preco STRING COMMENT 'Faixa pelo preço atual do produto: PREMIUM (acima de R$ 1.000), MEDIO (acima de R$ 500), BASICO (até R$ 500) ou "Não cadastrado".',
  id_cliente STRING COMMENT 'Identificador do cliente que comprou. Para contar clientes use COUNT(DISTINCT id_cliente).',
  nome_cliente STRING COMMENT 'Nome do cliente sem pronome de tratamento.',
  estado STRING COMMENT 'Sigla da UF do cliente (ex.: MG, SP).',
  regiao STRING COMMENT 'Região do Brasil do cliente: Norte, Nordeste, Centro-Oeste, Sudeste ou Sul.',
  segmento_cliente STRING COMMENT 'Segmento do cliente, vindo de gold.clientes_segmentacao: VIP (receita a partir de R$ 22.000 no período), TOP_TIER (de R$ 17.000 até R$ 21.999,99) ou REGULAR (abaixo de R$ 17.000).',
  quantidade BIGINT COMMENT 'Unidades vendidas nesta venda.',
  preco_unitario DECIMAL(10,2) COMMENT 'Preço unitário cobrado nesta venda, em R$.',
  receita DECIMAL(10,2) COMMENT 'Receita da venda em R$: quantidade × preço unitário. Some esta coluna para obter a receita total.',
  produto_cadastrado BOOLEAN COMMENT 'false quando o produto vendido não existe no cadastro. A venda conta na receita mesmo assim.',
  venda_antes_do_cadastro BOOLEAN COMMENT 'true quando a venda aconteceu antes da data de criação do produto no cadastro (problema de qualidade conhecido). A venda conta na receita mesmo assim.'
)
CLUSTER BY (data)
COMMENT 'Uma linha por venda, com dados de tempo, canal, produto e cliente (estado, região e segmento). Use para perguntas que cruzam dimensões, como receita por região e categoria ou canal preferido dos VIPs, e para os filtros do dashboard. Período dos dados: 13/12/2025 a 11/01/2026. Valores em R$. Para totais simples por tempo ou produto, prefira gold.vendas_temporais e gold.vendas_produtos.'
AS
SELECT
  v.id_venda,
  v.data_venda,
  v.data,
  v.dia_semana,
  CAST(v.dia_semana_num AS INT) AS dia_semana_num,
  CAST(v.hora AS INT) AS hora,
  v.canal_venda,
  v.id_produto,
  COALESCE(p.nome_produto, 'Produto não cadastrado') AS nome_produto,
  COALESCE(p.categoria, 'Não cadastrado') AS categoria,
  COALESCE(p.marca, 'Não cadastrado') AS marca,
  COALESCE(p.faixa_preco, 'Não cadastrado') AS faixa_preco,
  v.id_cliente,
  c.nome_cliente,
  c.estado,
  c.regiao,
  c.segmento_cliente,
  v.quantidade,
  v.preco_unitario,
  v.receita,
  v.produto_cadastrado,
  v.venda_antes_do_cadastro
FROM silver.vendas v
LEFT JOIN silver.produtos p ON p.id_produto = v.id_produto
LEFT JOIN gold.clientes_segmentacao c ON c.id_cliente = v.id_cliente
