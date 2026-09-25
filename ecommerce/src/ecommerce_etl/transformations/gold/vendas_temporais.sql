-- gold.vendas_temporais: Diretoria Comercial, visão de QUANDO e ONDE (canal) vendemos.
--
-- Por que estas regras:
-- - Grão data × hora × canal_venda: responde "quanto vendemos por dia, hora, dia da semana e canal"
--   sem ler venda a venda. Os níveis mais altos (dia, semana, canal) saem de SUM sobre esta tabela.
-- - Entram TODAS as vendas, inclusive de produto não cadastrado: o dinheiro entrou, então é receita.
--   Assim a receita total bate com silver.vendas.
-- - clientes_unicos é um COUNT DISTINCT dentro da linha e NÃO pode ser somado entre linhas: o mesmo
--   cliente compra em dias e horas diferentes. Para clientes únicos no período, use
--   gold.clientes_segmentacao.

CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_temporais (
  data DATE COMMENT 'Data da venda (sem horário).',
  dia_semana STRING COMMENT 'Dia da semana da venda, em português: Domingo, Segunda, Terça, Quarta, Quinta, Sexta ou Sábado.',
  dia_semana_num INT COMMENT 'Número do dia da semana: 1 = Domingo, 2 = Segunda ... 7 = Sábado. Use para ordenar os dias.',
  hora INT COMMENT 'Hora da venda, de 0 a 23.',
  canal_venda STRING COMMENT 'Canal da venda: ecommerce (loja online) ou loja_fisica.',
  total_vendas BIGINT COMMENT 'Quantidade de vendas (pedidos) na data, hora e canal. Pode ser somada entre linhas.',
  itens_vendidos BIGINT COMMENT 'Soma das unidades vendidas (quantidade) na data, hora e canal. Pode ser somada entre linhas.',
  receita DECIMAL(10,2) COMMENT 'Receita em R$ na data, hora e canal: soma de quantidade × preço unitário, inclusive de produtos não cadastrados. Pode ser somada entre linhas.',
  clientes_unicos BIGINT COMMENT 'Clientes distintos que compraram nesta data, hora e canal. ATENÇÃO: NÃO somar entre linhas (o mesmo cliente aparece em várias linhas). Para clientes únicos no período, use gold.clientes_segmentacao.'
)
COMMENT 'Vendas agregadas por data, hora e canal de venda (ecommerce ou loja_fisica). Use para perguntas sobre quanto vendemos e quando: por dia, hora, dia da semana ou canal. Período dos dados: 13/12/2025 a 11/01/2026. Valores em R$. Não some clientes_unicos entre linhas.'
AS
SELECT
  data,
  dia_semana,
  CAST(dia_semana_num AS INT) AS dia_semana_num,
  CAST(hora AS INT) AS hora,
  canal_venda,
  COUNT(*) AS total_vendas,
  CAST(SUM(quantidade) AS BIGINT) AS itens_vendidos,
  CAST(SUM(receita) AS DECIMAL(10,2)) AS receita,
  COUNT(DISTINCT id_cliente) AS clientes_unicos
FROM silver.vendas
GROUP BY data, dia_semana, dia_semana_num, hora, canal_venda
