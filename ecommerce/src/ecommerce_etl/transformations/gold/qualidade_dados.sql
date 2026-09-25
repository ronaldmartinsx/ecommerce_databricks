-- gold.qualidade_dados: placar de qualidade dos dados, para todas as diretorias.
--
-- Por que estas regras:
-- - As expectations da silver medem os problemas a cada execução, mas essas métricas ficam no event
--   log do pipeline, que o diretor não abre. Esta tabela leva o mesmo placar para o dashboard e para o
--   Genie: "quanto da receita tem problema de cadastro?", "posso confiar no preço do concorrente?".
-- - Uma linha por regra. Cada regra conta as linhas marcadas na silver; nenhuma linha é descartada
--   em lugar nenhum, então os números aqui explicam, sem alterar, a receita das outras golds.
-- - Severidade:
--     ALERTA       problema real que alguém precisa resolver na origem
--     INFORMATIVO  característica do dado que muda a leitura dos números
--     CORRIGIDO    a silver já trata; a linha registra quantas vezes aconteceu
-- - receita_afetada só existe para regras sobre vendas; nas demais fica nula (não zero), porque a
--   regra não envolve receita.

CREATE OR REFRESH MATERIALIZED VIEW gold.qualidade_dados (
  regra STRING COMMENT 'Descrição da regra de qualidade verificada. Chave da tabela: uma linha por regra.',
  tabela STRING COMMENT 'Tabela silver onde a regra é verificada (ex.: silver.vendas).',
  severidade STRING COMMENT 'ALERTA (problema real a resolver na origem), INFORMATIVO (característica do dado que muda a leitura dos números) ou CORRIGIDO (a silver já trata; a linha registra quantas vezes aconteceu).',
  linhas_afetadas BIGINT COMMENT 'Quantidade de linhas da tabela silver que caem na regra. Para regras de produto, é a quantidade de produtos (id_produto).',
  receita_afetada DECIMAL(10,2) COMMENT 'Receita em R$ das vendas afetadas pela regra, no período de 13/12/2025 a 11/01/2026. Nula quando a regra não envolve vendas. Essa receita CONTINUA nas outras golds: nenhuma venda é descartada.'
)
COMMENT 'Placar de qualidade dos dados: uma linha por regra, com severidade, quantas linhas cada problema afeta e quanta receita está envolvida. Use para perguntas sobre confiabilidade dos números, vendas de produtos não cadastrados, preços suspeitos de concorrentes e erros de cadastro de produtos e clientes. Período dos dados: 13/12/2025 a 11/01/2026.'
AS
SELECT
  'Venda de produto não cadastrado' AS regra,
  'silver.vendas' AS tabela,
  'ALERTA' AS severidade,
  COUNT(*) AS linhas_afetadas,
  CAST(SUM(receita) AS DECIMAL(10,2)) AS receita_afetada
FROM silver.vendas
WHERE NOT produto_cadastrado

UNION ALL

SELECT 'Venda anterior à criação do produto', 'silver.vendas', 'ALERTA', COUNT(*), CAST(SUM(receita) AS DECIMAL(10,2))
FROM silver.vendas
WHERE venda_antes_do_cadastro

UNION ALL

SELECT 'Preço de concorrente abaixo de 60% do nosso', 'silver.preco_competidores', 'ALERTA', COUNT(*), CAST(NULL AS DECIMAL(10,2))
FROM silver.preco_competidores
WHERE preco_suspeito

UNION ALL

SELECT 'Marca do produto diferente da marca citada no nome', 'silver.produtos', 'ALERTA', COUNT(*), CAST(NULL AS DECIMAL(10,2))
FROM silver.produtos
WHERE marca_divergente_do_nome

UNION ALL

SELECT 'Produto com nome igual ao de outro produto', 'silver.produtos', 'INFORMATIVO', COUNT(*), CAST(NULL AS DECIMAL(10,2))
FROM (
  SELECT COUNT(*) OVER (PARTITION BY nome_produto) AS mesmo_nome
  FROM silver.produtos
)
WHERE mesmo_nome > 1

UNION ALL

SELECT 'Produto monitorado em menos de 4 concorrentes', 'silver.preco_competidores', 'INFORMATIVO', COUNT(*), CAST(NULL AS DECIMAL(10,2))
FROM (
  SELECT id_produto
  FROM silver.preco_competidores
  GROUP BY id_produto
  HAVING COUNT(DISTINCT nome_concorrente) < 4
)

UNION ALL

SELECT 'Nome de cliente com pronome de tratamento', 'silver.clientes', 'CORRIGIDO', COUNT(*), CAST(NULL AS DECIMAL(10,2))
FROM silver.clientes
WHERE nome_original RLIKE '^ *(Sr|Sra|Srta|Dr|Dra)[.]'
