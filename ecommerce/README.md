# Bundle `ecommerce`

Databricks Asset Bundle com todos os recursos do projeto: ingestão, pipeline silver/gold, testes, Job, 3 dashboards AI/BI e o Genie space. Visão geral, arquitetura e passo a passo completo no [README da raiz](../README.md).

## Conteúdo

```
ecommerce/
├── databricks.yml                     ← bundle: variáveis e targets dev / prod
├── resources/                         ← recursos declarados em YAML
│   ├── pipeline_ecommerce.job.yml     ← Job: ingestão → pipeline → testes (diário, 6h)
│   ├── ecommerce_etl.pipeline.yml     ← Lakeflow pipeline serverless
│   ├── diretoria_*.dashboard.yml      ← 3 dashboards
│   └── diretoria.genie_space.yml      ← Genie space (serialized space dentro do YAML)
├── src/
│   ├── ingestao/ingestao_bronze.py    ← Supabase (S3) + API do IBGE → bronze
│   ├── ecommerce_etl/
│   │   ├── transformations/silver/    ← 4 materialized views em PySpark
│   │   ├── transformations/gold/      ← 6 materialized views em SQL
│   │   └── testes/testes_qualidade.py ← 22 testes entre tabelas
│   └── dashboards/                    ← JSON dos 3 dashboards
├── AGENTS.md                          ← convenções e números de referência
└── CLAUDE.md                          ← importa o AGENTS.md
```

## Comandos

Sempre com o perfil da CLI (`-p`), a partir desta pasta:

```bash
databricks bundle validate --strict -t dev -p <perfil>
databricks bundle deploy -t dev -p <perfil>
databricks bundle run pipeline_ecommerce -t dev -p <perfil>
databricks bundle summary -t dev -p <perfil>      # links do Job, dashboards e Genie
```

| Target | Catálogo | Comportamento |
|---|---|---|
| `dev` (padrão) | `ecommerce` | Recursos com prefixo `[dev <usuario>]`, agendamento pausado |
| `prod` | `ecommerce_prod` | Nomes limpos, Job todo dia às 6h (`America/Sao_Paulo`) |

**Primeiro deploy num ambiente novo:** `deploy` → `run` → `deploy`. O Genie só aceita tabelas que já existem, e as golds nascem quando o Job roda.
