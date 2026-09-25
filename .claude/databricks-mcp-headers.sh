#!/bin/sh
# Emite o header de autenticação do MCP gerenciado de SQL do Databricks para o Claude Code.
# O token vem da Databricks CLI (OAuth, renovado automaticamente): nenhum token fica em arquivo.
# Perfil da CLI: variável DATABRICKS_CONFIG_PROFILE, ou ecommerce_profile se ela não existir.
PROFILE="${DATABRICKS_CONFIG_PROFILE:-ecommerce_profile}"
TOKEN=$(databricks auth token -p "$PROFILE" -o json | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
printf '{"Authorization": "Bearer %s"}\n' "$TOKEN"
