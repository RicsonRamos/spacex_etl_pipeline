# scripts\generate_docs.sh
#!/bin/bash
set -e

echo "=== Gerando documentação dbt ==="

# Detectar diretório automaticamente
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DBT_PATH="$PROJECT_ROOT/dbt_spacex"

# Ou usar variável de ambiente se disponível
DBT_PATH="${DBT_PROJECT_PATH:-$DBT_PATH}"

if [ ! -d "$DBT_PATH" ]; then
    echo "ERRO: Diretório dbt_spacex não encontrado em: $DBT_PATH"
    exit 1
fi

echo "Path: $DBT_PATH"
cd "$DBT_PATH"

# Verificar dbt
if ! command -v dbt &> /dev/null; then
    echo "ERRO: dbt não encontrado no PATH"
    exit 1
fi

echo "DBT versão: $(dbt --version | grep 'installed version' | head -1)"

# Gerar docs
dbt docs generate

echo ""
echo "=== Documentação gerada com sucesso ==="
echo "Local: $DBT_PATH/target/"
echo "Acesse: http://localhost:8081"

# Opcional: copiar para volume compartilhado se existir
if [ -d "/docs-output" ]; then
    cp -r "$DBT_PATH/target/"* /docs-output/ 2>/dev/null || true
    echo "Copiado para: /docs-output/"
fi