#!/usr/bin/env bash
# Gera o clean_transactions.txt a partir do CSV de entrada usando o job MapReduce em modo LOCAL.
# Uso:
#   ./run.sh in/operacoes_comerciais_inteira.csv
#
# Saída:
#   out/clean_transactions.txt  (consolidado)
#   out/clean_out/part-m-*      (arquivos de partição)

set -euo pipefail

# --------- Config checagens ---------
if ! command -v javac >/dev/null; then
  echo "ERRO: 'javac' não encontrado. Instale o JDK 8 (openjdk-8-jdk)." >&2
  exit 1
fi

: "${HADOOP_HOME:?ERRO: HADOOP_HOME não está setado. Ex: export HADOOP_HOME=\$HOME/apps/hadoop-3.3.6}"

# --------- Args ---------
if [[ $# -lt 1 ]]; then
  echo "Uso: $0 <csv_de_entrada>" >&2
  exit 2
fi

INPUT_CSV="$1"
if [[ ! -f "$INPUT_CSV" ]]; then
  echo "ERRO: arquivo de entrada não existe: $INPUT_CSV" >&2
  exit 3
fi

# --------- Paths do projeto ---------
SRC_JAVA="src/basic/PreprocessTransactions.java"
BUILD_DIR="build/classes"
JAR_OUT="preprocess.jar"
OUT_DIR_LOCAL="out/clean_out"
OUT_FILE="out/clean_transactions.txt"

mkdir -p "$BUILD_DIR" out

# --------- Compila ---------
echo ">> Compilando..."
HCP="$("$HADOOP_HOME/bin/hadoop" classpath)"
javac -source 1.8 -target 1.8 -cp "$HCP" -d "$BUILD_DIR" "$SRC_JAVA"

# --------- Empacota ---------
echo ">> Empacotando JAR..."
jar cf "$JAR_OUT" -C "$BUILD_DIR" .

# --------- Executa em modo LOCAL ---------
echo ">> Rodando MapReduce (modo local)..."
rm -rf "$OUT_DIR_LOCAL"
"$JAVA_HOME/bin/java" \
  -Dmapreduce.framework.name=local \
  -Dfs.defaultFS=file:/// \
  -cp "$JAR_OUT:$HCP" \
  basic.PreprocessTransactions \
  "file://$PWD/$INPUT_CSV" "file://$PWD/$OUT_DIR_LOCAL"

# --------- Consolida saída ---------
echo ">> Consolidando saída..."
cat "$OUT_DIR_LOCAL"/part-m-* > "$OUT_FILE"

# --------- Checagens rápidas ---------
echo ">> Verificando integridade..."
LINES=$(wc -l < "$OUT_FILE")
BAD_COLS=$(awk -F';' 'NF!=10{c++} END{print c+0}' "$OUT_FILE")
echo "Linhas no arquivo final: $LINES"
if [[ "$BAD_COLS" -ne 0 ]]; then
  echo "ATENÇÃO: Encontradas $BAD_COLS linhas com colunas != 10." >&2
  exit 4
fi

echo
echo "✅ Pronto! Arquivo final: $OUT_FILE"
echo "   Exemplo de visualização:"
echo "   head -n 5 $OUT_FILE | column -s ';' -t"
