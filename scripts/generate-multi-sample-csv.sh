#!/usr/bin/env bash

# --------------------------------------------------
# Multi CSV Generator (Bash Optimized)
# --------------------------------------------------
# Usage:
#   ./generate-and-copy-csv.sh <rows_per_file> <file_count> [file_prefix]
#
# Example:
#   ./generate-and-copy-csv.sh 100000 100 users
# --------------------------------------------------

ROWS=${1:-1000}
COUNT=${2:-1}
PREFIX=${3:-sample}

BASE_FILE="${PREFIX}-base.csv"

echo "🚀 CSV generation started"
echo "• Rows per file : $ROWS"
echo "• File count   : $COUNT"
echo "• Prefix       : $PREFIX"
echo "----------------------------------"

# Generate base CSV only once
if [ ! -f "$BASE_FILE" ]; then
  echo "📄 Generating base CSV file..."

  echo "id,name,email,created_at" > "$BASE_FILE"

  for ((i=1; i<=ROWS; i++)); do
    echo "$i,User $i,user$i@example.com,$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$BASE_FILE"
  done
else
  echo "⚡ Base file already exists → $BASE_FILE"
fi

echo "📄 Copying files..."

for ((i=1; i<=COUNT; i++)); do
  cp "$BASE_FILE" "${PREFIX}-${i}.csv"
  echo "✅ ${PREFIX}-${i}.csv"
done

echo "----------------------------------"
echo "🎉 All CSV files ready"
