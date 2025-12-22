#!/bin/bash

# Upload Test Script
# 
# Tests the CSV ingestion API by uploading a sample CSV file.
# 
# Usage:
#   ./scripts/upload-test.sh [file_path]
#
# Example:
#   ./scripts/upload-test.sh scripts/sample-1000-rows.csv

API_URL="${API_URL:-http://localhost:3000/api/v1/upload}"
FILE_PATH="${1:-scripts/sample-1000-rows.csv}"

if [ ! -f "$FILE_PATH" ]; then
    echo "Error: File not found: $FILE_PATH"
    echo "Usage: $0 [file_path]"
    exit 1
fi

echo "Uploading CSV file to ingestion API..."
echo "File: $FILE_PATH"
echo "API: $API_URL"
echo ""

# Upload file
response=$(curl -s -w "\n%{http_code}" -X POST "$API_URL" \
    -F "file=@$FILE_PATH" \
    -H "Content-Type: multipart/form-data")

# Extract HTTP status code (last line)
http_code=$(echo "$response" | tail -n1)

# Extract response body (all but last line)
body=$(echo "$response" | sed '$d')

echo "HTTP Status: $http_code"
echo ""
echo "Response:"
echo "$body" | jq '.' 2>/dev/null || echo "$body"

if [ "$http_code" -eq 201 ]; then
    echo ""
    echo "✅ Upload successful!"
    job_id=$(echo "$body" | jq -r '.jobId' 2>/dev/null)
    if [ -n "$job_id" ] && [ "$job_id" != "null" ]; then
        echo "Job ID: $job_id"
    fi
else
    echo ""
    echo "❌ Upload failed!"
    exit 1
fi

