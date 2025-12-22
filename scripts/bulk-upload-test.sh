#!/bin/bash

# Bulk Upload Test Script
# 
# Tests the CSV ingestion API by uploading multiple CSV files.
# Useful for testing the pipeline with 100 files as specified in requirements.
# 
# Usage:
#   ./scripts/bulk-upload-test.sh [file_count] [rows_per_file]
# 
# Examples:
#   ./scripts/bulk-upload-test.sh 10 1000
#   ./scripts/bulk-upload-test.sh 100 10000

API_URL="${API_URL:-http://localhost:3000/api/v1/upload}"
file_count="${1:-10}"
rows_per_file="${2:-1000}"

echo "Bulk Upload Test"
echo "================"
echo "Files to upload: $file_count"
echo "Rows per file: $rows_per_file"
echo "API URL: $API_URL"
echo ""

# Create temp directory
temp_dir="./temp-test-files"
mkdir -p "$temp_dir"

# Generate CSV function
generate_csv() {
    local file_path="$1"
    local rows="$2"
    
    # Write headers
    echo "id,name,email,created_at" > "$file_path"
    
    # Generate rows
    for ((i=1; i<=rows; i++)); do
        echo "$i,User $i,user$i@example.com,$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")" >> "$file_path"
    done
}

success=0
failed=0
start_time=$(date +%s)

echo "Generating and uploading files..."
echo ""

for ((i=1; i<=file_count; i++)); do
    file_name="test-file-${i}.csv"
    file_path="$temp_dir/$file_name"
    
    # Generate CSV
    printf "[%d/%d] Generating %s... " "$i" "$file_count" "$file_name"
    generate_csv "$file_path" "$rows_per_file"
    printf "✓ "
    
    # Upload file
    printf "Uploading... "
    response=$(curl -s -w "\n%{http_code}" -X POST "$API_URL" \
        -F "file=@$file_path" \
        -H "Content-Type: multipart/form-data")
    
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | sed '$d')
    
    if [ "$http_code" -eq 201 ]; then
        success=$((success + 1))
        echo "✓ Success"
    else
        failed=$((failed + 1))
        echo "✗ Failed (HTTP $http_code)"
    fi
    
    # Clean up file
    rm -f "$file_path"
done

# Clean up temp directory
rmdir "$temp_dir" 2>/dev/null

# Calculate elapsed time
end_time=$(date +%s)
elapsed=$((end_time - start_time))

# Print summary
echo ""
echo "=================================================="
echo "Summary"
echo "=================================================="
echo "Total files: $file_count"
echo "Successful: $success"
echo "Failed: $failed"
echo "Time elapsed: ${elapsed}s"
if [ $file_count -gt 0 ]; then
    avg_time=$(echo "scale=2; $elapsed * 1000 / $file_count" | bc)
    echo "Average time per file: ${avg_time}ms"
fi

exit $([ $failed -gt 0 ] && echo 1 || echo 0)

