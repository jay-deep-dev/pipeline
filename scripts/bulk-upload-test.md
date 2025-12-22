# Bulk Upload Test Script

Bash script to test the CSV ingestion API by uploading multiple CSV files. Useful for testing the pipeline with 100 files as specified in requirements.

## Usage

```bash
./scripts/bulk-upload-test.sh [file_count] [rows_per_file]
```

## Parameters

- `file_count` (optional): Number of files to upload (default: 10)
- `rows_per_file` (optional): Number of rows per file (default: 1000)

## Examples

```bash
# Upload 10 files with 1000 rows each
./scripts/bulk-upload-test.sh 10 1000

# Upload 100 files with 10,000 rows each (as per requirements)
./scripts/bulk-upload-test.sh 100 10000

# Upload 50 files with 5,000 rows each
./scripts/bulk-upload-test.sh 50 5000

# Test with small files (quick test)
./scripts/bulk-upload-test.sh 5 100
```

## Environment Variables

- `API_URL`: API endpoint URL (default: `http://localhost:3000/api/v1/upload`)

## Workflow

1. Creates a temporary directory (`./temp-test-files`) for test files
2. Generates CSV files with specified row count
3. Uploads each file sequentially
4. Cleans up generated files after upload
5. Displays summary statistics

## Output

The script displays:
- Progress for each file (generation and upload)
- Success/failure status for each file
- Summary statistics:
  - Total files processed
  - Successful uploads
  - Failed uploads
  - Total time elapsed
  - Average time per file

## Example Output

```
Bulk Upload Test
================
Files to upload: 10
Rows per file: 1000
API URL: http://localhost:3000/api/v1/upload

Generating and uploading files...

[1/10] Generating test-file-1.csv... ✓ Uploading... ✓ Success
[2/10] Generating test-file-2.csv... ✓ Uploading... ✓ Success
[3/10] Generating test-file-3.csv... ✓ Uploading... ✓ Success
...
[10/10] Generating test-file-10.csv... ✓ Uploading... ✓ Success

==================================================
Summary
==================================================
Total files: 10
Successful: 10
Failed: 0
Time elapsed: 45s
Average time per file: 4500.00ms
```

## Requirements

- `curl` installed
- `bc` installed (for calculations, usually pre-installed)
- API service running
- Write permissions for temporary directory

## Performance Notes

- Files are generated and uploaded sequentially
- For 100 files, expect several minutes of processing time
- Each file goes through full validation cycle
- Monitor API logs for processing status: `docker-compose logs -f ingestion-api`

## Troubleshooting

### Permission Denied

```bash
chmod +x scripts/bulk-upload-test.sh
```

### bc not found

- Install bc:
  - macOS: `brew install bc`
  - Linux: `apt-get install bc` or `yum install bc`
- Or remove average time calculation from script (script will still work)

### Upload Failures

- Check API service logs: `docker-compose logs ingestion-api`
- Verify file validation requirements
- Check network connectivity
- Ensure sufficient disk space for temporary files

### Slow Performance

- Large file counts take time (this is expected)
- Monitor system resources (CPU, memory, disk)
- Consider testing with smaller file counts first

## Use Cases

### Testing Requirements (100 files)

```bash
# Test with 100 files, 10,000 rows each
./scripts/bulk-upload-test.sh 100 10000
```

### Quick Smoke Test

```bash
# Quick test with 5 files, 100 rows each
./scripts/bulk-upload-test.sh 5 100
```

### Load Testing

```bash
# Test with many files
./scripts/bulk-upload-test.sh 50 5000
```

## Notes

- Temporary files are automatically cleaned up
- Script exits with code 1 if any uploads fail
- Progress is shown in real-time
- Each file is validated before upload

