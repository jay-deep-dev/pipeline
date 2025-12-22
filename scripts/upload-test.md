# Upload Test Script

Bash script to test the CSV ingestion API by uploading a single file.

## Usage

```bash
./scripts/upload-test.sh [file_path]
```

## Parameters

- `file_path` (optional): Path to CSV file to upload (default: `scripts/sample-1000-rows.csv`)

## Examples

```bash
# Upload default sample file
./scripts/upload-test.sh

# Upload a specific file
./scripts/upload-test.sh /path/to/my-file.csv

# Upload a generated file
node scripts/generate-sample-csv.js 5000 test.csv
./scripts/upload-test.sh test.csv
```

## Environment Variables

- `API_URL`: API endpoint URL (default: `http://localhost:3000/api/v1/upload`)

### Example with Custom API URL

```bash
# Set custom API URL
export API_URL=http://localhost:3000/api/v1/upload
./scripts/upload-test.sh my-file.csv
```

## Output

The script displays:
- HTTP status code
- JSON response (formatted with `jq` if available)
- Success/failure indicator
- Job ID (if successful)

## Example Output

```
Uploading CSV file to ingestion API...
File: sample.csv
API: http://localhost:3000/api/v1/upload

HTTP Status: 201

Response:
{
  "success": true,
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "fileId": "660e8400-e29b-41d4-a716-446655440001",
  "fileName": "sample.csv",
  "fileSize": 123456,
  "headers": ["id", "name", "email", "created_at"],
  "estimatedRowCount": 1000,
  "status": "pending",
  "message": "File uploaded and validated successfully...",
  "processingTimeMs": 1234
}

✅ Upload successful!
Job ID: 550e8400-e29b-41d4-a716-446655440000
```

## Requirements

- `curl` installed
- `jq` installed (optional, for JSON formatting)
- API service running

## Troubleshooting

### Permission Denied

```bash
chmod +x scripts/upload-test.sh
```

### Connection Refused

- Ensure API service is running: `docker-compose up -d ingestion-api`
- Check API URL is correct
- Verify port 3000 is accessible

### jq not found

- Install jq: 
  - macOS: `brew install jq`
  - Linux: `apt-get install jq` or `yum install jq`
- Or view raw JSON output without formatting (script will still work)

### Upload Fails

- Check file size (max 5000 MB by default)
- Verify file is a valid CSV
- Ensure required columns are present: `id`, `name`, `email`, `created_at`
- Review error response for specific validation failure

