# Postman Collection for CSV Ingestion API

This directory contains a Postman collection for testing the CSV Ingestion API.

## Importing the Collection

1. Open Postman
2. Click **Import** button (top left)
3. Select **File** tab
4. Choose `CSV_Ingestion_API.postman_collection.json`
5. Click **Import**

## Collection Variables

The collection uses the following variables:

- `base_url`: Base URL for the API (default: `http://localhost:3000`)
- `last_job_id`: Automatically set after successful upload (for status check)

You can modify these in Postman:
- Click on the collection name
- Go to **Variables** tab
- Modify values as needed

## Endpoints

### 1. Health Check
- **Method:** GET
- **URL:** `{{base_url}}/health`
- **Description:** Check if the API service is running
- **Expected Response:** 200 OK with service status

### 2. Upload CSV File
- **Method:** POST
- **URL:** `{{base_url}}/api/v1/upload`
- **Body Type:** form-data
- **Field:** `file` (file type)
- **Description:** Upload a CSV file for processing

**Success Response (201):**
```json
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
```

**Error Response (400):**
```json
{
  "error": "Validation failed",
  "code": "MISSING_COLUMNS",
  "message": "Missing required columns: id, name",
  "details": { ... }
}
```

### 3. Check Upload Status
- **Method:** GET
- **URL:** `{{base_url}}/api/v1/upload/status/:jobId`
- **Description:** Check status of an upload job (placeholder endpoint)

## Usage Examples

### Example 1: Upload a CSV File

1. Select **Upload CSV File** request
2. Go to **Body** tab
3. Click **Select Files** next to the `file` field
4. Choose your CSV file
5. Click **Send**

### Example 2: Check Health

1. Select **Health Check** request
2. Click **Send**
3. Verify response shows `"status": "healthy"`

### Example 3: Check Upload Status

1. After a successful upload, the `last_job_id` variable is automatically set
2. Select **Check Upload Status** request
3. The `:jobId` parameter will use `last_job_id` if available
4. Click **Send**

## Error Codes Reference

| Code | Description |
|------|-------------|
| `NO_FILE` | No file provided in request |
| `FILE_TOO_LARGE` | File exceeds maximum size (default: 5000 MB) |
| `INVALID_FILE_TYPE` | File is not a CSV file |
| `EMPTY_FILE` | File is empty |
| `NO_HEADERS` | CSV file has no headers |
| `MISSING_COLUMNS` | Required columns are missing |
| `CSV_PARSE_ERROR` | CSV parsing failed |
| `INTERNAL_ERROR` | Unexpected server error |

## Tips

1. **Generate Test CSV:** Use the `scripts/generate-sample-csv.js` script to create test files
2. **Environment Variables:** Create a Postman environment for different environments (dev, staging, prod)
3. **Pre-request Scripts:** The collection includes scripts to set default values
4. **Test Scripts:** The collection includes test scripts that log job IDs automatically

## Troubleshooting

### Connection Refused
- Ensure the API service is running: `docker-compose up -d ingestion-api`
- Check the port: Default is 3000
- Verify `base_url` variable is correct

### File Upload Fails
- Check file size (max 5000 MB by default)
- Verify file is a valid CSV
- Ensure required columns are present: `id`, `name`, `email`, `created_at`

### Validation Errors
- Review error response for specific validation failure
- Check CSV format (headers, row structure)
- Verify file encoding (UTF-8 recommended)

