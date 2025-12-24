# API Documentation

Complete API documentation for the CSV Ingestion Pipeline API.

## Base URL

```
http://localhost:3000
```

For Docker deployments, use the host machine's IP or domain.

## Authentication

Currently, the API does not require authentication. For production deployments, implement authentication/authorization.

## Endpoints

### 1. Health Check

Check if the API service is running and healthy.

**Endpoint:** `GET /health`

**Request:**
```http
GET /health HTTP/1.1
Host: localhost:3000
```

**Response (200 OK):**
```json
{
  "status": "healthy",
  "service": "ingestion-api",
  "timestamp": "2024-01-01T00:00:00.000Z"
}
```

**Response Fields:**
- `status` (string): Service status ("healthy" or "unhealthy")
- `service` (string): Service name
- `timestamp` (string): ISO 8601 timestamp

---

### 2. Upload Single CSV File

Upload a single CSV file for ingestion and processing.

**Endpoint:** `POST /api/v1/upload`

**Content-Type:** `multipart/form-data`

**Request:**
```http
POST /api/v1/upload HTTP/1.1
Host: localhost:3000
Content-Type: multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW

------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="file"; filename="data.csv"
Content-Type: text/csv

id,name,email,created_at
1,User 1,user1@example.com,2024-01-01T00:00:00.000Z
...
------WebKitFormBoundary7MA4YWxkTrZu0gW--
```

**cURL Example:**
```bash
curl -X POST http://localhost:3000/api/v1/upload \
  -F "file=@/path/to/file.csv"
```

**Request Parameters:**
- `file` (file, required): CSV file to upload
  - File extension: `.csv`
  - Max size: 5000 MB (configurable via `MAX_FILE_SIZE_MB`)
  - MIME types: `text/csv`, `application/csv`, `text/plain`

---

### 2a. Upload Multiple CSV Files (Batch)

Upload up to 100 CSV files in a single request for batch processing.

**Endpoint:** `POST /api/v1/upload/batch`

**Content-Type:** `multipart/form-data`

**Request:**
```http
POST /api/v1/upload/batch HTTP/1.1
Host: localhost:3000
Content-Type: multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW

------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="files"; filename="file1.csv"
Content-Type: text/csv
...
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="files"; filename="file2.csv"
Content-Type: text/csv
...
------WebKitFormBoundary7MA4YWxkTrZu0gW--
```

**cURL Example:**
```bash
curl -X POST http://localhost:3000/api/v1/upload/batch \
  -F "files=@/path/to/file1.csv" \
  -F "files=@/path/to/file2.csv" \
  -F "files=@/path/to/file3.csv"
```

**Request Parameters:**
- `files` (file[], required): Array of CSV files to upload (max 100 files)
  - Each file: extension `.csv`, max size 5000 MB
  - MIME types: `text/csv`, `application/csv`, `text/plain`

**Success Response (201 Created):**
```json
{
  "success": true,
  "totalFiles": 3,
  "successful": 3,
  "failed": 0,
  "results": [
    {
      "success": true,
      "jobId": "550e8400-e29b-41d4-a716-446655440000",
      "fileId": "660e8400-e29b-41d4-a716-446655440001",
      "fileName": "file1.csv",
      "fileSize": 1048576,
      "status": "pending"
    },
    ...
  ],
  "message": "3 file(s) uploaded successfully. Processing will begin shortly.",
  "processingTimeMs": 1234
}
```

**Success Response (201 Created):**
```json
{
  "success": true,
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "fileId": "660e8400-e29b-41d4-a716-446655440001",
  "fileName": "data.csv",
  "fileSize": 1048576,
  "status": "pending",
  "message": "File uploaded successfully. Processing will begin shortly.",
  "processingTimeMs": 1234
}
```

**Response Fields:**
- `success` (boolean): Request success status
- `jobId` (string): Unique job identifier (UUID)
- `fileId` (string): Unique file identifier (UUID)
- `fileName` (string): Original filename
- `fileSize` (number): File size in bytes
- `status` (string): Job status ("pending")
- `message` (string): Human-readable message
- `processingTimeMs` (number): Processing time in milliseconds

**Note:** CSV format validation (headers, row count) is performed by the validation service, not the API. The API only performs basic validation (file size, file type).

**Error Response (400 Bad Request):**
```json
{
  "error": "Validation failed",
  "code": "MISSING_COLUMNS",
  "message": "Missing required columns: id, name",
  "details": {
    "filePath": "/path/to/file.csv",
    "headers": ["email", "created_at"],
    "requiredColumns": ["id", "name", "email", "created_at"],
    "missingColumns": ["id", "name"]
  },
  "processingTimeMs": 567
}
```

**Error Codes:**

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `NO_FILE` | 400 | No file provided in request |
| `FILE_TOO_LARGE` | 400 | File exceeds maximum size |
| `INVALID_FILE_TYPE` | 400 | File is not a CSV file |
| `EMPTY_FILE` | 400 | File is empty |
| `NO_HEADERS` | 400 | CSV file has no headers |
| `MISSING_COLUMNS` | 400 | Required columns are missing |
| `CSV_PARSE_ERROR` | 400 | CSV parsing failed |
| `INTERNAL_ERROR` | 500 | Unexpected server error |

**Error Response Fields:**
- `error` (string): Error type
- `code` (string): Error code (see table above)
- `message` (string): Human-readable error message
- `details` (object): Additional error details
- `processingTimeMs` (number): Processing time before error

**Validation Rules (API Level - Basic Validation Only):**

1. **File Size:**
   - File must not be empty
   - File size must not exceed `MAX_FILE_SIZE_MB` (default: 5000 MB)

2. **File Type:**
   - File extension must be `.csv`
   - MIME type should be `text/csv`, `application/csv`, or `text/plain`

**Note:** Heavy CSV format validation (headers, row structure, required columns) is performed by the **Validation Service**, not the API. This separation ensures:
- Fast API response times
- API only handles basic file validation
- Detailed CSV validation happens asynchronously in the validation service

**Processing Flow:**

1. File(s) uploaded via multipart/form-data
2. File(s) stored temporarily
3. **Basic validation** performed (API level):
   - File size check
   - File type check
4. If basic validation fails:
   - Error response returned immediately
   - File deleted
   - No Kafka event produced
5. If basic validation succeeds:
   - Kafka event produced to `file-ingestion` topic
   - Success response returned
   - File remains for validation service processing
6. **Validation Service** (asynchronous):
   - Consumes from `file-ingestion` topic
   - Performs CSV format validation (headers, rows)
   - Validates row data (id, name, email, created_at)
   - Produces validated chunks to `validated-chunks` topic
7. **DB Ingestion Service** (asynchronous):
   - Consumes validated chunks
   - Inserts valid rows to `csv_records`
   - Inserts invalid rows to `error_records`

---

### 3. Check Upload Status

Check the status of an upload job.

**Endpoint:** `GET /api/v1/upload/status/:jobId`

**Request:**
```http
GET /api/v1/upload/status/550e8400-e29b-41d4-a716-446655440000 HTTP/1.1
Host: localhost:3000
```

**cURL Example:**
```bash
curl http://localhost:3000/api/v1/upload/status/550e8400-e29b-41d4-a716-446655440000
```

**Path Parameters:**
- `jobId` (string, required): Job identifier returned from upload endpoint

**Response (200 OK):**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Job status tracking not implemented in this version"
}
```

**Note:** This is a placeholder endpoint. In production, this would query a job status database or cache to return actual job status (pending, processing, completed, failed).

---

## Rate Limiting

Currently, the API does not implement rate limiting. For production deployments, implement rate limiting to prevent abuse.

**Recommended Limits:**
- 100 requests per minute per IP
- 10 MB per request
- 1000 MB per hour per IP

## Error Handling

### Standard Error Format

All errors follow this format:

```json
{
  "error": "Error type",
  "code": "ERROR_CODE",
  "message": "Human-readable message",
  "details": {},
  "processingTimeMs": 123
}
```

### HTTP Status Codes

| Status Code | Description |
|-------------|-------------|
| 200 | Success |
| 201 | Created (file uploaded successfully) |
| 400 | Bad Request (validation error) |
| 404 | Not Found (route not found) |
| 500 | Internal Server Error |

## Examples

### Example 1: Successful Single File Upload

**Request:**
```bash
curl -X POST http://localhost:3000/api/v1/upload \
  -F "file=@sample.csv"
```

**Response:**
```json
{
  "success": true,
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "fileId": "660e8400-e29b-41d4-a716-446655440001",
  "fileName": "sample.csv",
  "fileSize": 123456,
  "status": "pending",
  "message": "File uploaded successfully. Processing will begin shortly.",
  "processingTimeMs": 1234
}
```

### Example 1a: Successful Batch Upload (100 files)

**Request:**
```bash
curl -X POST http://localhost:3000/api/v1/upload/batch \
  -F "files=@file1.csv" \
  -F "files=@file2.csv" \
  ... (up to 100 files)
```

**Response:**
```json
{
  "success": true,
  "totalFiles": 100,
  "successful": 100,
  "failed": 0,
  "results": [...],
  "message": "100 file(s) uploaded successfully. Processing will begin shortly.",
  "processingTimeMs": 5678
}
```

### Example 2: Validation Error

**Request:**
```bash
curl -X POST http://localhost:3000/api/v1/upload \
  -F "file=@invalid.csv"
```

**Response:**
```json
{
  "error": "Validation failed",
  "code": "MISSING_COLUMNS",
  "message": "Missing required columns: id, name",
  "details": {
    "filePath": "/path/to/invalid.csv",
    "headers": ["email", "created_at"],
    "requiredColumns": ["id", "name", "email", "created_at"],
    "missingColumns": ["id", "name"]
  },
  "processingTimeMs": 567
}
```

### Example 3: File Too Large

**Request:**
```bash
curl -X POST http://localhost:3000/api/v1/upload \
  -F "file=@huge-file.csv"
```

**Response:**
```json
{
  "error": "File too large",
  "code": "FILE_TOO_LARGE",
  "message": "File size exceeds maximum allowed size of 5000 MB",
  "maxSizeMB": 5000,
  "processingTimeMs": 123
}
```

## Testing

### Using cURL

```bash
# Health check
curl http://localhost:3000/health

# Upload file
curl -X POST http://localhost:3000/api/v1/upload \
  -F "file=@sample.csv"

# Check status
curl http://localhost:3000/api/v1/upload/status/{jobId}
```

### Using Postman

Import the Postman collection from `postman/CSV_Ingestion_API.postman_collection.json`.

See [postman/README.md](postman/README.md) for details.

### Using Scripts

See [scripts/README.md](scripts/README.md) for test scripts.

## Configuration

API behavior can be configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `API_PORT` | 3000 | API server port |
| `API_HOST` | 0.0.0.0 | API server host |
| `MAX_FILE_SIZE_MB` | 5000 | Maximum file size in MB |
| `REQUIRED_COLUMNS` | id,name,email,created_at | Required CSV columns |
| `LOG_LEVEL` | info | Logging level |

See `.env.example` for all configuration options.

## Monitoring

### Health Check

Use the `/health` endpoint for health checks in monitoring systems:

```bash
# Simple health check
curl http://localhost:3000/health

# With timeout
curl --max-time 5 http://localhost:3000/health
```

### Logs

API logs are output as structured JSON:

```json
{
  "timestamp": "2024-01-01T00:00:00.000Z",
  "level": "INFO",
  "service": "ingestion-api",
  "message": "File upload received",
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "fileId": "660e8400-e29b-41d4-a716-446655440001"
}
```

View logs:
```bash
docker-compose logs -f ingestion-api
```

## Best Practices

1. **File Naming:** Use descriptive filenames
2. **File Size:** Keep files under 5000 MB for optimal performance
3. **CSV Format:** Ensure CSV follows standard format with headers
4. **Error Handling:** Always check response status and error codes
5. **Retry Logic:** Implement retry logic for transient failures
6. **Monitoring:** Monitor API health and performance metrics

## Support

For issues or questions:
1. Check service logs: `docker-compose logs ingestion-api`
2. Review error responses for specific error codes
3. Verify configuration in `.env` file
4. Check file format and validation requirements

