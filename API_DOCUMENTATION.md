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

### 2. Upload CSV File

Upload a CSV file for ingestion and processing.

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

**Success Response (201 Created):**
```json
{
  "success": true,
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "fileId": "660e8400-e29b-41d4-a716-446655440001",
  "fileName": "data.csv",
  "fileSize": 1048576,
  "headers": ["id", "name", "email", "created_at"],
  "estimatedRowCount": 10000,
  "status": "pending",
  "message": "File uploaded and validated successfully. Processing will begin shortly.",
  "processingTimeMs": 1234
}
```

**Response Fields:**
- `success` (boolean): Request success status
- `jobId` (string): Unique job identifier (UUID)
- `fileId` (string): Unique file identifier (UUID)
- `fileName` (string): Original filename
- `fileSize` (number): File size in bytes
- `headers` (array): CSV column headers
- `estimatedRowCount` (number): Estimated number of rows
- `status` (string): Job status ("pending")
- `message` (string): Human-readable message
- `processingTimeMs` (number): Validation processing time in milliseconds

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

**Validation Rules:**

1. **File Size:**
   - File must not be empty
   - File size must not exceed `MAX_FILE_SIZE_MB` (default: 5000 MB)

2. **File Type:**
   - File extension must be `.csv`
   - MIME type should be `text/csv`, `application/csv`, or `text/plain`

3. **CSV Format:**
   - File must have headers (first row)
   - Headers must include required columns (configurable via `REQUIRED_COLUMNS`)
   - Default required columns: `id`, `name`, `email`, `created_at`
   - CSV must be parseable (valid CSV format)

**Processing Flow:**

1. File is uploaded via multipart/form-data
2. File is stored temporarily
3. Validation is performed:
   - File size check
   - File type check
   - CSV format validation (headers, sample rows)
4. If validation fails:
   - Error response is returned
   - File is deleted
   - No Kafka event is produced
5. If validation succeeds:
   - Kafka event is produced with job metadata
   - Success response is returned
   - File remains for worker processing

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

### Example 1: Successful Upload

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
  "headers": ["id", "name", "email", "created_at"],
  "estimatedRowCount": 1000,
  "status": "pending",
  "message": "File uploaded and validated successfully. Processing will begin shortly.",
  "processingTimeMs": 1234
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

