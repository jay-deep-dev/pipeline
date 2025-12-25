# High-Volume CSV Ingestion Pipeline

A scalable, high-performance data ingestion pipeline designed to handle large-scale CSV file processing. The system supports uploading up to 100 CSV files, each containing up to 10 million rows, with comprehensive validation, Kafka-based event processing, and MongoDB batch insertion.

## 🏗️ Architecture Overview

The pipeline uses a **two-stage architecture** for optimal performance and scalability:

1. **Ingestion API Service** - Handles file uploads (up to 100 files), performs basic validation, and produces Kafka events
2. **Validation Service** - Consumes file events, validates CSV rows, chunks data, and produces validated chunks
3. **DB Ingestion Service** - Consumes validated chunks and inserts into MongoDB (valid rows → csv_records, invalid rows → error_records)
4. **DLQ Handler Service** - Processes failed jobs from Dead Letter Queues

### Architecture Diagram

```
┌─────────────┐
│   Client    │
│ (Upload 100 │
│   files)    │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│      Ingestion API Service          │
│  ┌───────────────────────────────┐  │
│  │ 1. File Upload (Multer)      │  │
│  │ 2. Basic Validation          │  │
│  │    - File Size               │  │
│  │    - File Type              │  │
│  │ 3. Kafka Producer           │  │
│  │    → file-ingestion topic   │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │
               ▼
        ┌──────────┐
        │  Kafka   │
        │ (Topic:  │
        │file-ingestion)│
        └────┬─────┘
             │
             ▼
┌─────────────────────────────────────┐
│     Validation Service              │
│  ┌───────────────────────────────┐  │
│  │ 1. Kafka Consumer            │  │
│  │ 2. CSV Streaming Parser      │  │
│  │ 3. Row Validation            │  │
│  │    (id, name, email, date)   │  │
│  │ 4. Chunking (5k rows)        │  │
│  │ 5. Kafka Producer            │  │
│  │    → validated-chunks topic  │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │
               ▼
        ┌──────────┐
        │  Kafka   │
        │ (Topic:  │
        │validated-chunks)│
        └────┬─────┘
             │
             ▼
┌─────────────────────────────────────┐
│    DB Ingestion Service             │
│  ┌───────────────────────────────┐  │
│  │ 1. Kafka Consumer            │  │
│  │ 2. Batch Insert              │  │
│  │    - Valid → csv_records     │  │
│  │    - Invalid → error_records │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │
               ▼
        ┌──────────┐
        │ MongoDB  │
        │ ┌──────┐ │
        │ │csv_  │ │
        │ │records││
        │ └──────┘ │
        │ ┌──────┐ │
        │ │error │ │
        │ │records││
        │ └──────┘ │
        └──────────┘
```

## 🚀 Features

### Core Features
- ✅ **File Upload API** - RESTful API for CSV file uploads (single or batch up to 100 files)
- ✅ **Two-Stage Pipeline** - Decoupled validation and persistence for optimal performance
- ✅ **Basic API Validation** - Fast file size and type validation at API level
- ✅ **Row-Level Validation** - Comprehensive validation in validation service (id, name, email, created_at)
- ✅ **Kafka Event Production** - Produces to `file-ingestion` topic after basic validation
- ✅ **Chunked Processing** - Files chunked into 5,000-row batches for efficient processing
- ✅ **Dual MongoDB Collections** - Valid rows → `csv_records`, Invalid rows → `error_records`
- ✅ **Dead Letter Queue (DLQ)** - Handles system failures gracefully

### Performance Optimizations
- **Streaming Processing** - Files are processed in streams, not loaded entirely into memory
- **Batch Inserts** - Configurable batch sizes (default: 10,000 rows) for optimal throughput
- **Horizontal Scaling** - Worker services can be scaled horizontally
- **Efficient Memory Usage** - Minimal memory footprint even for large files
- **Connection Pooling** - MongoDB connection pooling for better performance

### Fault Tolerance
- **Error Handling** - Comprehensive error handling at each stage
- **DLQ Support** - Failed jobs are sent to Dead Letter Queue
- **Idempotency** - Job tracking prevents duplicate processing
- **Graceful Shutdown** - Proper cleanup on service shutdown

## 📋 Prerequisites

- **Node.js** 20+ (or Docker)
- **Docker & Docker Compose** (recommended)
- **Kafka** (via Docker Compose)
- **MongoDB** (via Docker Compose)

## 🛠️ Installation

### Option 1: Docker Compose (Recommended)

1. Clone the repository:
```bash
cd "CSV Ingestion Pipeline"
```

2. Copy environment file:
```bash
cp .env.example .env
```

3. Start all services:
```bash
docker-compose up -d
```

This will start:
- Zookeeper
- Kafka
- MongoDB
- Ingestion API (port 3000)
- Worker Service (2 instances)
- DLQ Handler

### Option 2: Local Development

1. Install dependencies:
```bash
# Install root dependencies
npm install

# Install shared library dependencies
cd shared && npm install && cd ..

# Install ingestion-api dependencies
cd ingestion-api && npm install && cd ..

# Install worker dependencies
cd worker && npm install && cd ..
```

2. Start infrastructure services (Kafka, MongoDB):
```bash
docker-compose up -d zookeeper kafka mongodb
```

3. Configure environment:
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Start services:
```bash
# Terminal 1: Ingestion API
npm run start:api

# Terminal 2: Worker
npm run start:worker

# Terminal 3: DLQ Handler (optional)
npm run start:dlq-handler
```

## 📖 Usage

### Upload a Single CSV File

```bash
curl -X POST http://localhost:3000/api/v1/upload \
  -F "file=@/path/to/your/file.csv"
```

### Upload Multiple CSV Files (Batch - up to 100 files)

```bash
curl -X POST http://localhost:3000/api/v1/upload/batch \
  -F "files=@/path/to/file1.csv" \
  -F "files=@/path/to/file2.csv" \
  -F "files=@/path/to/file3.csv"
```

Or use the batch upload script:
```bash
node scripts/batch-upload.js 100 10000
```

### Example Response

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

### Check Service Health

```bash
curl http://localhost:3000/health
```

## ⚙️ Configuration

Key configuration options in `.env`:

```env
# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_INGESTION_TOPIC=csv.ingestion
KAFKA_DLQ_TOPIC=csv.ingestion.dlq

# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=csv_ingestion
MONGODB_COLLECTION=csv_records

# File Storage
UPLOAD_DIR=./uploads
MAX_FILE_SIZE_MB=5000

# Validation
REQUIRED_COLUMNS=id,name,email,created_at
SAMPLE_ROWS_FOR_VALIDATION=1000

# Worker Configuration
BATCH_SIZE=10000
MAX_CONCURRENT_FILES=5
```

## 📁 Project Structure

```
CSV Ingestion Pipeline/
├── shared/                      # Shared utilities and libraries
│   ├── src/
│   │   ├── config.js           # Configuration management
│   │   ├── logger.js            # Logging utility
│   │   ├── validation.js        # File validation logic
│   │   ├── row-validation.js   # Row-level validation
│   │   ├── kafka-schemas.js     # Kafka message schemas
│   │   ├── kafka-admin.js      # Kafka topic management
│   │   └── storage.js          # Storage abstraction
│   └── package.json
├── ingestion-api/               # Upload API service
│   ├── src/
│   │   ├── index.js            # Express app entry point
│   │   ├── kafka-producer.js   # Kafka producer
│   │   ├── controllers/        # Request handlers
│   │   │   └── upload-controller.js
│   │   ├── services/           # Business logic
│   │   │   └── upload-service.js
│   │   ├── middleware/         # Middleware
│   │   │   └── upload-handler.js
│   │   └── routes/             # Route definitions
│   │       └── upload.js
│   ├── Dockerfile
│   └── package.json
├── validation-service/          # Row validation service
│   ├── src/
│   │   ├── index.js
│   │   ├── kafka-consumer.js   # Consumes file-ingestion
│   │   └── csv-validator.js   # Validates & chunks
│   ├── Dockerfile
│   └── package.json
├── worker/                      # DB ingestion service
│   ├── src/
│   │   ├── index.js            # Service entry point
│   │   ├── kafka-consumer.js   # Consumes validated-chunks
│   │   ├── chunk-processor.js  # Processes chunks
│   │   ├── mongodb-client.js   # MongoDB client (dual collections)
│   │   └── dlq-handler.js      # DLQ handler service
│   ├── Dockerfile
│   └── package.json
├── scripts/                     # Test utilities
│   ├── batch-upload.js         # Batch upload script
│   ├── bulk-upload-test.js     # Bulk upload test
│   └── generate-sample-csv.js  # CSV generator
├── docker-compose.yml           # Docker Compose configuration
│   env.example                  # Environment configurations
│   env.local
│   env.docker
└── README.md                    # This file
```

## 🔍 Validation Details

### API-Level Validation (Basic - Fast)
Performed by **Ingestion API** before producing Kafka event:

1. **File Size Validation**
   - Checks if file is empty
   - Validates against maximum file size (default: 5000 MB)
   - Throws `ValidationError` with code `FILE_TOO_LARGE` if exceeded

2. **File Type Validation**
   - Validates file extension (.csv)
   - Validates MIME type (text/csv, application/csv)
   - Throws `ValidationError` with code `INVALID_FILE_TYPE` if invalid

### Validation Service (Row-Level - Heavy)
Performed by **Validation Service** after consuming from `file-ingestion` topic:

1. **CSV Format Validation**
   - Validates CSV headers exist
   - Checks for required columns: `id`, `name`, `email`, `created_at`
   - Validates row structure and parsing

2. **Row Data Validation**
   - `id`: Required, string or number
   - `name`: Required, non-empty string, max 255 characters
   - `email`: Required, valid email format (regex validated)
   - `created_at`: Required, valid ISO 8601 datetime

3. **Chunking**
   - Valid rows and invalid rows separated
   - Chunked into 5,000-row batches
   - Produced to `validated-chunks` topic

## 📊 Performance Considerations

### Batch Size Tuning
- Default batch size: 10,000 rows
- Adjust `BATCH_SIZE` in `.env` based on:
  - Row size (larger rows = smaller batches)
  - Available memory
  - MongoDB write performance

### Horizontal Scaling
- Worker services can be scaled horizontally
- Kafka consumer groups ensure load distribution
- Each worker processes different partitions

### Memory Optimization
- Streaming CSV parser prevents loading entire files
- Batches are cleared after insertion
- Connection pooling reduces overhead

## 🐛 Error Handling

### Validation Errors
- Returned immediately with HTTP 400 status
- File is deleted if validation fails
- No Kafka event is produced

### Processing Errors
- Logged with full context
- Job sent to DLQ for analysis
- Original file preserved for retry

### DLQ Messages
- Contain full error details
- Include original job message
- Track retry count
- Can be manually reviewed and retried

## 📝 API Documentation

### POST /api/v1/upload
Upload a single CSV file for processing.

**Request:**
- Method: `POST`
- Content-Type: `multipart/form-data`
- Body: `file` field containing CSV file

**Response (Success - 201):**
```json
{
  "success": true,
  "jobId": "uuid",
  "fileId": "uuid",
  "fileName": "file.csv",
  "fileSize": 1234567,
  "status": "pending",
  "message": "File uploaded successfully. Processing will begin shortly.",
  "processingTimeMs": 1234
}
```

### POST /api/v1/upload/batch
Upload multiple CSV files (up to 100) in a single request.

**Request:**
- Method: `POST`
- Content-Type: `multipart/form-data`
- Body: `files` field containing array of CSV files (max 100)

**Response (Success - 201):**
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

**Response (Error - 400):**
```json
{
  "error": "Validation failed",
  "code": "MISSING_COLUMNS",
  "message": "Missing required columns: id, name",
  "details": { ... }
}
```

### GET /health
Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "service": "ingestion-api",
  "timestamp": "2024-01-01T00:00:00.000Z"
}
```

## 🧪 Testing

See `scripts/` directory for test utilities:
- `generate-sample-csv.js` - Generate test CSV files
- `upload-test.sh` - Upload test script

## 🔒 Security Considerations

- File size limits prevent DoS attacks
- File type validation prevents malicious uploads
- Input sanitization in CSV parsing
- Connection security for Kafka and MongoDB (configure in production)

## 📈 Monitoring & Observability

- Structured JSON logging throughout
- Log levels: ERROR, WARN, INFO, DEBUG
- Processing metrics (rows/second, batch counts)
- Error tracking via DLQ

## 🚀 Production Deployment

### Recommendations:
1. Use environment-specific configuration
2. Enable Kafka SSL/TLS
3. Use MongoDB authentication
4. Set up monitoring and alerting
5. Configure log aggregation
6. Use cloud storage (S3) instead of local filesystem
7. Set up auto-scaling for workers
8. Implement rate limiting on API
9. Add API authentication/authorization

## 📄 License

MIT

## 👥 Contributing

This is an assessment project. For production use, consider:
- Adding unit and integration tests
- Implementing retry mechanisms
- Adding metrics and monitoring
- Implementing API authentication
- Adding data transformation capabilities
- Supporting multiple file formats

