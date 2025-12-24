# Architecture Documentation

## System Architecture

### Overview

The CSV Ingestion Pipeline is designed as a microservices architecture with clear separation of concerns. The system follows an event-driven pattern using Kafka for asynchronous processing, ensuring high throughput and scalability.

## Components

### 1. Ingestion API Service

**Purpose:** Handles file uploads (single or batch up to 100 files), performs basic validation, and initiates processing workflow.

**Responsibilities:**
- Accept file uploads via HTTP API (single or batch)
- Perform basic validation (file size, file type only)
- Store uploaded files
- Produce Kafka events to `file-ingestion` topic
- Return immediate feedback to clients

**Technology Stack:**
- Express.js for HTTP server
- Multer for file upload handling
- KafkaJS for Kafka producer

**Key Design Decisions:**
- **Controller/Route/Service Pattern:** Clean separation of concerns for maintainability
- **Basic Validation Only:** Fast file size and type checks. Heavy CSV validation moved to validation service.
- **Batch Upload Support:** Handles up to 100 files in a single request
- **Metadata-Only Events:** Only metadata sent to Kafka, not file contents
- **Immediate Error Response:** Basic validation errors returned immediately

### 2. Validation Service

**Purpose:** Consumes file ingestion events, validates CSV rows, and produces validated chunks.

**Responsibilities:**
- Consume messages from `file-ingestion` topic
- Stream CSV files (memory-efficient)
- Validate each row (id, name, email, created_at)
- Chunk rows into 5,000-row batches
- Separate valid and invalid rows
- Produce validated chunks to `validated-chunks` topic

**Technology Stack:**
- KafkaJS for Kafka consumer and producer
- csv-parser for streaming CSV parsing
- Row validation logic

**Key Design Decisions:**
- **Streaming Processing:** Files processed using Node.js streams, preventing memory exhaustion
- **Chunk-Based Processing:** 5,000 rows per chunk for optimal throughput
- **Row-Level Validation:** Validates data quality (email format, date format, etc.)
- **Separation of Concerns:** Validation decoupled from persistence

### 3. DB Ingestion Service

**Purpose:** Consumes validated chunks and inserts them into MongoDB.

**Responsibilities:**
- Consume validated chunk messages from `validated-chunks` topic
- Insert valid rows to `csv_records` collection
- Insert invalid rows to `error_records` collection
- Handle errors and send failed chunks to DLQ
- Retry logic with exponential backoff

**Technology Stack:**
- KafkaJS for Kafka consumer
- MongoDB Node.js driver for database operations

**Key Design Decisions:**
- **Dual Collection Strategy:** Valid and invalid rows stored separately
- **Batch Inserts:** Optimized bulk operations for high throughput
- **Retry Logic:** 3 retries with exponential backoff before DLQ
- **Idempotency:** Chunk tracking prevents duplicate processing

### 4. DLQ Handler Service

**Purpose:** Processes failed jobs from Dead Letter Queues and stores them in MongoDB.

**Responsibilities:**
- Consume messages from DLQ topics (`file-ingestion-dlq`, `validated-chunks-dlq`)
- Store failed jobs in MongoDB `jobs` collection
- Log error details for analysis
- Provide visibility into processing failures

**Technology Stack:**
- KafkaJS for Kafka consumer
- MongoDB Node.js driver

**Key Design Decisions:**
- **Separate Service:** Runs as separate service for isolation
- **MongoDB Storage:** Failed jobs stored for analysis and monitoring
- **Extensible:** Can be extended for retry mechanisms, alerting, or archival

## Data Flow

### Happy Path (Two-Stage Pipeline)

1. **Client Uploads File(s)**
   - Client sends POST request with CSV file(s) to `/api/v1/upload` or `/api/v1/upload/batch`
   - Ingestion API receives file(s) via Multer middleware
   - Supports up to 100 files in batch upload

2. **Basic Validation (API Level)**
   - File size validation (check against max size)
   - File type validation (CSV extension and MIME type)
   - **Note:** CSV format validation moved to validation service

3. **File Storage**
   - Validated file(s) stored on filesystem
   - File path recorded for later processing

4. **Kafka Event Production (Stage 1)**
   - Ingestion API produces message to `file-ingestion` topic
   - Message contains: jobId, fileId, filePath, fileSize, etc.
   - **No CSV content or headers** - determined by validation service

5. **Validation Service Consumption**
   - Validation service consumes message from `file-ingestion` topic
   - Opens CSV file from storage

6. **Row Validation & Chunking**
   - CSV file streamed row by row
   - Each row validated (id, name, email, created_at)
   - Rows separated into valid and invalid
   - Rows chunked into 5,000-row batches

7. **Validated Chunk Production (Stage 2)**
   - Validation service produces validated chunks to `validated-chunks` topic
   - Each chunk contains: validRows[], invalidRows[], chunkNumber, totalChunks

8. **DB Ingestion Service Consumption**
   - DB ingestion service consumes validated chunks
   - Inserts valid rows to `csv_records` collection
   - Inserts invalid rows to `error_records` collection

9. **Completion**
   - All chunks processed
   - Kafka offsets committed
   - Job status updated in MongoDB

### Error Path

1. **Basic Validation Failure (API Level)**
   - File size or type validation fails
   - File is deleted immediately
   - HTTP 400 error returned to client
   - No Kafka event is produced

2. **File Processing Failure (Validation Service)**
   - Error occurs during CSV streaming or validation
   - Error is logged with full context
   - DLQ message produced to `file-ingestion-dlq` topic
   - DLQ handler stores failure in MongoDB

3. **Chunk Insertion Failure (DB Ingestion Service)**
   - Error occurs during MongoDB insertion
   - Retry logic attempts 3 times with exponential backoff
   - If all retries fail, DLQ message produced to `validated-chunks-dlq` topic
   - DLQ handler stores failure in MongoDB
   - **Note:** Invalid rows (data validation failures) go to `error_records`, not DLQ

## Scalability Design

### Horizontal Scaling

**Ingestion API:**
- Can be scaled horizontally behind a load balancer
- Stateless design allows multiple instances
- File storage should be shared (NFS, S3, etc.)

**Worker Service:**
- Can be scaled horizontally
- Kafka consumer groups ensure load distribution
- Each worker processes different partitions
- MongoDB handles concurrent writes efficiently

**DLQ Handler:**
- Can be scaled independently
- Processes failures asynchronously

### Vertical Scaling

- Batch size can be tuned based on available memory
- MongoDB connection pool size can be adjusted
- Kafka consumer fetch size can be optimized

## Performance Optimizations

### Memory Efficiency

1. **Streaming Processing**
   - CSV files are never fully loaded into memory
   - Rows are processed one at a time
   - Batches are cleared after insertion

2. **Batch Size Tuning**
   - Configurable batch size (default: 10,000 rows)
   - Balance between memory usage and write performance
   - Smaller batches = less memory, more database calls
   - Larger batches = more memory, fewer database calls

3. **Connection Pooling**
   - MongoDB connection pooling reduces overhead
   - Reuses connections instead of creating new ones

### Throughput Optimization

1. **Parallel Processing**
   - Multiple workers can process different files simultaneously
   - Kafka partitions enable parallel consumption

2. **Bulk Operations**
   - MongoDB bulkWrite is more efficient than individual inserts
   - Reduces network round trips

3. **Stream Backpressure**
   - Stream pauses during database writes
   - Prevents memory buildup
   - Ensures writes complete before reading more

## Fault Tolerance

### Error Handling Strategy

1. **Validation Errors**
   - Caught immediately
   - Returned to client with clear error message
   - File is cleaned up

2. **Processing Errors**
   - Caught and logged
   - Job sent to DLQ
   - Original file preserved for retry
   - Kafka offset not committed (will retry)

3. **Infrastructure Failures**
   - Kafka: Automatic retry with exponential backoff
   - MongoDB: Connection retry logic
   - Graceful shutdown on SIGTERM/SIGINT

### Idempotency

- Job IDs prevent duplicate processing
- File IDs ensure consistent partitioning
- MongoDB can handle duplicate inserts (if needed)

## Security Considerations

### Input Validation
- File size limits prevent DoS attacks
- File type validation prevents malicious uploads
- CSV parsing handles malformed data safely

### Data Security
- Files stored securely (local filesystem or encrypted cloud storage)
- MongoDB connection should use authentication (configure in production)
- Kafka should use SSL/TLS in production

### API Security
- Rate limiting recommended (not implemented in this version)
- Authentication/authorization recommended (not implemented)
- Input sanitization in CSV parsing

## Monitoring & Observability

### Logging
- Structured JSON logging throughout
- Log levels: ERROR, WARN, INFO, DEBUG
- Includes context: jobId, fileId, timestamps, etc.

### Metrics (Recommended for Production)
- Upload count and success rate
- Processing time per file
- Rows processed per second
- Error rates by type
- Kafka lag
- MongoDB write performance

### Health Checks
- `/health` endpoint for service health
- Kafka connectivity check
- MongoDB connectivity check

## Technology Choices

### Why Node.js?
- Excellent streaming capabilities
- Non-blocking I/O for high concurrency
- Rich ecosystem (Kafka, MongoDB drivers)
- Good performance for I/O-bound workloads

### Why Kafka?
- High throughput message queue
- Durability and reliability
- Horizontal scalability
- Consumer groups for load distribution
- DLQ pattern support

### Why MongoDB?
- Flexible schema (good for CSV data)
- High write performance
- Horizontal scaling capability
- Bulk write operations
- Rich query capabilities

### Why Streaming?
- Memory efficiency for large files
- Can handle files larger than available RAM
- Backpressure handling
- Real-time processing

## Future Enhancements

1. **Data Transformation**
   - Support for data transformations before insertion
   - Schema mapping and validation
   - Data enrichment

2. **Retry Mechanism**
   - Automatic retry for transient failures
   - Exponential backoff
   - Max retry limits

3. **Progress Tracking**
   - Job status API
   - Progress percentage
   - Estimated completion time

4. **Multi-Format Support**
   - Support for other file formats (JSON, Parquet, etc.)
   - Format detection
   - Format-specific processors

5. **Cloud Storage Integration**
   - S3, Azure Blob, GCS support
   - Direct streaming from cloud storage
   - No local file storage needed

6. **Advanced Monitoring**
   - Prometheus metrics
   - Grafana dashboards
   - Distributed tracing

7. **API Enhancements**
   - Authentication/authorization
   - Rate limiting
   - API versioning
   - OpenAPI/Swagger documentation

