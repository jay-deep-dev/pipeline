# Architecture Documentation

## System Architecture

### Overview

The CSV Ingestion Pipeline is designed as a microservices architecture with clear separation of concerns. The system follows an event-driven pattern using Kafka for asynchronous processing, ensuring high throughput and scalability.

## Components

### 1. Ingestion API Service

**Purpose:** Handles file uploads, performs validation, and initiates processing workflow.

**Responsibilities:**
- Accept file uploads via HTTP API
- Validate file size, type, and format
- Store uploaded files
- Produce Kafka events with job metadata
- Return immediate feedback to clients

**Technology Stack:**
- Express.js for HTTP server
- Multer for file upload handling
- KafkaJS for Kafka producer
- csv-parser for streaming CSV validation

**Key Design Decisions:**
- **Synchronous Validation:** Validation happens synchronously before producing Kafka event. This ensures invalid files don't enter the processing pipeline.
- **Metadata-Only Events:** Only metadata is sent to Kafka, not file contents. This reduces Kafka message size and improves performance.
- **Immediate Error Response:** Validation errors are returned immediately, providing fast feedback to clients.

### 2. Worker Service

**Purpose:** Consumes Kafka events and processes CSV files, inserting data into MongoDB.

**Responsibilities:**
- Consume ingestion job messages from Kafka
- Stream CSV files (memory-efficient)
- Process rows in batches
- Insert batches into MongoDB using bulk operations
- Handle errors and send failed jobs to DLQ

**Technology Stack:**
- KafkaJS for Kafka consumer
- csv-parser for streaming CSV parsing
- MongoDB Node.js driver for database operations

**Key Design Decisions:**
- **Streaming Processing:** Files are processed using Node.js streams, preventing memory exhaustion for large files.
- **Batch Inserts:** Rows are accumulated in batches before inserting to MongoDB, optimizing write performance.
- **Backpressure Handling:** Stream is paused during database writes to prevent memory buildup.
- **Idempotency:** Job tracking prevents duplicate processing of the same file.

### 3. DLQ Handler Service

**Purpose:** Processes failed jobs from the Dead Letter Queue.

**Responsibilities:**
- Consume messages from DLQ topic
- Log error details
- Optionally archive or retry failed jobs
- Provide visibility into processing failures

**Technology Stack:**
- KafkaJS for Kafka consumer

**Key Design Decisions:**
- **Separate Service:** DLQ handler runs as a separate service for isolation and independent scaling.
- **Error Logging:** Comprehensive error logging for debugging and analysis.
- **Extensible:** Can be extended to support retry mechanisms, alerting, or archival.

## Data Flow

### Happy Path

1. **Client Uploads File**
   - Client sends POST request with CSV file to `/api/v1/upload`
   - Ingestion API receives file via Multer middleware

2. **Validation**
   - File size validation (check against max size)
   - File type validation (CSV extension and MIME type)
   - CSV format validation (headers, sample rows)

3. **File Storage**
   - Validated file is stored on filesystem (or cloud storage)
   - File path is recorded for later processing

4. **Kafka Event Production**
   - Ingestion API produces message to `csv.ingestion` topic
   - Message contains: jobId, fileId, filePath, headers, row count, etc.

5. **Worker Consumption**
   - Worker service consumes message from Kafka
   - Worker opens CSV file from storage

6. **CSV Processing**
   - CSV file is streamed row by row
   - Rows are accumulated into batches
   - When batch reaches configured size, insert to MongoDB

7. **MongoDB Insertion**
   - Batch is inserted using MongoDB bulkWrite
   - Process continues until all rows are processed

8. **Completion**
   - Worker logs completion statistics
   - Kafka offset is committed

### Error Path

1. **Validation Failure**
   - Validation error is thrown
   - File is deleted
   - HTTP 400 error returned to client
   - No Kafka event is produced

2. **Processing Failure**
   - Error occurs during CSV processing or MongoDB insertion
   - Error is logged with full context
   - DLQ message is produced to `csv.ingestion.dlq` topic
   - DLQ handler processes the failure

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

