# Quick Start Guide

Get the CSV Ingestion Pipeline up and running in minutes!

## Prerequisites

- Docker and Docker Compose installed
- curl (for testing)

## Step 1: Start Services

```bash
# Start all services (Kafka, MongoDB, API, Workers)
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f ingestion-api
docker-compose logs -f worker
```

## Step 2: Wait for Services to be Ready

Wait about 30-60 seconds for all services to start up. You can check logs:

```bash
docker-compose logs ingestion-api | grep "server started"
docker-compose logs worker | grep "started successfully"
```

## Step 3: Test the API

### Health Check
```bash
curl http://localhost:3000/health
```

### Generate a Sample CSV File
```bash
# Generate a CSV with 1000 rows
node scripts/generate-sample-csv.js 1000 sample.csv

# Or generate a larger file (10,000 rows)
node scripts/generate-sample-csv.js 10000 large-sample.csv
```

### Upload a CSV File
```bash
# Upload the generated file
curl -X POST http://localhost:3000/api/v1/upload \
  -F "file=@sample.csv"
```

### Expected Response
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

## Step 4: Monitor Processing

### View Worker Logs
```bash
docker-compose logs -f worker
```

You should see logs like:
```
{"timestamp":"...","level":"INFO","service":"worker","message":"Processing ingestion job",...}
{"timestamp":"...","level":"INFO","service":"csv-processor","message":"CSV file processing completed",...}
```

### Check MongoDB
```bash
# Connect to MongoDB
docker-compose exec mongodb mongosh csv_ingestion

# In MongoDB shell:
db.csv_records.countDocuments()
db.csv_records.findOne()
```

## Step 5: Test with Multiple Files

### Upload 10 Files
```bash
./scripts/bulk-upload-test.sh 10 1000
```

### Upload 100 Files (as per requirements)
```bash
./scripts/bulk-upload-test.sh 100 10000
```

## Troubleshooting

### Services Not Starting
```bash
# Check if ports are already in use
lsof -i :3000
lsof -i :9092
lsof -i :27017

# Restart services
docker-compose restart
```

### Kafka Connection Issues
```bash
# Check Kafka logs
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka
```

### MongoDB Connection Issues
```bash
# Check MongoDB logs
docker-compose logs mongodb

# Restart MongoDB
docker-compose restart mongodb
```

### File Upload Fails
```bash
# Check API logs
docker-compose logs ingestion-api

# Common issues:
# - File too large (check MAX_FILE_SIZE_MB in .env)
# - Invalid file type (must be .csv)
# - Missing required columns
```

## Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clears data)
docker-compose down -v
```

## Next Steps

- Read [README.md](README.md) for detailed documentation
- Read [ARCHITECTURE.md](ARCHITECTURE.md) for architecture details
- Customize configuration in `.env` file
- Scale workers: `docker-compose up -d --scale worker=5`

## Performance Testing

For testing with large files (10 million rows):

```bash
# Generate a large CSV file (this may take a while)
node scripts/generate-sample-csv.js 10000000 large-file.csv

# Upload it
curl -X POST http://localhost:3000/api/v1/upload \
  -F "file=@large-file.csv"
```

Monitor the processing in worker logs. The system will process the file in batches of 10,000 rows (configurable).

