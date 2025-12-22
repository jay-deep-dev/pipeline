/**
 * Shared Configuration Module
 * 
 * Centralized configuration management for the CSV ingestion pipeline.
 * Loads environment variables and provides typed configuration access.
 * 
 * @module config
 */

import { join } from 'path';

/**
 * Application configuration object
 * @typedef {Object} Config
 * @property {Object} kafka - Kafka configuration
 * @property {Object} mongodb - MongoDB configuration
 * @property {Object} storage - File storage configuration
 * @property {Object} validation - Validation configuration
 * @property {Object} worker - Worker configuration
 * @property {Object} api - API configuration
 */

/**
 * Get application configuration from environment variables
 * @returns {Config} Configuration object
 */
export function getConfig() {
  return {
    kafka: {
      brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
      clientId: process.env.KAFKA_CLIENT_ID || 'csv-ingestion-pipeline',
      ingestionTopic: process.env.KAFKA_INGESTION_TOPIC || 'csv.ingestion',
      dlqTopic: process.env.KAFKA_DLQ_TOPIC || 'csv.ingestion.dlq',
      consumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID || 'csv-ingestion-workers',
      ingestionPartitions: parseInt(process.env.KAFKA_INGESTION_PARTITIONS || '6', 10),
      ingestionReplicationFactor: parseInt(process.env.KAFKA_INGESTION_REPLICATION_FACTOR || '1', 10),
      dlqPartitions: parseInt(process.env.KAFKA_DLQ_PARTITIONS || '3', 10),
      dlqReplicationFactor: parseInt(process.env.KAFKA_DLQ_REPLICATION_FACTOR || '1', 10),
    },
    mongodb: {
      uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
      database: process.env.MONGODB_DATABASE || 'csv_ingestion',
      collection: process.env.MONGODB_COLLECTION || 'csv_records',
    },
    storage: {
      uploadDir: (() => {
        const uploadDir = process.env.UPLOAD_DIR || join(process.cwd(), 'uploads');
        // Resolve relative paths to absolute paths
        return uploadDir.startsWith('/') ? uploadDir : join(process.cwd(), uploadDir);
      })(),
      maxFileSizeMB: parseInt(process.env.MAX_FILE_SIZE_MB || '5000', 10),
      allowedFileTypes: (process.env.ALLOWED_FILE_TYPES || 'csv,text/csv').split(','),
    },
    validation: {
      validateHeaders: process.env.VALIDATE_HEADERS === 'true',
      requiredColumns: (process.env.REQUIRED_COLUMNS || 'id,name,email,created_at').split(','),
      sampleRowsForValidation: parseInt(process.env.SAMPLE_ROWS_FOR_VALIDATION || '1000', 10),
      maxValidationRows: parseInt(process.env.MAX_VALIDATION_ROWS || '1000000', 10),
    },
    worker: {
      batchSize: parseInt(process.env.BATCH_SIZE || '10000', 10),
      maxConcurrentFiles: parseInt(process.env.MAX_CONCURRENT_FILES || '5', 10),
      pollIntervalMs: parseInt(process.env.WORKER_POLL_INTERVAL_MS || '100', 10),
    },
    api: {
      port: parseInt(process.env.API_PORT || '3000', 10),
      host: process.env.API_HOST || '0.0.0.0',
    },
    logging: {
      level: process.env.LOG_LEVEL || 'info',
    },
    ingestion: {
      chunkSize : 10000,
    }
  };
}

// Don't export a default cached config - always use getConfig() to get fresh values
// export default getConfig();

