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
      // Two-Stage Pipeline Topics
      fileIngestionTopic: process.env.KAFKA_FILE_INGESTION_TOPIC || 'file-ingestion',
      validatedChunksTopic: process.env.KAFKA_VALIDATED_CHUNKS_TOPIC || 'validated-chunks',
      // DLQ Topics
      fileIngestionDlqTopic: process.env.KAFKA_FILE_INGESTION_DLQ_TOPIC || 'file-ingestion-dlq',
      validatedChunksDlqTopic: process.env.KAFKA_VALIDATED_CHUNKS_DLQ_TOPIC || 'validated-chunks-dlq',
      // Consumer Groups
      validationConsumerGroupId: process.env.KAFKA_VALIDATION_CONSUMER_GROUP_ID || 'validation-service-workers',
      dbIngestionConsumerGroupId: process.env.KAFKA_DB_INGESTION_CONSUMER_GROUP_ID || 'db-ingestion-service-workers',
      // Topic Partitions & Replication
      fileIngestionPartitions: parseInt(process.env.KAFKA_FILE_INGESTION_PARTITIONS || '6', 10),
      fileIngestionReplicationFactor: parseInt(process.env.KAFKA_FILE_INGESTION_REPLICATION_FACTOR || '3', 10),
      validatedChunksPartitions: parseInt(process.env.KAFKA_VALIDATED_CHUNKS_PARTITIONS || '24', 10),
      validatedChunksReplicationFactor: parseInt(process.env.KAFKA_VALIDATED_CHUNKS_REPLICATION_FACTOR || '3', 10),
      // Legacy support (for backward compatibility)
      ingestionTopic: process.env.KAFKA_FILE_INGESTION_TOPIC || 'file-ingestion',
      dlqTopic: process.env.KAFKA_FILE_INGESTION_DLQ_TOPIC || 'file-ingestion-dlq',
      consumerGroupId: process.env.KAFKA_VALIDATION_CONSUMER_GROUP_ID || 'validation-service-workers',
      ingestionPartitions: parseInt(process.env.KAFKA_FILE_INGESTION_PARTITIONS || '6', 10),
      ingestionReplicationFactor: parseInt(process.env.KAFKA_FILE_INGESTION_REPLICATION_FACTOR || '3', 10),
      dlqPartitions: parseInt(process.env.KAFKA_FILE_INGESTION_DLQ_PARTITIONS || '3', 10),
      dlqReplicationFactor: parseInt(process.env.KAFKA_FILE_INGESTION_DLQ_REPLICATION_FACTOR || '3', 10),
    },
    mongodb: {
      uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
      database: process.env.MONGODB_DATABASE || 'csv_ingestion',
      validRecordsCollection: process.env.MONGODB_VALID_RECORDS_COLLECTION || 'csv_records',
      errorRecordsCollection: process.env.MONGODB_ERROR_RECORDS_COLLECTION || 'error_records',
      jobsCollection: process.env.MONGODB_JOBS_COLLECTION || 'jobs',
      // Legacy support
      collection: process.env.MONGODB_VALID_RECORDS_COLLECTION || 'csv_records',
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
    validation: {
      chunkSize: parseInt(process.env.VALIDATION_CHUNK_SIZE || '5000', 10),
      maxConcurrentFiles: parseInt(process.env.VALIDATION_MAX_CONCURRENT_FILES || '6', 10),
      pollIntervalMs: parseInt(process.env.VALIDATION_POLL_INTERVAL_MS || '100', 10),
    },
    dbIngestion: {
      batchSize: parseInt(process.env.DB_INGESTION_BATCH_SIZE || '5000', 10),
      maxConcurrentChunks: parseInt(process.env.DB_INGESTION_MAX_CONCURRENT_CHUNKS || '24', 10),
      pollIntervalMs: parseInt(process.env.DB_INGESTION_POLL_INTERVAL_MS || '100', 10),
      retryCount: parseInt(process.env.DB_INGESTION_RETRY_COUNT || '3', 10),
      retryBackoffMs: parseInt(process.env.DB_INGESTION_RETRY_BACKOFF_MS || '1000', 10),
    },
    // Legacy worker config (for backward compatibility)
    worker: {
      batchSize: parseInt(process.env.DB_INGESTION_BATCH_SIZE || '5000', 10),
      maxConcurrentFiles: parseInt(process.env.VALIDATION_MAX_CONCURRENT_FILES || '6', 10),
      pollIntervalMs: parseInt(process.env.DB_INGESTION_POLL_INTERVAL_MS || '100', 10),
    },
    api: {
      port: parseInt(process.env.API_PORT || '3000', 10),
      host: process.env.API_HOST || '0.0.0.0',
    },
    logging: {
      level: process.env.LOG_LEVEL || 'info',
    },
  };
}

// Don't export a default cached config - always use getConfig() to get fresh values
// export default getConfig();

