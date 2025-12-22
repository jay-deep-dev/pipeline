/**
 * Kafka Message Schemas
 * 
 * Defines the structure of messages sent through Kafka topics.
 * Ensures consistency between producer and consumer services.
 * 
 * @module kafka-schemas
 */

/**
 * Ingestion job metadata message schema
 * Sent to csv.ingestion topic after successful validation
 * 
 * @typedef {Object} IngestionJobMessage
 * @property {string} jobId - Unique job identifier (UUID)
 * @property {string} fileId - Unique file identifier (UUID)
 * @property {string} fileName - Original filename
 * @property {string} filePath - Path to the uploaded file (relative or absolute)
 * @property {number} fileSizeBytes - File size in bytes
 * @property {string[]} headers - CSV column headers
 * @property {number} estimatedRowCount - Estimated number of rows (may be sampled)
 * @property {number} timestamp - Unix timestamp when job was created
 * @property {string} status - Job status (e.g., 'pending', 'processing', 'completed', 'failed')
 * @property {Object} metadata - Additional metadata (optional)
 */

/**
 * Create an ingestion job message
 * @param {Object} params - Message parameters
 * @param {string} params.jobId - Unique job identifier
 * @param {string} params.fileId - Unique file identifier
 * @param {string} params.fileName - Original filename
 * @param {string} params.filePath - Path to the uploaded file
 * @param {number} params.fileSizeBytes - File size in bytes
 * @param {string[]} params.headers - CSV column headers
 * @param {number} params.estimatedRowCount - Estimated row count
 * @param {Object} [params.metadata] - Additional metadata
 * @returns {IngestionJobMessage} Formatted message object
 */
export function createIngestionJobMessage({
  jobId,
  fileId,
  fileName,
  filePath,
  fileSizeBytes,
  headers,
  estimatedRowCount,
  metadata = {},
}) {
  return {
    jobId,
    fileId,
    fileName,
    filePath,
    fileSizeBytes,
    headers,
    estimatedRowCount,
    timestamp: Date.now(),
    status: 'pending',
    metadata,
  };
}

/**
 * Dead Letter Queue message schema
 * Sent to csv.ingestion.dlq topic when processing fails
 * 
 * @typedef {Object} DLQMessage
 * @property {string} jobId - Original job identifier
 * @property {string} fileId - Original file identifier
 * @property {string} fileName - Original filename
 * @property {string} filePath - Path to the file
 * @property {string} errorCode - Error code
 * @property {string} errorMessage - Human-readable error message
 * @property {Object} errorDetails - Additional error details
 * @property {number} timestamp - Unix timestamp when error occurred
 * @property {number} retryCount - Number of retry attempts
 * @property {Object} originalMessage - Original ingestion job message
 */

/**
 * Create a Dead Letter Queue message
 * @param {Object} params - DLQ message parameters
 * @param {string} params.jobId - Original job identifier
 * @param {string} params.fileId - Original file identifier
 * @param {string} params.fileName - Original filename
 * @param {string} params.filePath - Path to the file
 * @param {string} params.errorCode - Error code
 * @param {string} params.errorMessage - Error message
 * @param {Object} [params.errorDetails] - Additional error details
 * @param {number} [params.retryCount] - Retry count
 * @param {Object} [params.originalMessage] - Original ingestion message
 * @returns {DLQMessage} Formatted DLQ message object
 */
export function createDLQMessage({
  jobId,
  fileId,
  fileName,
  filePath,
  errorCode,
  errorMessage,
  errorDetails = {},
  retryCount = 0,
  originalMessage = null,
}) {
  return {
    jobId,
    fileId,
    fileName,
    filePath,
    errorCode,
    errorMessage,
    errorDetails,
    timestamp: Date.now(),
    retryCount,
    originalMessage,
  };
}

/**
 * Validate ingestion job message structure
 * @param {Object} message - Message to validate
 * @returns {boolean} True if valid
 * @throws {Error} If message is invalid
 */
export function validateIngestionJobMessage(message) {
  const requiredFields = [
    'jobId',
    'fileId',
    'fileName',
    'filePath',
    'fileSizeBytes',
    'headers',
    'estimatedRowCount',
    'timestamp',
    'status',
  ];

  for (const field of requiredFields) {
    if (!(field in message)) {
      throw new Error(`Missing required field: ${field}`);
    }
  }

  if (!Array.isArray(message.headers)) {
    throw new Error('headers must be an array');
  }

  if (typeof message.estimatedRowCount !== 'number' || message.estimatedRowCount < 0) {
    throw new Error('estimatedRowCount must be a non-negative number');
  }

  return true;
}

