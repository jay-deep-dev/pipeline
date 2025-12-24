/**
 * Kafka Message Schemas
 * 
 * Defines the structure of messages sent through Kafka topics.
 * Ensures consistency between producer and consumer services.
 * 
 * @module kafka-schemas
 */

/**
 * File ingestion job message schema
 * Sent to file-ingestion topic after file upload
 * 
 * @typedef {Object} FileIngestionMessage
 * @property {string} jobId - Unique job identifier (UUID)
 * @property {string} fileId - Unique file identifier (UUID)
 * @property {string} fileName - Original filename
 * @property {string} filePath - Path to the uploaded file (relative or absolute)
 * @property {number} fileSizeBytes - File size in bytes
 * @property {string[]} headers - CSV column headers
 * @property {number} estimatedRowCount - Estimated number of rows (may be sampled)
 * @property {number} timestamp - Unix timestamp when job was created
 * @property {string} status - Job status (e.g., 'UPLOADED', 'VALIDATING', 'COMPLETED', 'FAILED')
 * @property {Object} metadata - Additional metadata (optional)
 */

/**
 * Validated chunk message schema
 * Sent to validated-chunks topic after row validation
 * 
 * @typedef {Object} ValidatedChunkMessage
 * @property {string} jobId - Unique job identifier (UUID)
 * @property {string} fileId - Unique file identifier (UUID)
 * @property {string} chunkId - Unique chunk identifier (UUID)
 * @property {number} chunkNumber - Chunk sequence number (1-indexed)
 * @property {number} totalChunks - Total number of chunks for this file
 * @property {Object[]} validRows - Array of validated row objects
 * @property {Object[]} invalidRows - Array of invalid row objects with error messages
 * @property {number} startRowNumber - Starting row number (1-indexed)
 * @property {number} endRowNumber - Ending row number (1-indexed)
 * @property {number} timestamp - Unix timestamp when chunk was created
 * @property {string} status - Chunk status (e.g., 'VALIDATED', 'PROCESSING', 'COMPLETED', 'FAILED')
 */

/**
 * Legacy ingestion job metadata message schema
 * @deprecated Use FileIngestionMessage instead
 * @typedef {Object} IngestionJobMessage
 */

/**
 * Create a file ingestion message
 * @param {Object} params - Message parameters
 * @param {string} params.jobId - Unique job identifier
 * @param {string} params.fileId - Unique file identifier
 * @param {string} params.fileName - Original filename
 * @param {string} params.filePath - Path to the uploaded file
 * @param {number} params.fileSizeBytes - File size in bytes
 * @param {string[]} params.headers - CSV column headers
 * @param {number} params.estimatedRowCount - Estimated row count
 * @param {Object} [params.metadata] - Additional metadata
 * @returns {FileIngestionMessage} Formatted message object
 */
export function createFileIngestionMessage({
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
    status: 'UPLOADED',
    metadata,
  };
}

/**
 * Create a validated chunk message
 * @param {Object} params - Message parameters
 * @param {string} params.jobId - Unique job identifier
 * @param {string} params.fileId - Unique file identifier
 * @param {string} params.chunkId - Unique chunk identifier
 * @param {number} params.chunkNumber - Chunk sequence number
 * @param {number} params.totalChunks - Total number of chunks
 * @param {Object[]} params.validRows - Array of validated rows
 * @param {Object[]} params.invalidRows - Array of invalid rows
 * @param {number} params.startRowNumber - Starting row number
 * @param {number} params.endRowNumber - Ending row number
 * @returns {ValidatedChunkMessage} Formatted message object
 */
export function createValidatedChunkMessage({
  jobId,
  fileId,
  chunkId,
  chunkNumber,
  totalChunks,
  validRows,
  invalidRows,
  startRowNumber,
  endRowNumber,
}) {
  return {
    jobId,
    fileId,
    chunkId,
    chunkNumber,
    totalChunks,
    validRows: validRows || [],
    invalidRows: invalidRows || [],
    startRowNumber,
    endRowNumber,
    timestamp: Date.now(),
    status: 'VALIDATED',
  };
}

/**
 * Create an ingestion job message (legacy)
 * @deprecated Use createFileIngestionMessage instead
 * @param {Object} params - Message parameters
 * @returns {IngestionJobMessage} Formatted message object
 */
export function createIngestionJobMessage(params) {
  return createFileIngestionMessage(params);
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
 * Validate file ingestion message structure
 * @param {Object} message - Message to validate
 * @returns {boolean} True if valid
 * @throws {Error} If message is invalid
 */
export function validateFileIngestionMessage(message) {
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

/**
 * Validate validated chunk message structure
 * @param {Object} message - Message to validate
 * @returns {boolean} True if valid
 * @throws {Error} If message is invalid
 */
export function validateValidatedChunkMessage(message) {
  const requiredFields = [
    'jobId',
    'fileId',
    'chunkId',
    'chunkNumber',
    'totalChunks',
    'validRows',
    'invalidRows',
    'startRowNumber',
    'endRowNumber',
    'timestamp',
    'status',
  ];

  for (const field of requiredFields) {
    if (!(field in message)) {
      throw new Error(`Missing required field: ${field}`);
    }
  }

  if (!Array.isArray(message.validRows)) {
    throw new Error('validRows must be an array');
  }

  if (!Array.isArray(message.invalidRows)) {
    throw new Error('invalidRows must be an array');
  }

  if (typeof message.chunkNumber !== 'number' || message.chunkNumber < 1) {
    throw new Error('chunkNumber must be a positive number');
  }

  return true;
}

/**
 * Validate ingestion job message structure (legacy)
 * @deprecated Use validateFileIngestionMessage instead
 * @param {Object} message - Message to validate
 * @returns {boolean} True if valid
 * @throws {Error} If message is invalid
 */
export function validateIngestionJobMessage(message) {
  return validateFileIngestionMessage(message);
}

