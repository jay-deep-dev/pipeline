/**
 * Shared Library Entry Point
 * 
 * Exports all shared utilities and configurations
 * for use across ingestion-api and worker services.
 * 
 * @module shared
 */

export { getConfig } from './src/config.js';
export { createLogger } from './src/logger.js';
export {
  validateFile,
  validateFileSize,
  validateFileType,
  validateCsvFormat,
  ValidationError,
} from './src/validation.js';
export {
  createFileIngestionMessage,
  createValidatedChunkMessage,
  createIngestionJobMessage, // Legacy
  createDLQMessage,
  validateFileIngestionMessage,
  validateValidatedChunkMessage,
  validateIngestionJobMessage, // Legacy
} from './src/kafka-schemas.js';
export { createStorage } from './src/storage.js';
export { validateRow, validateBatch, getValidationStats } from './src/row-validation.js';

