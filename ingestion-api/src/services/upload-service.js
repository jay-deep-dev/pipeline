/**
 * Upload Service
 * 
 * Business logic for file upload processing.
 * Handles basic validation and Kafka event production.
 * 
 * @module services/upload-service
 */

import { v4 as uuidv4 } from 'uuid';
import { statSync, unlinkSync } from 'fs';
import { getConfig, createLogger, validateFileSize, validateFileType, ValidationError } from '../../../shared/index.js';
import { getKafkaProducer } from '../kafka-producer.js';
import { getFilePath, getFileId } from '../middleware/upload-handler.js';

const config = getConfig();
const logger = createLogger('upload-service', config.logging.level);

/**
 * Process single file upload
 * Performs basic validation (size, type) and produces Kafka event
 * 
 * @param {Object} file - Multer file object
 * @param {Object} req - Express request object
 * @returns {Promise<Object>} Upload result with job metadata
 */
export async function processFileUpload(file, req) {
  const filePath = getFilePath(file);
  const fileId = getFileId(req, file);
  const jobId = uuidv4();

  logger.info('Processing file upload', {
    jobId,
    fileId,
    fileName: file.originalname,
    fileSize: file.size,
    filePath,
  });

  try {
    // Basic validation only (size and type)
    // Heavy CSV format validation is done in validation service
    validateFileSize(filePath, config.storage.maxFileSizeMB);
    validateFileType(filePath, file.originalname, file.mimetype, config.storage.allowedFileTypes);

    // Get file stats
    const fileStats = statSync(filePath);

    // Produce Kafka event with job metadata
    // Note: headers and rowCount will be determined by validation service
    const kafkaProducer = getKafkaProducer();
    await kafkaProducer.produceIngestionJob({
      jobId,
      fileId,
      fileName: file.originalname,
      filePath,
      fileSizeBytes: fileStats.size,
      headers: [], // Will be populated by validation service
      estimatedRowCount: 0, // Will be determined by validation service
      metadata: {
        uploadedAt: new Date().toISOString(),
        mimeType: file.mimetype,
      },
    });

    logger.info('File upload processed successfully', {
      jobId,
      fileId,
      fileName: file.originalname,
    });

    return {
      success: true,
      jobId,
      fileId,
      fileName: file.originalname,
      fileSize: fileStats.size,
      status: 'pending',
    };
  } catch (error) {
    // Clean up file on error
    try {
      unlinkSync(filePath);
      logger.info('File deleted after error', { filePath });
    } catch (deleteError) {
      logger.warn('Failed to delete file after error', {
        filePath,
        error: deleteError.message,
      });
    }

    throw error;
  }
}

/**
 * Process multiple file uploads
 * 
 * @param {Object[]} files - Array of Multer file objects
 * @param {Object} req - Express request object
 * @returns {Promise<Object>} Upload results
 */
export async function processMultipleFileUploads(files, req) {
  const results = [];
  const errors = [];

  logger.info('Processing multiple file uploads', {
    fileCount: files.length,
  });

  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    try {
      const result = await processFileUpload(file, req);
      results.push(result);
    } catch (error) {
      const fileId = getFileId(req, file);
      errors.push({
        fileName: file.originalname,
        fileId,
        error: error.message,
        code: error.code || 'PROCESSING_ERROR',
      });
      logger.error('File upload failed', {
        fileName: file.originalname,
        error: error.message,
        code: error.code,
      });
    }
  }

  return {
    success: results.length > 0,
    totalFiles: files.length,
    successful: results.length,
    failed: errors.length,
    results,
    errors: errors.length > 0 ? errors : undefined,
  };
}

