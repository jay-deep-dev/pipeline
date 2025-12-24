/**
 * Upload Controller
 * 
 * Handles HTTP requests and responses for file upload endpoints.
 * 
 * @module controllers/upload-controller
 */

import { createLogger, ValidationError } from '../../../shared/index.js';
import { processFileUpload, processMultipleFileUploads } from '../services/upload-service.js';

const logger = createLogger('upload-controller');

/**
 * Upload single file
 * POST /api/v1/upload
 */
export async function uploadSingleFile(req, res) {
  const startTime = Date.now();

  try {
    if (!req.file) {
      return res.status(400).json({
        error: 'No file uploaded',
        code: 'NO_FILE',
        message: 'Please provide a CSV file in the "file" field',
      });
    }

    const result = await processFileUpload(req.file, req);
    const processingTime = Date.now() - startTime;

    res.status(201).json({
      ...result,
      message: 'File uploaded successfully. Processing will begin shortly.',
      processingTimeMs: processingTime,
    });
  } catch (error) {
    const processingTime = Date.now() - startTime;

    if (error instanceof ValidationError) {
      return res.status(400).json({
        error: 'Validation failed',
        code: error.code,
        message: error.message,
        details: error.details,
        processingTimeMs: processingTime,
      });
    }

    logger.error('Unexpected error during file upload', {
      error: error.message,
      stack: error.stack,
      processingTimeMs: processingTime,
    });

    res.status(500).json({
      error: 'Internal server error',
      code: 'INTERNAL_ERROR',
      message: 'An unexpected error occurred during file processing',
      processingTimeMs: processingTime,
    });
  }
}

/**
 * Upload multiple files (up to 100)
 * POST /api/v1/upload/batch
 */
export async function uploadMultipleFiles(req, res) {
  const startTime = Date.now();

  try {
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({
        error: 'No files uploaded',
        code: 'NO_FILES',
        message: 'Please provide CSV files in the "files" field',
      });
    }

    // Limit to 100 files
    if (req.files.length > 100) {
      return res.status(400).json({
        error: 'Too many files',
        code: 'TOO_MANY_FILES',
        message: 'Maximum 100 files can be uploaded at once',
        received: req.files.length,
        maxAllowed: 100,
      });
    }

    const result = await processMultipleFileUploads(req.files, req);
    const processingTime = Date.now() - startTime;

    res.status(201).json({
      ...result,
      message: `${result.successful} file(s) uploaded successfully. Processing will begin shortly.`,
      processingTimeMs: processingTime,
    });
  } catch (error) {
    const processingTime = Date.now() - startTime;

    logger.error('Unexpected error during batch file upload', {
      error: error.message,
      stack: error.stack,
      processingTimeMs: processingTime,
    });

    res.status(500).json({
      error: 'Internal server error',
      code: 'INTERNAL_ERROR',
      message: 'An unexpected error occurred during batch file processing',
      processingTimeMs: processingTime,
    });
  }
}

/**
 * Get upload status
 * GET /api/v1/upload/status/:jobId
 */
export async function getUploadStatus(req, res) {
  const { jobId } = req.params;

  logger.info('Status check requested', { jobId });

  // In production, this would query a job status store
  res.json({
    jobId,
    status: 'pending',
    message: 'Job status tracking not implemented in this version',
  });
}

