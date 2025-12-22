/**
 * Upload Routes Module
 * 
 * Express routes for CSV file upload and ingestion.
 * Handles file upload, validation, and Kafka event production.
 * 
 * @module routes/upload
 */

import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { getConfig, createLogger, validateFile } from '../../../shared/index.js';
import { getKafkaProducer } from '../kafka-producer.js';
import { upload, getFilePath, getFileId } from '../upload-handler.js';
import { statSync } from 'fs';

const router = express.Router();
const config = getConfig();
const logger = createLogger('upload-routes', config.logging.level);
const kafkaProducer = getKafkaProducer();

/**
 * Health check endpoint
 * GET /health
 */
router.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    service: 'ingestion-api',
    timestamp: new Date().toISOString(),
  });
});

/**
 * Upload CSV file endpoint
 * POST /api/v1/upload
 * 
 * Multipart form data with 'file' field containing CSV file.
 * 
 * Flow:
 * 1. Receive file upload
 * 2. Validate file (size, type, format)
 * 3. If validation fails, return error immediately
 * 4. If validation succeeds, produce Kafka event
 * 5. Return success response with job metadata
 */
router.post('/api/v1/upload', upload.single('file'), async (req, res) => {
  const startTime = Date.now();
  let filePath = null;
  let fileId = null;
  let jobId = null;

  try {
    // Check if file was uploaded
    if (!req.file) {
      return res.status(400).json({
        error: 'No file uploaded',
        code: 'NO_FILE',
        message: 'Please provide a CSV file in the "file" field',
      });
    }

    filePath = getFilePath(req.file);
    fileId = getFileId(req);
    jobId = uuidv4();

    logger.info('File upload received', {
      jobId,
      fileId,
      fileName: req.file.originalname,
      fileSize: req.file.size,
      filePath,
    });

    // Validate file (size, type, format)
    const validationResult = await validateFile(
      filePath,
      req.file.originalname,
      req.file.mimetype,
      config
    );

    logger.info('File validation passed', {
      jobId,
      fileId,
      fileName: req.file.originalname,
      headers: validationResult.headers,
      rowCount: validationResult.rowCount,
    });

    // Get file stats for metadata
    const fileStats = statSync(filePath);

    // Produce Kafka event with job metadata
    await kafkaProducer.produceIngestionJob({
      jobId,
      fileId,
      fileName: req.file.originalname,
      filePath,
      fileSizeBytes: fileStats.size,
      headers: validationResult.headers,
      estimatedRowCount: validationResult.rowCount,
      metadata: {
        uploadedAt: new Date().toISOString(),
        mimeType: req.file.mimetype,
        sampledRows: validationResult.sampledRows,
      },
    });

    const processingTime = Date.now() - startTime;

    logger.info('File upload and validation completed successfully', {
      jobId,
      fileId,
      fileName: req.file.originalname,
      processingTimeMs: processingTime,
    });

    res.status(201).json({
      success: true,
      jobId,
      fileId,
      fileName: req.file.originalname,
      fileSize: fileStats.size,
      headers: validationResult.headers,
      estimatedRowCount: validationResult.rowCount,
      status: 'pending',
      message: 'File uploaded and validated successfully. Processing will begin shortly.',
      processingTimeMs: processingTime,
    });
  } catch (error) {
    const processingTime = Date.now() - startTime;

    // Handle validation errors (business logic errors only)
    if (error.name === 'ValidationError') {
      logger.error('File validation failed', {
        jobId,
        fileId,
        errorCode: error.code,
        errorMessage: error.message,
        details: error.details,
        processingTimeMs: processingTime,
      });

      // Clean up uploaded file on validation failure
      if (filePath) {
        try {
          const { unlinkSync } = await import('fs');
          unlinkSync(filePath);
          logger.info('Invalid file deleted', { filePath });
        } catch (deleteError) {
          logger.warn('Failed to delete invalid file', {
            filePath,
            error: deleteError.message,
          });
        }
      }

      return res.status(400).json({
        error: 'Validation failed',
        code: error.code,
        message: error.message,
        details: error.details,
        processingTimeMs: processingTime,
      });
    }

    // Handle other unexpected errors (Kafka, etc.)
    logger.error('Unexpected error during file processing', {
      jobId,
      fileId,
      error: error.message,
      stack: error.stack,
      processingTimeMs: processingTime,
    });

    // Clean up on error
    if (filePath) {
      try {
        const { unlinkSync } = await import('fs');
        unlinkSync(filePath);
      } catch (deleteError) {
        logger.warn('Failed to delete file after error', {
          filePath,
          error: deleteError.message,
        });
      }
    }

    res.status(500).json({
      error: 'Internal server error',
      code: 'INTERNAL_ERROR',
      message: 'An unexpected error occurred during file processing',
      processingTimeMs: processingTime,
    });
  }
});

/**
 * Get upload status endpoint
 * GET /api/v1/upload/status/:jobId
 * 
 * Note: In a production system, this would query a database or cache
 * to get the actual job status. For this implementation, we return
 * a placeholder response.
 */
router.get('/api/v1/upload/status/:jobId', (req, res) => {
  const { jobId } = req.params;

  logger.info('Status check requested', { jobId });

  // In production, this would query a job status store
  res.json({
    jobId,
    status: 'pending',
    message: 'Job status tracking not implemented in this version',
  });
});

export default router;

