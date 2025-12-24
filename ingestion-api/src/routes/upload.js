/**
 * Upload Routes Module
 * 
 * Express routes for CSV file upload and ingestion.
 * Uses controller pattern for better code organization.
 * 
 * @module routes/upload
 */

import express from 'express';
import { createLogger } from '../../../shared/index.js';
import { uploadSingle, uploadMultiple } from '../middleware/upload-handler.js';
import { uploadSingleFile, uploadMultipleFiles, getUploadStatus } from '../controllers/upload-controller.js';

const router = express.Router();
const logger = createLogger('upload-routes');

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
 * Upload single CSV file endpoint
 * POST /api/v1/upload
 * 
 * Multipart form data with 'file' field containing CSV file.
 */
router.post('/api/v1/upload', uploadSingle, uploadSingleFile);

/**
 * Upload multiple CSV files endpoint (up to 100 files)
 * POST /api/v1/upload/batch
 * 
 * Multipart form data with 'files' field containing array of CSV files.
 */
router.post('/api/v1/upload/batch', uploadMultiple, uploadMultipleFiles);

/**
 * Get upload status endpoint
 * GET /api/v1/upload/status/:jobId
 */
router.get('/api/v1/upload/status/:jobId', getUploadStatus);

export default router;

