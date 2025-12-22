/**
 * Upload Handler Module
 * 
 * Handles file upload using multer middleware.
 * Configures multer for CSV file uploads with size limits.
 * Includes error handling for multer-specific errors.
 * 
 * @module upload-handler
 */

import multer from 'multer';
import { getConfig, createLogger } from '../../shared/index.js';
import { createStorage } from '../../shared/src/storage.js';
import { v4 as uuidv4 } from 'uuid';
import { join } from 'path';

const config = getConfig();
const logger = createLogger('upload-handler', config.logging.level);

// Initialize storage
const storage = createStorage(config.storage.uploadDir);

/**
 * Configure multer storage
 * Stores files with unique identifiers
 */
const multerStorage = multer.diskStorage({
  destination: async (req, file, cb) => {
    try {
      await storage.initialize();
      cb(null, config.storage.uploadDir);
    } catch (error) {
      cb(error);
    }
  },
  filename: (req, file, cb) => {
    // Generate unique file ID and preserve original filename
    const fileId = uuidv4();
    req.fileId = fileId; // Attach to request for later use
    const fileName = `${fileId}_${file.originalname}`;
    cb(null, fileName);
  },
});

/**
 * File filter for CSV files only
 */
const fileFilter = (req, file, cb) => {
  const allowedMimeTypes = [
    'text/csv',
    'application/csv',
    'text/plain',
    'application/vnd.ms-excel',
  ];
  
  const allowedExtensions = ['.csv'];

  const fileExtension = file.originalname.toLowerCase().substring(
    file.originalname.lastIndexOf('.')
  );

  if (
    allowedMimeTypes.includes(file.mimetype) ||
    allowedExtensions.includes(fileExtension)
  ) {
    cb(null, true);
  } else {
    const error = new Error(
      `Invalid file type. Only CSV files are allowed. Received: ${file.mimetype}`
    );
    error.code = 'INVALID_FILE_TYPE';
    cb(error, false);
  }
};

/**
 * Multer middleware configuration
 * Limits file size and handles CSV files
 */
const multerUpload = multer({
  storage: multerStorage,
  fileFilter,
  limits: {
    fileSize: config.storage.maxFileSizeMB * 1024 * 1024, // Convert MB to bytes
    files: 1, // Only one file at a time
  },
});

/**
 * Middleware wrapper that handles multer errors
 * This ensures all multer-related errors are handled consistently
 * 
 * @param {Object} req - Express request
 * @param {Object} res - Express response
 * @param {Function} next - Next middleware
 */
export const upload = {
  single: (fieldName) => {
    return (req, res, next) => {
      const uploadSingle = multerUpload.single(fieldName);
      
      uploadSingle(req, res, (error) => {
        if (error) {
          // Handle Multer errors
          if (error instanceof multer.MulterError) {
            if (error.code === 'LIMIT_FILE_SIZE') {
              logger.error('File size limit exceeded', {
                error: error.message,
                maxSizeMB: config.storage.maxFileSizeMB,
                fileName: req.file?.originalname,
              });

              return res.status(400).json({
                error: 'File too large',
                code: 'FILE_TOO_LARGE',
                message: `File size exceeds maximum allowed size of ${config.storage.maxFileSizeMB} MB`,
                maxSizeMB: config.storage.maxFileSizeMB,
              });
            }

            if (error.code === 'LIMIT_FILE_COUNT') {
              logger.error('Too many files uploaded', {
                error: error.message,
              });

              return res.status(400).json({
                error: 'Too many files',
                code: 'LIMIT_FILE_COUNT',
                message: 'Only one file can be uploaded at a time',
              });
            }

            if (error.code === 'LIMIT_UNEXPECTED_FILE') {
              logger.error('Unexpected file field', {
                error: error.message,
                field: error.field,
              });

              return res.status(400).json({
                error: 'Unexpected file field',
                code: 'LIMIT_UNEXPECTED_FILE',
                message: `Unexpected field "${error.field}". Please use "file" field name`,
              });
            }

            // Handle other multer errors
            logger.error('Multer error', {
              code: error.code,
              message: error.message,
            });

            return res.status(400).json({
              error: 'Upload error',
              code: error.code,
              message: error.message,
            });
          }

          // Handle custom file filter errors
          if (error.code === 'INVALID_FILE_TYPE') {
            logger.error('Invalid file type', {
              error: error.message,
            });

            return res.status(400).json({
              error: 'Invalid file type',
              code: 'INVALID_FILE_TYPE',
              message: error.message,
            });
          }

          // Handle unexpected errors during upload
          logger.error('Unexpected upload error', {
            error: error.message,
            stack: error.stack,
          });

          return res.status(500).json({
            error: 'Upload failed',
            code: 'UPLOAD_ERROR',
            message: 'An error occurred during file upload',
          });
        }

        // No error, continue to next middleware
        next();
      });
    };
  },
};

/**
 * Get file path from uploaded file
 * @param {Object} file - Multer file object
 * @returns {string} File path
 */
export function getFilePath(file) {
  return join(config.storage.uploadDir, file.filename);
}

/**
 * Get file ID from request
 * @param {Object} req - Express request object
 * @returns {string} File ID
 */
export function getFileId(req) {
  return req.fileId || (req.file ? req.file.filename.split('_')[0] : null);
}

export default upload;