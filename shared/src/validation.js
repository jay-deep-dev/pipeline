/**
 * Validation Module
 * 
 * Provides file validation utilities for CSV ingestion pipeline.
 * Validates file size, type, and format before processing.
 * 
 * @module validation
 */

import { createReadStream, statSync } from 'fs';
import { extname } from 'path';
import csvParser from 'csv-parser';
import { createLogger } from './logger.js';

const logger = createLogger('validation');

/**
 * Custom error class for validation failures
 */
export class ValidationError extends Error {
  constructor(message, code, details = {}) {
    super(message);
    
    // Maintains proper stack trace for where our error was thrown
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, ValidationError);
    }
    
    this.name = 'ValidationError';
    this.code = code;
    this.details = details;
    
    // Ensure the error is properly recognized as an Error instance
    Object.setPrototypeOf(this, ValidationError.prototype);
  }
}

/**
 * Validate file size
 * @param {string} filePath - Path to the file
 * @param {number} maxSizeMB - Maximum file size in MB
 * @throws {ValidationError} If file size exceeds limit
 */
export function validateFileSize(filePath, maxSizeMB) {
  try {
    const stats = statSync(filePath);
    const fileSizeMB = stats.size / (1024 * 1024);

    if (fileSizeMB === 0) {
      throw new ValidationError(
        'File is empty',
        'EMPTY_FILE',
        { filePath, fileSizeMB }
      );
    }

    if (fileSizeMB > maxSizeMB) {
      throw new ValidationError(
        `File size (${fileSizeMB.toFixed(2)} MB) exceeds maximum allowed size (${maxSizeMB} MB)`,
        'FILE_TOO_LARGE',
        { filePath, fileSizeMB, maxSizeMB }
      );
    }

    logger.debug('File size validation passed', { filePath, fileSizeMB, maxSizeMB });
    return true;
  } catch (error) {
    if (error instanceof ValidationError) {
      throw error;
    }
    throw new ValidationError(
      `Failed to read file: ${error.message}`,
      'FILE_READ_ERROR',
      { filePath, error: error.message }
    );
  }
}

/**
 * Validate file type (extension and MIME type)
 * @param {string} filePath - Path to the file
 * @param {string} originalName - Original filename
 * @param {string} mimeType - MIME type of the file
 * @param {string[]} allowedTypes - Allowed file extensions/types
 * @throws {ValidationError} If file type is not allowed
 */
export function validateFileType(filePath, originalName, mimeType, allowedTypes) {
  const extension = extname(originalName).toLowerCase().replace('.', '');
  
  // Check extension
  const allowedExtensions = allowedTypes.filter(type => !type.includes('/'));
  if (!allowedExtensions.includes(extension) && !allowedExtensions.includes('csv')) {
    throw new ValidationError(
      `File extension '${extension}' is not allowed. Allowed types: ${allowedTypes.join(', ')}`,
      'INVALID_FILE_TYPE',
      { filePath, extension, allowedTypes }
    );
  }

  // Check MIME type if provided
  if (mimeType) {
    const allowedMimeTypes = allowedTypes.filter(type => type.includes('/'));
    if (!allowedMimeTypes.includes(mimeType.toLowerCase()) && 
        !mimeType.toLowerCase().includes('csv') &&
        !mimeType.toLowerCase().includes('text')) {
      logger.warn('MIME type validation warning', { 
        filePath, 
        mimeType, 
        allowedTypes 
      });
      // Don't throw error for MIME type, just warn (some systems don't set it correctly)
    }
  }

  logger.debug('File type validation passed', { filePath, extension, mimeType });
  return true;
}

/**
 * Validate CSV file format (headers and sample rows)
 * Uses streaming to avoid loading entire file into memory
 * 
 * @param {string} filePath - Path to the CSV file
 * @param {string[]} requiredColumns - Required column names
 * @param {number} sampleRows - Number of rows to sample for validation
 * @param {number} maxRows - Maximum rows to validate (if exceeded, throws error)
 * @returns {Promise<Object>} Validation result with headers and row count
 * @throws {ValidationError} If validation fails
 */
export async function validateCsvFormat(filePath, requiredColumns = [], sampleRows = 1000, maxRows = 50000000) {
  return new Promise((resolve, reject) => {
    const headers = [];
    const rows = [];
    let rowCount = 0;
    let hasRejected = false; // ✅ FIXED: Must start as false!

    const stream = createReadStream(filePath)
      .pipe(csvParser())
      .on('headers', (headerList) => {
        if (hasRejected) return;
        
        headers.push(...headerList);
        
        // Check for empty headers
        if (headerList.length === 0) {
          hasRejected = true;
          stream.destroy();
          reject(new ValidationError(
            'CSV file has no headers',
            'NO_HEADERS',
            { filePath }
          ));
          return;
        }
        
        // Validate required columns
        if (requiredColumns.length > 0) {
          const missingColumns = requiredColumns.filter(
            col => !headers.some(h => h.toLowerCase() === col.toLowerCase())
          );
          
          if (missingColumns.length > 0) {
            hasRejected = true;
            stream.destroy();
            reject(new ValidationError(
              `Missing required columns: ${missingColumns.join(', ')}`,
              'MISSING_COLUMNS',
              { filePath, headers, requiredColumns, missingColumns }
            ));
            return;
          }
        }

        logger.debug('CSV headers validated', { filePath, headers, requiredColumns });
      })
      .on('data', (row) => {
        if (hasRejected) return;

        rowCount++;

        // Safety limit to prevent excessive memory usage or malicious files
        if (rowCount > maxRows) {
          hasRejected = true;
          logger.error('Maximum rows exceeded', { filePath, rowCount, maxRows });
          stream.destroy();
          reject(new ValidationError(
            `File exceeds maximum row limit of ${maxRows.toLocaleString()} rows`,
            'TOO_MANY_ROWS',
            { filePath, rowCount, maxRows }
          ));
          return;
        }

        // Sample rows for validation
        if (rows.length < sampleRows) {
          rows.push(row);
        }

        // Basic row validation - check for empty rows or malformed data
        const rowValues = Object.values(row);
        if (rowValues.every(val => !val || val.trim() === '')) {
          // Empty row, skip but don't fail
          return;
        }
      })
      .on('end', () => {
        if (hasRejected) return;

        if (headers.length === 0) {
          hasRejected = true;
          reject(new ValidationError(
            'CSV file has no headers',
            'NO_HEADERS',
            { filePath }
          ));
          return;
        }

        if (rowCount === 0) {
          hasRejected = true;
          reject(new ValidationError(
            'CSV file contains only headers with no data rows',
            'NO_DATA_ROWS',
            { filePath, headers }
          ));
          return;
        }

        logger.info('CSV format validation passed', { 
          filePath, 
          headers, 
          rowCount,
          sampledRows: rows.length 
        });

        resolve({
          headers,
          rowCount,
          sampledRows: rows.length,
          sampleData: rows.slice(0, 10), // Return first 10 rows as sample
        });
      })
      .on('error', (error) => {
        if (hasRejected) return;
        
        hasRejected = true;
        logger.error('CSV parsing error', { filePath, error: error.message });
        reject(new ValidationError(
          `Failed to parse CSV file: ${error.message}`,
          'CSV_PARSE_ERROR',
          { filePath, error: error.message }
        ));
      });
  });
}

/**
 * Comprehensive file validation
 * Validates size, type, and format in sequence
 * 
 * @param {string} filePath - Path to the file
 * @param {string} originalName - Original filename
 * @param {string} mimeType - MIME type
 * @param {Object} config - Validation configuration
 * @returns {Promise<Object>} Validation result
 * @throws {ValidationError} If any validation fails
 */
export async function validateFile(filePath, originalName, mimeType, config) {
  logger.info('Starting file validation', { filePath, originalName });

  // Step 1: Validate file size
  validateFileSize(filePath, config.storage.maxFileSizeMB);

  // Step 2: Validate file type
  validateFileType(filePath, originalName, mimeType, config.storage.allowedFileTypes);

  // Step 3: Validate CSV format
  const formatResult = await validateCsvFormat(
    filePath,
    config.validation.requiredColumns,
    config.validation.sampleRowsForValidation,
    config.validation.maxValidationRows
  );

  logger.info('File validation completed successfully', { 
    filePath, 
    headers: formatResult.headers,
    rowCount: formatResult.rowCount 
  });

  return formatResult;
}