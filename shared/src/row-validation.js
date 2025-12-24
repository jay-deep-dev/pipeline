/**
 * Row Validation Module
 * 
 * Validates individual CSV rows based on schema requirements.
 * Used by validation service to validate rows before chunking.
 * 
 * @module row-validation
 */

import { createLogger } from './logger.js';

const logger = createLogger('row-validation');

/**
 * Validation result for a single row
 * @typedef {Object} RowValidationResult
 * @property {boolean} isValid - Whether the row is valid
 * @property {string|null} errorMessage - Error message if invalid
 * @property {Object} rowData - The validated/enriched row data
 */

/**
 * Validate email format
 * @param {string} email - Email to validate
 * @returns {boolean} True if valid email format
 */
function isValidEmail(email) {
  if (!email || typeof email !== 'string') {
    return false;
  }
  
  // RFC 5322 compliant regex (simplified)
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email.trim());
}

/**
 * Validate ISO 8601 datetime format
 * @param {string} dateTime - DateTime string to validate
 * @returns {boolean} True if valid ISO 8601 format
 */
function isValidDateTime(dateTime) {
  if (!dateTime || typeof dateTime !== 'string') {
    return false;
  }
  
  // ISO 8601 format: YYYY-MM-DDTHH:mm:ss.sssZ or variations
  const iso8601Regex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/;
  
  if (!iso8601Regex.test(dateTime.trim())) {
    return false;
  }
  
  // Try to parse as Date
  const date = new Date(dateTime);
  return !isNaN(date.getTime());
}

/**
 * Validate a single CSV row
 * Based on schema: id, name, email, created_at
 * 
 * @param {Object} row - CSV row object
 * @param {number} rowNumber - Row number (1-indexed, excluding header)
 * @param {string} jobId - Job identifier
 * @param {string} fileId - File identifier
 * @returns {RowValidationResult} Validation result
 */
export function validateRow(row, rowNumber, jobId, fileId) {
  const errors = [];
  
  // Validate id
  if (!row.id || (typeof row.id !== 'string' && typeof row.id !== 'number')) {
    errors.push('id is required and must be a string or number');
  }
  
  // Validate name
  if (!row.name || typeof row.name !== 'string' || row.name.trim().length === 0) {
    errors.push('name is required and must be a non-empty string');
  } else if (row.name.trim().length > 255) {
    errors.push('name exceeds maximum length of 255 characters');
  }
  
  // Validate email
  if (!row.email || typeof row.email !== 'string') {
    errors.push('email is required and must be a string');
  } else if (!isValidEmail(row.email)) {
    errors.push(`email validation failed: invalid format (${row.email})`);
  }
  
  // Validate created_at
  if (!row.created_at || typeof row.created_at !== 'string') {
    errors.push('created_at is required and must be a string');
  } else if (!isValidDateTime(row.created_at)) {
    errors.push(`created_at validation failed: invalid ISO 8601 format (${row.created_at})`);
  }
  
  // If there are errors, return invalid result
  if (errors.length > 0) {
    return {
      isValid: false,
      errorMessage: errors.join('; '),
      rowData: {
        ...row,
        jobId,
        fileId,
        rowNumber,
      },
    };
  }
  
  // Return valid result with enriched data
  return {
    isValid: true,
    errorMessage: null,
    rowData: {
      id: String(row.id).trim(),
      name: row.name.trim(),
      email: row.email.trim().toLowerCase(),
      created_at: row.created_at.trim(),
      jobId,
      fileId,
      rowNumber,
      insertedAt: new Date(),
    },
  };
}

/**
 * Validate a batch of rows
 * Separates valid and invalid rows
 * 
 * @param {Object[]} rows - Array of CSV row objects
 * @param {number} startRowNumber - Starting row number (1-indexed)
 * @param {string} jobId - Job identifier
 * @param {string} fileId - File identifier
 * @returns {Object} Object with validRows and invalidRows arrays
 */
export function validateBatch(rows, startRowNumber, jobId, fileId) {
  const validRows = [];
  const invalidRows = [];
  
  rows.forEach((row, index) => {
    const rowNumber = startRowNumber + index;
    const result = validateRow(row, rowNumber, jobId, fileId);
    
    if (result.isValid) {
      validRows.push(result.rowData);
    } else {
      invalidRows.push({
        jobId,
        fileId,
        rowNumber,
        rowData: row,
        errorMessage: result.errorMessage,
        createdAt: new Date(),
      });
    }
  });
  
  logger.debug('Batch validation completed', {
    jobId,
    fileId,
    totalRows: rows.length,
    validRows: validRows.length,
    invalidRows: invalidRows.length,
  });
  
  return {
    validRows,
    invalidRows,
  };
}

/**
 * Get validation statistics
 * @param {Object[]} validRows - Array of valid rows
 * @param {Object[]} invalidRows - Array of invalid rows
 * @returns {Object} Statistics object
 */
export function getValidationStats(validRows, invalidRows) {
  return {
    total: validRows.length + invalidRows.length,
    valid: validRows.length,
    invalid: invalidRows.length,
    validityRate: (validRows.length / (validRows.length + invalidRows.length || 1)) * 100,
  };
}

