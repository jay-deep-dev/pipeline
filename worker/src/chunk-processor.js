/**
 * Chunk Processor Module
 * 
 * Processes validated chunk messages and inserts them into MongoDB.
 * Handles valid rows (csv_records) and invalid rows (error_records).
 * 
 * @module chunk-processor
 */

import { getConfig, createLogger } from '../../shared/index.js';
import { getMongoDBClient } from './mongodb-client.js';

const config = getConfig();
const logger = createLogger('chunk-processor', config.logging.level);

/**
 * Process a validated chunk message
 * Inserts valid rows to csv_records and invalid rows to error_records
 * 
 * @param {Object} chunkMessage - Validated chunk message
 * @param {string} chunkMessage.jobId - Job identifier
 * @param {string} chunkMessage.fileId - File identifier
 * @param {string} chunkMessage.chunkId - Chunk identifier
 * @param {number} chunkMessage.chunkNumber - Chunk sequence number
 * @param {Object[]} chunkMessage.validRows - Array of valid rows
 * @param {Object[]} chunkMessage.invalidRows - Array of invalid rows
 * @returns {Promise<Object>} Processing result with statistics
 */
export async function processChunk(chunkMessage) {
  const {
    jobId,
    fileId,
    chunkId,
    chunkNumber,
    validRows,
    invalidRows,
  } = chunkMessage;

  const startTime = Date.now();
  const mongoClient = getMongoDBClient();

  logger.info('Processing validated chunk', {
    jobId,
    fileId,
    chunkId,
    chunkNumber,
    validRowsCount: validRows.length,
    invalidRowsCount: invalidRows.length,
  });

  let validInserted = 0;
  let invalidInserted = 0;

  try {
    // Insert valid rows to csv_records
    if (validRows.length > 0) {
      const validResult = await mongoClient.insertValidRecords(
        validRows,
        jobId,
        fileId,
        { ordered: false }
      );
      validInserted = validResult.insertedCount || 0;
    }

    // Insert invalid rows to error_records
    if (invalidRows.length > 0) {
      const invalidResult = await mongoClient.insertErrorRecords(
        invalidRows,
        jobId,
        fileId,
        { ordered: false }
      );
      invalidInserted = invalidResult.insertedCount || 0;
    }

    const processingTime = Date.now() - startTime;

    logger.info('Chunk processed successfully', {
      jobId,
      fileId,
      chunkId,
      chunkNumber,
      validInserted,
      invalidInserted,
      processingTimeMs: processingTime,
    });

    return {
      success: true,
      validInserted,
      invalidInserted,
      processingTimeMs: processingTime,
    };
  } catch (error) {
    logger.error('Failed to process chunk', {
      jobId,
      fileId,
      chunkId,
      chunkNumber,
      error: error.message,
      stack: error.stack,
    });

    throw error;
  }
}

