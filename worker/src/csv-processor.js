/**
 * CSV Processor Module
 * 
 * Streams and processes CSV files in batches for efficient memory usage.
 * Reads CSV files using streaming to avoid loading entire files into memory.
 * 
 * @module csv-processor
 */

import { createReadStream } from 'fs';
import csvParser from 'csv-parser';
import { getConfig, createLogger } from '../../shared/index.js';
import { getMongoDBClient } from './mongodb-client.js';

const config = getConfig();
const logger = createLogger('csv-processor', config.logging.level);

/**
 * Process CSV file and insert into MongoDB in batches
 * Uses streaming to handle large files efficiently
 * 
 * @param {Object} jobMessage - Kafka job message
 * @param {string} jobMessage.jobId - Job identifier
 * @param {string} jobMessage.fileId - File identifier
 * @param {string} jobMessage.filePath - Path to CSV file
 * @param {string[]} jobMessage.headers - CSV column headers
 * @param {number} jobMessage.estimatedRowCount - Estimated row count
 * @returns {Promise<Object>} Processing result with statistics
 */
export async function processCsvFile(jobMessage) {
  const {
    jobId,
    fileId,
    filePath,
    estimatedRowCount,
  } = jobMessage;

  const startTime = Date.now();
  let totalRowsProcessed = 0;
  let totalRowsInserted = 0;
  let batch = [];
  let batchNumber = 0;

  const mongoClient = getMongoDBClient();

  logger.info('Starting CSV file processing', {
    jobId,
    fileId,
    filePath,
    estimatedRowCount,
  });

  const stream = createReadStream(filePath).pipe(csvParser());

  for await (const row of stream) {
    totalRowsProcessed++;
    batch.push(row);

    if (batch.length >= config.worker.batchSize) {
      batchNumber++;

      const insertResult = await mongoClient.insertBatch(
        batch,
        jobId,
        fileId,
        { ordered: false }
      );

      totalRowsInserted += insertResult.insertedCount || 0;
      batch = [];

    }
  }

  // final batch
  if (batch.length > 0) {
    batchNumber++;

    const insertResult = await mongoClient.insertBatch(
      batch,
      jobId,
      fileId,
      { ordered: false }
    );

    totalRowsInserted += insertResult.insertedCount || 0;
  }

  const processingTime = Date.now() - startTime;

  logger.info('CSV file processing completed', {
    jobId,
    fileId,
    filePath,
    totalRowsProcessed,
    totalRowsInserted,
    batchesProcessed: batchNumber,
    processingTimeMs: processingTime,
    rowsPerSecond: Math.round((totalRowsProcessed / processingTime) * 1000),
  });

  return {
    success: true,
    totalRowsProcessed,
    totalRowsInserted,
    batchesProcessed: batchNumber,
  };
}


/**
 * Check if file exists before processing
 * @param {string} filePath - Path to file
 * @returns {Promise<boolean>} True if file exists
 */
export async function fileExists(filePath) {
  try {
    const { access } = await import('fs/promises');
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

