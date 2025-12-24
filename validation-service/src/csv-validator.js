/**
 * CSV Validator Module
 * 
 * Streams CSV files, validates rows, and chunks them into batches.
 * Produces validated-chunk messages to Kafka.
 * 
 * @module csv-validator
 */

import { createReadStream } from 'fs';
import { access } from 'fs/promises';
import csvParser from 'csv-parser';
import { v4 as uuidv4 } from 'uuid';
import { getConfig, createLogger } from '../../shared/index.js';
import { validateBatch } from '../../shared/src/row-validation.js';
import { createValidatedChunkMessage } from '../../shared/src/kafka-schemas.js';

const config = getConfig();
const logger = createLogger('csv-validator', config.logging.level);

/**
 * Check if file exists
 * @param {string} filePath - Path to file
 * @returns {Promise<boolean>} True if file exists
 */
async function fileExists(filePath) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

/**
 * Validate and chunk CSV file
 * Streams the file, validates rows, chunks them, and produces Kafka messages
 * 
 * @param {Object} fileMessage - File ingestion message
 * @param {string} fileMessage.jobId - Job identifier
 * @param {string} fileMessage.fileId - File identifier
 * @param {string} fileMessage.filePath - Path to CSV file
 * @param {string[]} fileMessage.headers - CSV column headers
 * @param {number} fileMessage.estimatedRowCount - Estimated row count
 * @param {Object} producer - Kafka producer instance
 * @returns {Promise<Object>} Processing result with statistics
 */
export async function validateAndChunkFile(fileMessage, producer) {
  const {
    jobId,
    fileId,
    filePath,
    estimatedRowCount,
  } = fileMessage;

  const startTime = Date.now();
  let totalRowsProcessed = 0;
  let totalValidRows = 0;
  let totalInvalidRows = 0;
  let chunkNumber = 0;
  let currentChunk = [];
  let rowNumber = 1; // 1-indexed (excluding header)

  logger.info('Starting CSV validation and chunking', {
    jobId,
    fileId,
    filePath,
    estimatedRowCount,
    chunkSize: config.validation.chunkSize,
  });

  // Check if file exists
  const exists = await fileExists(filePath);
  if (!exists) {
    throw new Error(`File not found: ${filePath}`);
  }

  // Calculate total chunks (approximate)
  const totalChunks = Math.ceil(estimatedRowCount / config.validation.chunkSize);

  // Stream CSV file
  const stream = createReadStream(filePath).pipe(csvParser());

  for await (const row of stream) {
    currentChunk.push(row);
    totalRowsProcessed++;

    // When chunk reaches target size, validate and produce
    if (currentChunk.length >= config.validation.chunkSize) {
      chunkNumber++;

      // Validate batch
      const { validRows, invalidRows } = validateBatch(
        currentChunk,
        rowNumber - currentChunk.length + 1,
        jobId,
        fileId
      );

      totalValidRows += validRows.length;
      totalInvalidRows += invalidRows.length;

      // Create validated chunk message
      const chunkId = uuidv4();
      const chunkMessage = createValidatedChunkMessage({
        jobId,
        fileId,
        chunkId,
        chunkNumber,
        totalChunks,
        validRows,
        invalidRows,
        startRowNumber: rowNumber - currentChunk.length + 1,
        endRowNumber: rowNumber,
      });

      // Produce to validated-chunks topic
      await producer.send({
        topic: config.kafka.validatedChunksTopic,
        messages: [
          {
            key: `${fileId}-${chunkNumber}`, // Partition key for ordering
            value: JSON.stringify(chunkMessage),
            headers: {
              'content-type': 'application/json',
              'job-id': jobId,
              'file-id': fileId,
              'chunk-id': chunkId,
            },
          },
        ],
      });

      logger.debug('Validated chunk produced', {
        jobId,
        fileId,
        chunkNumber,
        totalChunks,
        validRows: validRows.length,
        invalidRows: invalidRows.length,
        startRowNumber: rowNumber - currentChunk.length + 1,
        endRowNumber: rowNumber,
      });

      // Clear chunk
      currentChunk = [];
    }

    rowNumber++;
  }

  // Handle final chunk
  if (currentChunk.length > 0) {
    chunkNumber++;

    // Validate batch
    const { validRows, invalidRows } = validateBatch(
      currentChunk,
      rowNumber - currentChunk.length + 1,
      jobId,
      fileId
    );

    totalValidRows += validRows.length;
    totalInvalidRows += invalidRows.length;

    // Create validated chunk message
    const chunkId = uuidv4();
    const chunkMessage = createValidatedChunkMessage({
      jobId,
      fileId,
      chunkId,
      chunkNumber,
      totalChunks: chunkNumber, // Final chunk count
      validRows,
      invalidRows,
      startRowNumber: rowNumber - currentChunk.length + 1,
      endRowNumber: rowNumber - 1,
    });

    // Produce to validated-chunks topic
    await producer.send({
      topic: config.kafka.validatedChunksTopic,
      messages: [
        {
          key: `${fileId}-${chunkNumber}`,
          value: JSON.stringify(chunkMessage),
          headers: {
            'content-type': 'application/json',
            'job-id': jobId,
            'file-id': fileId,
            'chunk-id': chunkId,
          },
        },
      ],
    });

    logger.debug('Final validated chunk produced', {
      jobId,
      fileId,
      chunkNumber,
      validRows: validRows.length,
      invalidRows: invalidRows.length,
    });
  }

  const processingTime = Date.now() - startTime;

  logger.info('CSV validation and chunking completed', {
    jobId,
    fileId,
    filePath,
    totalRowsProcessed,
    totalValidRows,
    totalInvalidRows,
    totalChunks: chunkNumber,
    processingTimeMs: processingTime,
    rowsPerSecond: Math.round((totalRowsProcessed / processingTime) * 1000),
    validityRate: ((totalValidRows / totalRowsProcessed) * 100).toFixed(2) + '%',
  });

  return {
    success: true,
    totalRowsProcessed,
    totalValidRows,
    totalInvalidRows,
    totalChunks: chunkNumber,
    processingTimeMs: processingTime,
  };
}

