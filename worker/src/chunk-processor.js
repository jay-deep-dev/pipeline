/**
 * Chunk Processor Module (Fixed with Retry Logic)
 * 
 * Processes validated chunk messages with automatic retry on failures.
 * Implements exponential backoff for transient errors.
 */

import { getConfig, createLogger } from '../../shared/index.js';
import { getMongoDBClient } from './mongodb-client.js';

const config = getConfig();
const logger = createLogger('chunk-processor', config.logging.level);

/**
 * Sleep utility for retry delays
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Check if error is retryable
 */
function isRetryableError(error) {
  const retryableErrors = [
    'ENOTFOUND',
    'ECONNREFUSED',
    'ETIMEDOUT',
    'ECONNRESET',
    'EPIPE',
    'MongoNetworkError',
    'MongoTimeoutError',
    'MongoServerError',
  ];

  return retryableErrors.some(code => 
    error.message?.includes(code) || 
    error.name?.includes(code) ||
    error.code === code
  );
}

/**
 * Retry wrapper with exponential backoff
 */
async function retryWithBackoff(fn, options = {}) {
  const {
    maxRetries = 3,
    initialDelay = 1000,
    maxDelay = 30000,
    factor = 2,
    operationName = 'operation',
  } = options;

  let lastError;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      // Don't retry if it's not a retryable error
      if (!isRetryableError(error)) {
        logger.error(`Non-retryable error in ${operationName}`, {
          error: error.message,
          code: error.code,
        });
        throw error;
      }

      // Don't retry on last attempt
      if (attempt === maxRetries) {
        logger.error(`Max retries (${maxRetries}) reached for ${operationName}`, {
          error: error.message,
        });
        throw error;
      }

      // Calculate delay with exponential backoff
      const delay = Math.min(initialDelay * Math.pow(factor, attempt), maxDelay);
      
      logger.warn(`${operationName} failed, retrying in ${delay}ms (attempt ${attempt + 1}/${maxRetries})`, {
        error: error.message,
        attempt: attempt + 1,
        maxRetries,
      });

      await sleep(delay);
    }
  }

  throw lastError;
}

/**
 * Process a validated chunk message with retry logic
 */
export async function processChunk(chunkMessage) {
  const {
    jobId,
    fileName,
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
    fileName,
    fileId,
    chunkId,
    chunkNumber,
    validRowsCount: validRows.length,
    invalidRowsCount: invalidRows.length,
  });

  let validInserted = 0;
  let invalidInserted = 0;

  try {
    // Insert valid rows with retry logic
    if (validRows.length > 0) {
      const validResult = await retryWithBackoff(
        async () => {
          return await mongoClient.insertValidRecords(
            validRows,
            jobId,
            fileName,
            fileId,
            { ordered: false }
          );
        },
        {
          maxRetries: 3,
          initialDelay: 1000,
          operationName: `insertValidRecords (chunk ${chunkNumber})`,
        }
      );
      validInserted = validResult.insertedCount || 0;
    }

    // Insert invalid rows with retry logic
    if (invalidRows.length > 0) {
      const invalidResult = await retryWithBackoff(
        async () => {
          return await mongoClient.insertErrorRecords(
            invalidRows,
            jobId,
            fileName,
            fileId,
            { ordered: false }
          );
        },
        {
          maxRetries: 3,
          initialDelay: 1000,
          operationName: `insertErrorRecords (chunk ${chunkNumber})`,
        }
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
    const processingTime = Date.now() - startTime;
    
    logger.error('Failed to process chunk after retries', {
      jobId,
      fileId,
      chunkId,
      chunkNumber,
      error: error.message,
      stack: error.stack,
      processingTimeMs: processingTime,
    });

    // Re-throw to send to DLQ
    throw error;
  }
}

/**
 * Process chunk with rate limiting (optional enhancement)
 * Use this if you want to add artificial delays between chunks
 */
export async function processChunkWithRateLimit(chunkMessage, rateLimitMs = 0) {
  if (rateLimitMs > 0) {
    await sleep(rateLimitMs);
  }
  return processChunk(chunkMessage);
}