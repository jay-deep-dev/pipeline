/**
 * Kafka Consumer Module
 * 
 * Consumes ingestion job messages from Kafka and processes CSV files.
 * Handles message consumption, processing, and error handling with DLQ support.
 * 
 * @module kafka-consumer
 */

import { Kafka } from 'kafkajs';
import { getConfig, createLogger, validateIngestionJobMessage } from '../../shared/index.js';
import { processCsvFile, fileExists } from './csv-processor.js';
import { createDLQMessage } from '../../shared/src/kafka-schemas.js';

const config = getConfig();
const logger = createLogger('kafka-consumer', config.logging.level);
import os from 'os';

/**
 * Kafka Consumer Service
 * Manages Kafka consumption and message processing
 */
class KafkaConsumer {
  constructor() {
    this.kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      retry: {
        retries: 5,
        initialRetryTime: 100,
        multiplier: 2,
        maxRetryTime: 30000,
      },
    });
    this.consumer = null;
    this.producer = null; // For DLQ messages
    this.isRunning = false;
    this.processingJobs = new Set(); // Track currently processing jobs
  }

  /**
   * Initialize Kafka consumer and producer (for DLQ)
   * @returns {Promise<void>}
   */
  async connect() {
    try {
      // Create consumer
      this.consumer = this.kafka.consumer({
        groupId: config.kafka.consumerGroupId,
        maxPollIntervalMs: 600000,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        maxBytesPerPartition: 10485760, // 10MB
        minBytes: 1,
        maxBytes: 10485760,
        maxWaitTimeInMs: config.worker.pollIntervalMs,
      });

      await this.consumer.connect();
      logger.info('Kafka consumer connected', {
        brokers: config.kafka.brokers,
        groupId: config.kafka.consumerGroupId,
        topic: config.kafka.ingestionTopic,
      });

      // Create producer for DLQ messages
      this.producer = this.kafka.producer({
        maxInFlightRequests: 5,
        idempotent: true,
      });

      await this.producer.connect();
      logger.info('Kafka producer (DLQ) connected');

      // Subscribe to ingestion topic
      await this.consumer.subscribe({
        topic: config.kafka.ingestionTopic,
        fromBeginning: false, // Only consume new messages
      });

      logger.info('Subscribed to Kafka topic', {
        topic: config.kafka.ingestionTopic,
      });
    } catch (error) {
      logger.error('Failed to connect Kafka consumer', {
        error: error.message,
        brokers: config.kafka.brokers,
      });
      throw error;
    }
  }

  /**
   * Disconnect Kafka consumer and producer
   * @returns {Promise<void>}
   */
  async disconnect() {
    try {
      this.isRunning = false;

      if (this.consumer) {
        await this.consumer.disconnect();
        logger.info('Kafka consumer disconnected');
      }

      if (this.producer) {
        await this.producer.disconnect();
        logger.info('Kafka producer (DLQ) disconnected');
      }
    } catch (error) {
      logger.error('Error disconnecting Kafka consumer', {
        error: error.message,
      });
    }
  }

  /**
   * Send message to Dead Letter Queue
   * @param {Object} jobMessage - Original job message
   * @param {Error} error - Error that occurred
   * @param {number} retryCount - Number of retry attempts
   * @returns {Promise<void>}
   */
  async sendToDLQ(jobMessage, error, retryCount = 0) {
    try {
      const dlqMessage = createDLQMessage({
        jobId: jobMessage.jobId,
        fileId: jobMessage.fileId,
        fileName: jobMessage.fileName,
        filePath: jobMessage.filePath,
        errorCode: error.code || 'PROCESSING_ERROR',
        errorMessage: error.message,
        errorDetails: {
          stack: error.stack,
          ...error.details,
        },
        retryCount,
        originalMessage: jobMessage,
      });

      await this.producer.send({
        topic: config.kafka.dlqTopic,
        messages: [
          {
            key: jobMessage.fileId,
            value: JSON.stringify(dlqMessage),
            headers: {
              'content-type': 'application/json',
              'error-code': dlqMessage.errorCode,
            },
          },
        ],
      });

      logger.info('Message sent to DLQ', {
        jobId: jobMessage.jobId,
        fileId: jobMessage.fileId,
        errorCode: dlqMessage.errorCode,
        retryCount,
      });
    } catch (dlqError) {
      logger.error('Failed to send message to DLQ', {
        jobId: jobMessage.jobId,
        error: dlqError.message,
      });
      // Don't throw - we've already failed, just log the DLQ failure
    }
  }

  /**
   * Process a single ingestion job message
   * @param {Object} message - Kafka message
   * @returns {Promise<void>}
   */
  async processMessage(message) {
    let jobMessage = null;

    try {
      // Parse message
      const messageValue = JSON.parse(message.value.toString());
      jobMessage = messageValue;

      const WORKER_ID = `${os.hostname()}-${process.pid}`;

      // Log the worker, partition, offset, and jobId
      logger.info('Consuming Kafka message', {
        workerId: WORKER_ID,
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
        jobId: jobMessage.jobId,
        fileId: jobMessage.fileId,
      });

      // Validate message structure
      validateIngestionJobMessage(jobMessage);

      // Check if job is already being processed (idempotency)
      if (this.processingJobs.has(jobMessage.jobId)) {
        logger.warn('Job already being processed, skipping', {
          jobId: jobMessage.jobId,
          fileId: jobMessage.fileId,
        });
        return;
      }

      this.processingJobs.add(jobMessage.jobId);

      logger.info('Processing ingestion job', {
        jobId: jobMessage.jobId,
        fileId: jobMessage.fileId,
        fileName: jobMessage.fileName,
        estimatedRowCount: jobMessage.estimatedRowCount,
      });

      // Check if file exists
      const exists = await fileExists(jobMessage.filePath);
      if (!exists) {
        throw new Error(
          `File not found: ${jobMessage.filePath}`
        );
      }

      // Process CSV file
      const result = await processCsvFile(jobMessage);

      if (result.success) {
        logger.info('Job processed successfully', {
          jobId: jobMessage.jobId,
          fileId: jobMessage.fileId,
          totalRowsProcessed: result.totalRowsProcessed,
          totalRowsInserted: result.totalRowsInserted,
          processingTimeMs: result.processingTimeMs,
          rowsPerSecond: result.rowsPerSecond,
        });
      } else {
        throw new Error(
          `Processing completed with errors: ${result.errorsCount} errors occurred`
        );
      }
    } catch (error) {
      logger.error('Failed to process ingestion job', {
        jobId: jobMessage?.jobId,
        fileId: jobMessage?.fileId,
        error: error.message,
        stack: error.stack,
      });

      // Send to DLQ if we have a valid job message
      if (jobMessage) {
        await this.sendToDLQ(jobMessage, error, 0);
      }
    } finally {
      // Remove from processing set
      if (jobMessage) {
        this.processingJobs.delete(jobMessage.jobId);
      }
    }
  }

  /**
   * Start consuming messages from Kafka
   * @returns {Promise<void>}
   */
  async start() {
    if (this.isRunning) {
      logger.warn('Consumer is already running');
      return;
    }

    this.isRunning = true;

    try {
      await this.consumer.run({
        autoCommit: false,

        eachBatch: async ({
          batch,
          resolveOffset,
          heartbeat,
          commitOffsetsIfNecessary,
          isRunning,
          isStale,
        }) => {
          for (const message of batch.messages) {
            if (!isRunning() || isStale()) break;

            logger.debug('Received Kafka message', {
              topic: batch.topic,
              partition: batch.partition,
              offset: message.offset,
            });

            try {
              await this.processMessage(message);

              resolveOffset(message.offset); // mark as processed
              await heartbeat();
            } catch (error) {
              logger.error('Error processing Kafka message', {
                topic: batch.topic,
                partition: batch.partition,
                offset: message.offset,
                error: error.message,
              });

              throw error; // ❗ DO NOT commit offset
            }
          }

          await commitOffsetsIfNecessary(); // commit only after success
        },
      });


      logger.info('Kafka consumer started');
    } catch (error) {
      this.isRunning = false;
      logger.error('Failed to start Kafka consumer', {
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Stop consuming messages
   * @returns {Promise<void>}
   */
  async stop() {
    this.isRunning = false;
    logger.info('Stopping Kafka consumer...');
    // Consumer will stop on next iteration
  }
}

// Singleton instance
let consumerInstance = null;

/**
 * Get or create Kafka consumer instance
 * @returns {KafkaConsumer} Consumer instance
 */
export function getKafkaConsumer() {
  if (!consumerInstance) {
    consumerInstance = new KafkaConsumer();
  }
  return consumerInstance;
}

/**
 * Get Kafka producer instance (for DLQ)
 * @returns {Object} Producer instance
 */
export function getKafkaProducer() {
  if (!consumerInstance || !consumerInstance.producer) {
    throw new Error('Kafka producer not initialized');
  }
  return consumerInstance.producer;
}

export default getKafkaConsumer;

