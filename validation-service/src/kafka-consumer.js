/**
 * Kafka Consumer Module for Validation Service
 * 
 * Consumes file-ingestion messages from Kafka, validates CSV rows,
 * chunks them, and produces validated-chunks messages.
 * 
 * @module kafka-consumer
 */

import { Kafka } from 'kafkajs';
import { getConfig, createLogger, validateFileIngestionMessage } from '../../shared/index.js';
import { createValidatedChunkMessage, createDLQMessage } from '../../shared/src/kafka-schemas.js';
import { validateAndChunkFile } from './csv-validator.js';

const config = getConfig();
const logger = createLogger('validation-consumer', config.logging.level);

/**
 * Kafka Consumer Service for Validation
 */
class ValidationKafkaConsumer {
  constructor() {
    this.kafka = new Kafka({
      clientId: `${config.kafka.clientId}-validation`,
      brokers: config.kafka.brokers,
      retry: {
        retries: 5,
        initialRetryTime: 100,
        multiplier: 2,
        maxRetryTime: 30000,
      },
    });
    this.consumer = null;
    this.producer = null; // For validated-chunks and DLQ
    this.isRunning = false;
    this.processingFiles = new Set(); // Track currently processing files
  }

  /**
   * Initialize Kafka consumer and producer
   * @returns {Promise<void>}
   */
  async connect() {
    try {
      // Create consumer
      this.consumer = this.kafka.consumer({
        groupId: config.kafka.validationConsumerGroupId,
        maxPollIntervalMs: 600000, // 10 minutes for large files
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        maxBytesPerPartition: 10485760, // 10MB
        minBytes: 1,
        maxBytes: 10485760,
        maxWaitTimeInMs: config.validation.pollIntervalMs,
      });

      await this.consumer.connect();
      logger.info('Kafka consumer connected', {
        brokers: config.kafka.brokers,
        groupId: config.kafka.validationConsumerGroupId,
        topic: config.kafka.fileIngestionTopic,
      });

      // Create producer for validated-chunks and DLQ
      this.producer = this.kafka.producer({
        maxInFlightRequests: 5,
        idempotent: true,
        transactionTimeout: 30000,
        acks: -1,
      });

      await this.producer.connect();
      logger.info('Kafka producer (validated-chunks) connected');

      // Subscribe to file-ingestion topic
      await this.consumer.subscribe({
        topic: config.kafka.fileIngestionTopic,
        fromBeginning: false, // Only consume new messages
      });

      logger.info('Subscribed to Kafka topic', {
        topic: config.kafka.fileIngestionTopic,
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
        logger.info('Kafka producer disconnected');
      }
    } catch (error) {
      logger.error('Error disconnecting Kafka consumer', {
        error: error.message,
      });
    }
  }

  /**
   * Send message to Dead Letter Queue
   * @param {Object} fileMessage - Original file message
   * @param {Error} error - Error that occurred
   * @param {number} retryCount - Number of retry attempts
   * @returns {Promise<void>}
   */
  async sendToDLQ(fileMessage, error, retryCount = 0) {
    try {
      const dlqMessage = createDLQMessage({
        jobId: fileMessage.jobId,
        fileId: fileMessage.fileId,
        fileName: fileMessage.fileName,
        filePath: fileMessage.filePath,
        errorCode: error.code || 'VALIDATION_ERROR',
        errorMessage: error.message,
        errorDetails: {
          stack: error.stack,
          ...error.details,
        },
        retryCount,
        originalMessage: fileMessage,
      });

      await this.producer.send({
        topic: config.kafka.fileIngestionDlqTopic,
        messages: [
          {
            key: fileMessage.fileId,
            value: JSON.stringify(dlqMessage),
            headers: {
              'content-type': 'application/json',
              'error-code': dlqMessage.errorCode,
            },
          },
        ],
      });

      logger.info('Message sent to DLQ', {
        jobId: fileMessage.jobId,
        fileId: fileMessage.fileId,
        errorCode: dlqMessage.errorCode,
        retryCount,
      });
    } catch (dlqError) {
      logger.error('Failed to send message to DLQ', {
        jobId: fileMessage.jobId,
        error: dlqError.message,
      });
    }
  }

  /**
   * Process a single file ingestion message
   * @param {Object} message - Kafka message
   * @returns {Promise<void>}
   */
  async processMessage(message) {
    let fileMessage = null;

    try {
      // Parse message
      const messageValue = JSON.parse(message.value.toString());
      fileMessage = messageValue;

      logger.info('Consuming file ingestion message', {
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
        jobId: fileMessage.jobId,
        fileId: fileMessage.fileId,
      });

      // Validate message structure
      validateFileIngestionMessage(fileMessage);

      // Check if file is already being processed (idempotency)
      if (this.processingFiles.has(fileMessage.fileId)) {
        logger.warn('File already being processed, skipping', {
          jobId: fileMessage.jobId,
          fileId: fileMessage.fileId,
        });
        return;
      }

      this.processingFiles.add(fileMessage.fileId);

      logger.info('Processing file for validation', {
        jobId: fileMessage.jobId,
        fileId: fileMessage.fileId,
        fileName: fileMessage.fileName,
        estimatedRowCount: fileMessage.estimatedRowCount,
      });

      // Validate and chunk CSV file
      await validateAndChunkFile(fileMessage, this.producer);

      logger.info('File validation and chunking completed', {
        jobId: fileMessage.jobId,
        fileId: fileMessage.fileId,
      });
    } catch (error) {
      logger.error('Failed to process file ingestion message', {
        jobId: fileMessage?.jobId,
        fileId: fileMessage?.fileId,
        error: error.message,
        stack: error.stack,
      });

      // Send to DLQ if we have a valid file message
      if (fileMessage) {
        await this.sendToDLQ(fileMessage, error, 0);
      }
    } finally {
      // Remove from processing set
      if (fileMessage) {
        this.processingFiles.delete(fileMessage.fileId);
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
  }
}

// Singleton instance
let consumerInstance = null;

/**
 * Get or create Kafka consumer instance
 * @returns {ValidationKafkaConsumer} Consumer instance
 */
export function getKafkaConsumer() {
  if (!consumerInstance) {
    consumerInstance = new ValidationKafkaConsumer();
  }
  return consumerInstance;
}

export default getKafkaConsumer;

