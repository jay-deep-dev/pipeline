/**
 * Kafka Producer Module
 * 
 * Handles Kafka message production for ingestion jobs.
 * Produces messages to the csv.ingestion topic after successful validation.
 * 
 * @module kafka-producer
 */

import { Kafka } from 'kafkajs';
import { getConfig, createLogger } from '../../shared/index.js';
import { createFileIngestionMessage } from '../../shared/src/kafka-schemas.js';

const config = getConfig();
const logger = createLogger('kafka-producer', config.logging.level);

/**
 * Kafka Producer Service
 * Manages Kafka connection and message production
 */
class KafkaProducer {
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
    this.producer = null;
  }

  /**
   * Initialize and connect Kafka producer
   * @returns {Promise<void>}
   */
  async connect() {
    try {
      this.producer = this.kafka.producer({
        maxInFlightRequests: 5,
        idempotent: true,
        transactionTimeout: 30000,
      });

      await this.producer.connect();
      logger.info('Kafka producer connected', {
        brokers: config.kafka.brokers,
        topic: config.kafka.fileIngestionTopic,
      });
    } catch (error) {
      logger.error('Failed to connect Kafka producer', {
        error: error.message,
        brokers: config.kafka.brokers,
      });
      throw error;
    }
  }

  /**
   * Disconnect Kafka producer
   * @returns {Promise<void>}
   */
  async disconnect() {
    try {
      if (this.producer) {
        await this.producer.disconnect();
        logger.info('Kafka producer disconnected');
      }
    } catch (error) {
      logger.error('Error disconnecting Kafka producer', {
        error: error.message,
      });
    }
  }

  /**
   * Produce ingestion job message to Kafka
   * 
   * @param {Object} jobData - Job data
   * @param {string} jobData.jobId - Unique job identifier
   * @param {string} jobData.fileId - Unique file identifier
   * @param {string} jobData.fileName - Original filename
   * @param {string} jobData.filePath - Path to uploaded file
   * @param {number} jobData.fileSizeBytes - File size in bytes
   * @param {string[]} jobData.headers - CSV column headers
   * @param {number} jobData.estimatedRowCount - Estimated row count
   * @param {Object} [jobData.metadata] - Additional metadata
   * @returns {Promise<void>}
   */
  async produceIngestionJob(jobData) {
    if (!this.producer) {
      throw new Error('Kafka producer not connected');
    }

    try {
      const message = createFileIngestionMessage(jobData);

      // Use fileId as partition key for consistent partitioning
      // This ensures all messages for the same file go to the same partition
      await this.producer.send({
        topic: config.kafka.fileIngestionTopic,
        messages: [
          {
            key: jobData.fileId,
            value: JSON.stringify(message),
            headers: {
              'content-type': 'application/json',
              'job-id': jobData.jobId,
              'file-id': jobData.fileId,
            },
          },
        ],
      });

      logger.info('File ingestion message produced to Kafka', {
        jobId: jobData.jobId,
        fileId: jobData.fileId,
        fileName: jobData.fileName,
        topic: config.kafka.fileIngestionTopic,
      });
    } catch (error) {
      logger.error('Failed to produce ingestion job message', {
        error: error.message,
        jobId: jobData.jobId,
        fileId: jobData.fileId,
      });
      throw error;
    }
  }

  /**
   * Produce DLQ message for failed jobs
   * 
   * @param {Object} dlqData - DLQ message data
   * @returns {Promise<void>}
   */
  async produceDLQMessage(dlqData) {
    if (!this.producer) {
      throw new Error('Kafka producer not connected');
    }

    try {
      const { createDLQMessage } = await import('../../shared/src/kafka-schemas.js');
      const message = createDLQMessage(dlqData);

      await this.producer.send({
        topic: config.kafka.fileIngestionDlqTopic,
        messages: [
          {
            key: dlqData.fileId,
            value: JSON.stringify(message),
            headers: {
              'content-type': 'application/json',
              'error-code': dlqData.errorCode,
            },
          },
        ],
      });

      logger.info('DLQ message produced', {
        jobId: dlqData.jobId,
        fileId: dlqData.fileId,
        errorCode: dlqData.errorCode,
      });
    } catch (error) {
      logger.error('Failed to produce DLQ message', {
        error: error.message,
        jobId: dlqData.jobId,
      });
      throw error;
    }
  }
}

// Singleton instance
let producerInstance = null;

/**
 * Get or create Kafka producer instance
 * @returns {KafkaProducer} Producer instance
 */
export function getKafkaProducer() {
  if (!producerInstance) {
    producerInstance = new KafkaProducer();
  }
  return producerInstance;
}

export default getKafkaProducer;

