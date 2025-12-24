/**
 * Dead Letter Queue Handler
 * 
 * Separate service to handle messages from the Dead Letter Queue (DLQ).
 * Processes failed jobs, logs errors, and stores them in MongoDB for analysis.
 * 
 * @module dlq-handler
 */

import { Kafka } from 'kafkajs';
import { getConfig, createLogger } from '../../shared/index.js';
import { ensureKafkaTopics } from '../../shared/src/kafka-admin.js';
import { getMongoDBClient } from './mongodb-client.js';

const config = getConfig();
const logger = createLogger('dlq-handler', config.logging.level);

/**
 * DLQ Handler Service
 * Consumes messages from DLQ and stores them in MongoDB
 */
class DLQHandler {
  constructor() {
    this.kafka = new Kafka({
      clientId: `${config.kafka.clientId}-dlq-handler`,
      brokers: config.kafka.brokers,
    });
    this.consumer = null;
    this.mongoClient = null;
    this.isRunning = false;
  }

  /**
   * Initialize Kafka consumer for DLQ
   * @returns {Promise<void>}
   */
  async connect() {
    try {
      // Connect to MongoDB
      this.mongoClient = getMongoDBClient();
      await this.mongoClient.connect();

      // Determine DLQ topic based on environment
      // This handler can process both file-ingestion-dlq and validated-chunks-dlq
      const dlqTopic = process.env.KAFKA_DLQ_TOPIC || config.kafka.fileIngestionDlqTopic || config.kafka.validatedChunksDlqTopic;

      this.consumer = this.kafka.consumer({
        groupId: `${config.kafka.clientId}-dlq-handler`,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
      });

      await this.consumer.connect();
      logger.info('DLQ handler connected to Kafka', {
        brokers: config.kafka.brokers,
        topic: dlqTopic,
      });

      // Subscribe to DLQ topic
      await this.consumer.subscribe({
        topic: dlqTopic,
        fromBeginning: false,
      });

      logger.info('Subscribed to DLQ topic', {
        topic: dlqTopic,
      });
    } catch (error) {
      logger.error('Failed to connect DLQ handler', {
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Disconnect DLQ handler
   * @returns {Promise<void>}
   */
  async disconnect() {
    try {
      if (this.consumer) {
        await this.consumer.disconnect();
        logger.info('DLQ handler disconnected');
      }
      if (this.mongoClient) {
        await this.mongoClient.disconnect();
      }
    } catch (error) {
      logger.error('Error disconnecting DLQ handler', {
        error: error.message,
      });
    }
  }

  /**
   * Process DLQ message
   * Stores failed jobs in MongoDB for analysis and monitoring
   * 
   * @param {Object} dlqMessage - DLQ message
   * @returns {Promise<void>}
   */
  async processDLQMessage(dlqMessage) {
    try {
      logger.error('Processing DLQ message', {
        jobId: dlqMessage.jobId,
        fileId: dlqMessage.fileId,
        fileName: dlqMessage.fileName,
        errorCode: dlqMessage.errorCode,
        errorMessage: dlqMessage.errorMessage,
        retryCount: dlqMessage.retryCount,
        timestamp: new Date(dlqMessage.timestamp).toISOString(),
      });

      // Store failed job in MongoDB for analysis
      const failedJob = {
        jobId: dlqMessage.jobId,
        fileId: dlqMessage.fileId,
        fileName: dlqMessage.fileName,
        filePath: dlqMessage.filePath,
        errorCode: dlqMessage.errorCode,
        errorMessage: dlqMessage.errorMessage,
        errorDetails: dlqMessage.errorDetails || {},
        retryCount: dlqMessage.retryCount || 0,
        originalMessage: dlqMessage.originalMessage || null,
        createdAt: new Date(dlqMessage.timestamp),
        processedAt: new Date(),
      };

      // Store in jobs collection with failed status
      const jobsCollection = this.mongoClient.getJobsCollection();
      await jobsCollection.updateOne(
        { jobId: dlqMessage.jobId },
        {
          $set: {
            ...failedJob,
            status: 'FAILED',
            updatedAt: new Date(),
          },
        },
        { upsert: true }
      );

      logger.info('DLQ message stored in MongoDB', {
        jobId: dlqMessage.jobId,
        fileId: dlqMessage.fileId,
      });
    } catch (error) {
      logger.error('Error processing DLQ message', {
        jobId: dlqMessage?.jobId,
        error: error.message,
        stack: error.stack,
      });
    }
  }

  /**
   * Start consuming DLQ messages
   * @returns {Promise<void>}
   */
  async start() {
    if (this.isRunning) {
      logger.warn('DLQ handler is already running');
      return;
    }

    this.isRunning = true;

    try {
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            logger.debug('Received DLQ message', {
              topic,
              partition,
              offset: message.offset,
            });

            const dlqMessage = JSON.parse(message.value.toString());
            await this.processDLQMessage(dlqMessage);
          } catch (error) {
            logger.error('Error processing DLQ message', {
              topic,
              partition,
              offset: message.offset,
              error: error.message,
            });
          }
        },
      });

      logger.info('DLQ handler started');
    } catch (error) {
      this.isRunning = false;
      logger.error('Failed to start DLQ handler', {
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Stop consuming DLQ messages
   * @returns {Promise<void>}
   */
  async stop() {
    this.isRunning = false;
    logger.info('Stopping DLQ handler...');
  }
}

/**
 * Graceful shutdown handler
 */
async function shutdown() {
  logger.info('Shutting down DLQ handler...');

  try {
    const handler = new DLQHandler();
    await handler.stop();
    await handler.disconnect();

    logger.info('DLQ handler shut down successfully');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', { error: error.message });
    process.exit(1);
  }
}

// Handle shutdown signals
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

/**
 * Start the DLQ handler service
 */
async function start() {
  try {
    const dlqTopic = process.env.KAFKA_DLQ_TOPIC || config.kafka.fileIngestionDlqTopic || config.kafka.validatedChunksDlqTopic;
    
    logger.info('Starting DLQ handler service...', {
      kafkaBrokers: config.kafka.brokers,
      dlqTopic,
    });

    // Ensure DLQ topic exists
    await ensureKafkaTopics();

    const handler = new DLQHandler();
    await handler.connect();
    await handler.start();

    logger.info('DLQ handler service started successfully');
  } catch (error) {
    logger.error('Failed to start DLQ handler service', {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

// Start the application
start();

export default start;
