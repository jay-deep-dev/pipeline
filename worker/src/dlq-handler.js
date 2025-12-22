/**
 * Dead Letter Queue Handler
 * 
 * Separate service to handle messages from the Dead Letter Queue (DLQ).
 * Processes failed jobs, logs errors, and optionally retries or archives them.
 * 
 * @module dlq-handler
 */

import { Kafka } from 'kafkajs';
import { getConfig, createLogger } from '../../shared/index.js';
import { ensureKafkaTopics } from '../../shared/src/kafka-admin.js';

const config = getConfig();
const logger = createLogger('dlq-handler', config.logging.level);

/**
 * DLQ Handler Service
 * Consumes messages from DLQ and handles them appropriately
 */
class DLQHandler {
  constructor() {
    this.kafka = new Kafka({
      clientId: config.kafka.clientId + '-dlq-handler',
      brokers: config.kafka.brokers,
    });
    this.consumer = null;
    this.isRunning = false;
  }

  /**
   * Initialize Kafka consumer for DLQ
   * @returns {Promise<void>}
   */
  async connect() {
    try {
      this.consumer = this.kafka.consumer({
        groupId: config.kafka.consumerGroupId + '-dlq',
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
      });

      await this.consumer.connect();
      logger.info('DLQ handler connected to Kafka', {
        brokers: config.kafka.brokers,
        topic: config.kafka.dlqTopic,
      });

      // Subscribe to DLQ topic
      await this.consumer.subscribe({
        topic: config.kafka.dlqTopic,
        fromBeginning: false,
      });

      logger.info('Subscribed to DLQ topic', {
        topic: config.kafka.dlqTopic,
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
    } catch (error) {
      logger.error('Error disconnecting DLQ handler', {
        error: error.message,
      });
    }
  }

  /**
   * Process DLQ message
   * Logs error details and can be extended to:
   * - Store failed jobs in a database
   * - Send alerts/notifications
   * - Attempt manual retry
   * - Archive for analysis
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

      // Log detailed error information
      if (dlqMessage.errorDetails) {
        logger.error('DLQ error details', {
          jobId: dlqMessage.jobId,
          errorDetails: dlqMessage.errorDetails,
        });
      }

      // In a production system, you might want to:
      // 1. Store failed job in a database for analysis
      // 2. Send alert/notification to administrators
      // 3. Attempt automatic retry for transient errors
      // 4. Archive file for manual review

      logger.info('DLQ message processed', {
        jobId: dlqMessage.jobId,
        fileId: dlqMessage.fileId,
      });
    } catch (error) {
      logger.error('Error processing DLQ message', {
        jobId: dlqMessage?.jobId,
        error: error.message,
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
    logger.info('Starting DLQ handler service...', {
      kafkaBrokers: config.kafka.brokers,
      dlqTopic: config.kafka.dlqTopic,
    });

    // Ensure DLQ topic exists with the configured partitions/replication.
    // This mirrors the ingestion API and worker behavior and avoids relying
    // on broker-side auto topic creation in production.
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

