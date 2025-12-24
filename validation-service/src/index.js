/**
 * Validation Service Entry Point
 * 
 * Main validation service that consumes file-ingestion messages,
 * validates CSV rows, chunks them, and produces validated-chunks messages.
 * 
 * @module validation-service
 */

import { getConfig, createLogger } from '../../shared/index.js';
import { ensureKafkaTopics } from '../../shared/src/kafka-admin.js';
import { getKafkaConsumer } from './kafka-consumer.js';

const config = getConfig();
const logger = createLogger('validation-service', config.logging.level);

/**
 * Graceful shutdown handler
 */
async function shutdown() {
  logger.info('Shutting down validation service...');

  try {
    // Stop Kafka consumer
    const kafkaConsumer = getKafkaConsumer();
    await kafkaConsumer.stop();
    await kafkaConsumer.disconnect();

    logger.info('Validation service shut down successfully');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', { error: error.message });
    process.exit(1);
  }
}

// Handle shutdown signals
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception', {
    error: error.message,
    stack: error.stack,
  });
  shutdown();
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled rejection', {
    reason,
    promise,
  });
});

/**
 * Start the validation service
 */
async function start() {
  try {
    logger.info('Starting validation service...', {
      kafkaBrokers: config.kafka.brokers,
      fileIngestionTopic: config.kafka.fileIngestionTopic,
      validatedChunksTopic: config.kafka.validatedChunksTopic,
      consumerGroupId: config.kafka.validationConsumerGroupId,
      chunkSize: config.validation.chunkSize,
      maxConcurrentFiles: config.validation.maxConcurrentFiles,
    });

    // Ensure Kafka topics exist before connecting consumer
    logger.info('Ensuring Kafka topics exist...');
    await ensureKafkaTopics();

    // Connect to Kafka and start consuming
    logger.info('Connecting to Kafka...');
    const kafkaConsumer = getKafkaConsumer();
    await kafkaConsumer.connect();
    await kafkaConsumer.start();

    logger.info('Validation service started successfully');
  } catch (error) {
    logger.error('Failed to start validation service', {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

// Start the application
start();

export default start;

