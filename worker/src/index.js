/**
 * Worker Service Entry Point
 * 
 * Main worker service that consumes Kafka messages and processes CSV files.
 * Handles CSV file streaming, batch processing, and MongoDB insertion.
 * 
 * @module worker
 */

import { getConfig, createLogger } from '../../shared/index.js';
import { ensureKafkaTopics } from '../../shared/src/kafka-admin.js';
import { getMongoDBClient } from './mongodb-client.js';
import { getKafkaConsumer } from './kafka-consumer.js';

const config = getConfig();
const logger = createLogger('worker', config.logging.level);

/**
 * Graceful shutdown handler
 */
async function shutdown() {
  logger.info('Shutting down worker service...');

  try {
    // Stop Kafka consumer
    const kafkaConsumer = getKafkaConsumer();
    await kafkaConsumer.stop();
    await kafkaConsumer.disconnect();

    // Disconnect MongoDB
    const mongoClient = getMongoDBClient();
    await mongoClient.disconnect();

    logger.info('Worker service shut down successfully');
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
 * Start the worker service
 */
async function start() {
  try {
    logger.info('Starting worker service...', {
      kafkaBrokers: config.kafka.brokers,
      kafkaTopic: config.kafka.ingestionTopic,
      consumerGroupId: config.kafka.consumerGroupId,
      mongodbUri: config.mongodb.uri,
      batchSize: config.worker.batchSize,
      maxConcurrentFiles: config.worker.maxConcurrentFiles,
    });

    // Ensure Kafka topics exist before connecting consumer
    logger.info('Ensuring Kafka topics exist...');
    await ensureKafkaTopics();

    // Connect to MongoDB
    logger.info('Connecting to MongoDB...');
    const mongoClient = getMongoDBClient();
    await mongoClient.connect();

    // Connect to Kafka and start consuming
    logger.info('Connecting to Kafka...');
    const kafkaConsumer = getKafkaConsumer();
    await kafkaConsumer.connect();
    await kafkaConsumer.start();

    logger.info('Worker service started successfully');
  } catch (error) {
    logger.error('Failed to start worker service', {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

// Start the application
start();

export default start;

