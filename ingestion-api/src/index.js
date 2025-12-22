/**
 * Ingestion API Service Entry Point
 * 
 * Main Express application for CSV file upload and validation.
 * Handles file uploads, performs validation, and produces Kafka events.
 * 
 * @module ingestion-api
 */

import express from 'express';
import { getConfig, createLogger } from '../../shared/index.js';
import { ensureKafkaTopics } from '../../shared/src/kafka-admin.js';
import { getKafkaProducer } from './kafka-producer.js';
import uploadRoutes from './routes/upload.js';

const config = getConfig();
const logger = createLogger('ingestion-api', config.logging.level);

// Initialize Express app
const app = express();

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Request logging middleware
app.use((req, res, next) => {
  logger.debug('Incoming request', {
    method: req.method,
    path: req.path,
    ip: req.ip,
  });
  next();
});

// Routes
app.use('/', uploadRoutes);

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error('Unhandled error', {
    error: err.message,
    stack: err.stack,
    path: req.path,
  });

  res.status(err.status || 500).json({
    error: 'Internal server error',
    message: err.message,
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Not found',
    message: `Route ${req.method} ${req.path} not found`,
  });
});

/**
 * Graceful shutdown handler
 */
async function shutdown() {
  logger.info('Shutting down ingestion API...');

  try {
    // Disconnect Kafka producer
    const kafkaProducer = getKafkaProducer();
    await kafkaProducer.disconnect();

    logger.info('Ingestion API shut down successfully');
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
 * Start the server
 */
async function start() {
  try {
    // Debug: Log Kafka brokers configuration
    logger.info('Starting ingestion API', {
      kafkaBrokers: process.env.KAFKA_BROKERS || 'not set',
      kafkaBrokersFromConfig: config.kafka.brokers,
    });
    
    // Ensure Kafka topics exist with production-style configuration
    logger.info('Ensuring Kafka topics exist...');
    await ensureKafkaTopics();

    // Initialize Kafka producer
    logger.info('Initializing Kafka producer...');
    const kafkaProducer = getKafkaProducer();
    await kafkaProducer.connect();

    // Start HTTP server
    app.listen(config.api.port, config.api.host, () => {
      logger.info('Ingestion API server started', {
        host: config.api.host,
        port: config.api.port,
        environment: process.env.NODE_ENV || 'development',
      });
    });
  } catch (error) {
    logger.error('Failed to start ingestion API', {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

// Start the application
start();

export default app;

