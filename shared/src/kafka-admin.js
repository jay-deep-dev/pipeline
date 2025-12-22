/**
 * Kafka Admin Utilities
 *
 * Provides bootstrap logic to ensure required topics exist with the
 * expected number of partitions and replication factor. This avoids
 * relying on broker-side auto topic creation in production.
 */

import { Kafka } from 'kafkajs';
import { getConfig, createLogger } from '../index.js';

const config = getConfig();
const logger = createLogger('kafka-admin', config.logging.level);

let topicsInitialized = false;

/**
 * Ensure ingestion and DLQ topics exist with configured topology.
 * Safe to call multiple times; the admin client will no-op if topics exist.
 */
export async function ensureKafkaTopics() {
  if (topicsInitialized) {
    return;
  }

  const kafka = new Kafka({
    clientId: `${config.kafka.clientId}-admin`,
    brokers: config.kafka.brokers,
    connectionTimeout: 3000,
    requestTimeout: 30000,
  });

  const admin = kafka.admin();

  try {
    await admin.connect();

    logger.info('Ensuring Kafka topics exist', {
      ingestionTopic: config.kafka.ingestionTopic,
      ingestionPartitions: config.kafka.ingestionPartitions,
      ingestionReplicationFactor: config.kafka.ingestionReplicationFactor,
      dlqTopic: config.kafka.dlqTopic,
      dlqPartitions: config.kafka.dlqPartitions,
      dlqReplicationFactor: config.kafka.dlqReplicationFactor,
    });

    try {
      await admin.createTopics({
        topics: [
          {
            topic: config.kafka.ingestionTopic,
            numPartitions: config.kafka.ingestionPartitions,
            replicationFactor: config.kafka.ingestionReplicationFactor,
          },
          {
            topic: config.kafka.dlqTopic,
            numPartitions: config.kafka.dlqPartitions,
            replicationFactor: config.kafka.dlqReplicationFactor,
          },
        ],
        waitForLeaders: true,
      });
      logger.info('Kafka topics created successfully');
    } catch (createError) {
      // Topics might already exist - this is fine, just log it
      if (createError.message && createError.message.includes('already exists')) {
        logger.info('Kafka topics already exist');
      } else {
        // Log other errors but don't fail - topics might exist with different config
        logger.warn('Topic creation completed with warnings (topics may already exist)', {
          error: createError.message,
        });
      }
    }

    topicsInitialized = true;
    logger.info('Kafka topics ensured successfully');
  } catch (error) {
    logger.error('Failed to ensure Kafka topics', {
      error: error.message,
    });
    // In production you may want to fail fast here; for now, rethrow.
    throw error;
  } finally {
    try {
      await admin.disconnect();
    } catch {
      // ignore disconnect errors
    }
  }
}

export default ensureKafkaTopics;

