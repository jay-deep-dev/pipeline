/**
 * MongoDB Client Module
 * 
 * Manages MongoDB connection and provides batch insert operations.
 * Optimized for high-throughput bulk inserts with configurable batch sizes.
 * 
 * @module mongodb-client
 */

import { MongoClient } from 'mongodb';
import { getConfig, createLogger } from '../../shared/index.js';

const config = getConfig();
const logger = createLogger('mongodb-client', config.logging.level);

/**
 * MongoDB Client Service
 * Handles connection and batch operations
 */
class MongoDBClient {
  constructor() {
    this.client = null;
    this.db = null;
    this.collection = null;
    this.isConnected = false;
  }

  /**
   * Connect to MongoDB
   * @returns {Promise<void>}
   */
  async connect() {
    try {
      this.client = new MongoClient(config.mongodb.uri, {
        maxPoolSize: 50,
        minPoolSize: 5,
        maxIdleTimeMS: 30000,
        serverSelectionTimeoutMS: 5000,
        socketTimeoutMS: 45000,
      });

      await this.client.connect();
      this.db = this.client.db(config.mongodb.database);
      this.collection = this.db.collection(config.mongodb.collection);

      // Create indexes for better query performance
      await this.createIndexes();

      this.isConnected = true;
      logger.info('MongoDB connected', {
        uri: config.mongodb.uri,
        database: config.mongodb.database,
        collection: config.mongodb.collection,
      });
    } catch (error) {
      logger.error('Failed to connect to MongoDB', {
        error: error.message,
        uri: config.mongodb.uri,
      });
      throw error;
    }
  }

  /**
   * Create indexes for performance optimization
   * @returns {Promise<void>}
   */
  async createIndexes() {
    try {
      // Create index on jobId and fileId for faster lookups
      await this.collection.createIndex({ jobId: 1 });
      await this.collection.createIndex({ fileId: 1 });
      await this.collection.createIndex({ createdAt: 1 });

      // Compound index for common queries
      await this.collection.createIndex({ fileId: 1, createdAt: -1 });

      logger.debug('MongoDB indexes created');
    } catch (error) {
      logger.warn('Failed to create indexes', { error: error.message });
      // Don't throw - indexes might already exist
    }
  }

  /**
   * Disconnect from MongoDB
   * @returns {Promise<void>}
   */
  async disconnect() {
    try {
      if (this.client) {
        await this.client.close();
        this.isConnected = false;
        logger.info('MongoDB disconnected');
      }
    } catch (error) {
      logger.error('Error disconnecting from MongoDB', {
        error: error.message,
      });
    }
  }

  /**
   * Insert documents in batches using bulk operations
   * Optimized for high throughput with configurable batch size
   * 
   * @param {Object[]} documents - Array of documents to insert
   * @param {string} jobId - Job identifier
   * @param {string} fileId - File identifier
   * @param {Object} [options] - Insert options
   * @param {boolean} [options.ordered=false] - Whether inserts should be ordered
   * @returns {Promise<Object>} Insert result with statistics
   */
  async insertBatch(documents, jobId, fileId, options = {}) {
    if (!this.isConnected) {
      throw new Error('MongoDB not connected');
    }

    if (!documents || documents.length === 0) {
      return {
        insertedCount: 0,
        success: true,
      };
    }

    const { ordered = false } = options;

    try {
      // Add metadata to each document
      const enrichedDocuments = documents.map((doc) => ({
        ...doc,
        jobId,
        fileId,
        createdAt: new Date(),
        ingestedAt: new Date(),
      }));

      // Use bulkWrite for better performance and error handling
      const operations = enrichedDocuments.map((doc) => ({
        insertOne: {
          document: doc,
        },
      }));

      const result = await this.collection.bulkWrite(operations, {
        ordered,
        writeConcern: { w: 'majority' },
      });

      logger.debug('Batch inserted to MongoDB', {
        jobId,
        fileId,
        insertedCount: result.insertedCount,
        batchSize: documents.length,
      });

      return {
        insertedCount: result.insertedCount,
        success: true,
        result,
      };
    } catch (error) {
      logger.error('Failed to insert batch to MongoDB', {
        jobId,
        fileId,
        batchSize: documents.length,
        error: error.message,
      });

      // Return partial success information if available
      if (error.writeErrors && error.writeErrors.length > 0) {
        const successfulInserts = documents.length - error.writeErrors.length;
        return {
          insertedCount: successfulInserts,
          success: false,
          error: error.message,
          writeErrors: error.writeErrors,
        };
      }

      throw error;
    }
  }

  /**
   * Get collection instance (for advanced operations)
   * @returns {Object} MongoDB collection
   */
  getCollection() {
    if (!this.isConnected) {
      throw new Error('MongoDB not connected');
    }
    return this.collection;
  }

  /**
   * Check if connected
   * @returns {boolean} Connection status
   */
  isConnectedToDB() {
    return this.isConnected;
  }
}

// Singleton instance
let mongoClientInstance = null;

/**
 * Get or create MongoDB client instance
 * @returns {MongoDBClient} MongoDB client instance
 */
export function getMongoDBClient() {
  if (!mongoClientInstance) {
    mongoClientInstance = new MongoDBClient();
  }
  return mongoClientInstance;
}

export default getMongoDBClient;

