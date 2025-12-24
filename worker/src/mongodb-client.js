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
    this.validRecordsCollection = null;
    this.errorRecordsCollection = null;
    this.jobsCollection = null;
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
      this.validRecordsCollection = this.db.collection(config.mongodb.validRecordsCollection);
      this.errorRecordsCollection = this.db.collection(config.mongodb.errorRecordsCollection);
      this.jobsCollection = this.db.collection(config.mongodb.jobsCollection);

      // Create indexes for better query performance
      await this.createIndexes();

      this.isConnected = true;
      logger.info('MongoDB connected', {
        uri: config.mongodb.uri,
        database: config.mongodb.database,
        validRecordsCollection: config.mongodb.validRecordsCollection,
        errorRecordsCollection: config.mongodb.errorRecordsCollection,
        jobsCollection: config.mongodb.jobsCollection,
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
      // Create indexes for valid records collection
      await this.validRecordsCollection.createIndex({ jobId: 1 });
      await this.validRecordsCollection.createIndex({ fileId: 1 });
      await this.validRecordsCollection.createIndex({ createdAt: 1 });
      await this.validRecordsCollection.createIndex({ email: 1 }); // Optional: for email lookups
      await this.validRecordsCollection.createIndex({ fileId: 1, createdAt: -1 });

      // Create indexes for error records collection
      await this.errorRecordsCollection.createIndex({ jobId: 1 });
      await this.errorRecordsCollection.createIndex({ fileId: 1 });
      await this.errorRecordsCollection.createIndex({ createdAt: 1 });
      await this.errorRecordsCollection.createIndex({ fileId: 1, createdAt: -1 });

      // Create indexes for jobs collection
      await this.jobsCollection.createIndex({ jobId: 1 }, { unique: true });
      await this.jobsCollection.createIndex({ fileId: 1 });
      await this.jobsCollection.createIndex({ status: 1 });
      await this.jobsCollection.createIndex({ createdAt: 1 });

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
   * Insert valid records in batches
   * 
   * @param {Object[]} documents - Array of valid documents to insert
   * @param {string} jobId - Job identifier
   * @param {string} fileId - File identifier
   * @param {Object} [options] - Insert options
   * @param {boolean} [options.ordered=false] - Whether inserts should be ordered
   * @returns {Promise<Object>} Insert result with statistics
   */
  async insertValidRecords(documents, jobId, fileId, options = {}) {
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
      // Documents already have metadata from validation service
      const operations = documents.map((doc) => ({
        insertOne: {
          document: {
            ...doc,
            insertedAt: new Date(), // Ensure insertedAt is set
          },
        },
      }));

      const result = await this.validRecordsCollection.bulkWrite(operations, {
        ordered,
        writeConcern: { w: 'majority' },
      });

      logger.debug('Valid records batch inserted to MongoDB', {
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
      logger.error('Failed to insert valid records batch to MongoDB', {
        jobId,
        fileId,
        batchSize: documents.length,
        error: error.message,
      });

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
   * Insert error records in batches
   * 
   * @param {Object[]} documents - Array of error documents to insert
   * @param {string} jobId - Job identifier
   * @param {string} fileId - File identifier
   * @param {Object} [options] - Insert options
   * @param {boolean} [options.ordered=false] - Whether inserts should be ordered
   * @returns {Promise<Object>} Insert result with statistics
   */
  async insertErrorRecords(documents, jobId, fileId, options = {}) {
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
      // Documents already have metadata from validation service
      const operations = documents.map((doc) => ({
        insertOne: {
          document: {
            ...doc,
            createdAt: doc.createdAt || new Date(), // Ensure createdAt is set
          },
        },
      }));

      const result = await this.errorRecordsCollection.bulkWrite(operations, {
        ordered,
        writeConcern: { w: 'majority' },
      });

      logger.debug('Error records batch inserted to MongoDB', {
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
      logger.error('Failed to insert error records batch to MongoDB', {
        jobId,
        fileId,
        batchSize: documents.length,
        error: error.message,
      });

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
   * Insert documents in batches (legacy method for backward compatibility)
   * @deprecated Use insertValidRecords or insertErrorRecords instead
   */
  async insertBatch(documents, jobId, fileId, options = {}) {
    return this.insertValidRecords(documents, jobId, fileId, options);
  }

  /**
   * Get valid records collection instance
   * @returns {Object} MongoDB collection
   */
  getValidRecordsCollection() {
    if (!this.isConnected) {
      throw new Error('MongoDB not connected');
    }
    return this.validRecordsCollection;
  }

  /**
   * Get error records collection instance
   * @returns {Object} MongoDB collection
   */
  getErrorRecordsCollection() {
    if (!this.isConnected) {
      throw new Error('MongoDB not connected');
    }
    return this.errorRecordsCollection;
  }

  /**
   * Get jobs collection instance
   * @returns {Object} MongoDB collection
   */
  getJobsCollection() {
    if (!this.isConnected) {
      throw new Error('MongoDB not connected');
    }
    return this.jobsCollection;
  }

  /**
   * Get collection instance (legacy method for backward compatibility)
   * @deprecated Use getValidRecordsCollection instead
   * @returns {Object} MongoDB collection
   */
  getCollection() {
    return this.getValidRecordsCollection();
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

