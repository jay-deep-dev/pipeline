/**
 * MongoDB Client Module (Fixed with Retry & Health Checks)
 * 
 * Manages MongoDB connection with automatic reconnection, health checks,
 * and optimized settings for high-volume processing.
 */

import { MongoClient } from 'mongodb';
import { getConfig, createLogger } from '../../shared/index.js';

const config = getConfig();
const logger = createLogger('mongodb-client', config.logging.level);

class MongoDBClient {
  constructor() {
    this.client = null;
    this.db = null;
    this.validRecordsCollection = null;
    this.errorRecordsCollection = null;
    this.jobsCollection = null;
    this.isConnected = false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
    this.reconnectDelay = 5000; // Start with 5 seconds
  }

  /**
   * Connect to MongoDB with retry logic
   */
  async connect() {
    const maxRetries = 5;
    let attempt = 0;

    while (attempt < maxRetries) {
      try {
        // Close existing connection if any
        if (this.client) {
          await this.client.close().catch(() => {});
        }

        // Create new client with optimized settings for high load
        this.client = new MongoClient(config.mongodb.uri, {
          maxPoolSize: 20,             
          minPoolSize: 5,                
          maxIdleTimeMS: 60000,          
          serverSelectionTimeoutMS: 30000, 
          socketTimeoutMS: 60000,  
          connectTimeoutMS: 30000,       
          heartbeatFrequencyMS: 10000,
          retryWrites: true,          
          retryReads: true,         
          monitorCommands: false,
        });

        await this.client.connect();
        
        this.db = this.client.db(config.mongodb.database);
        this.validRecordsCollection = this.db.collection(config.mongodb.validRecordsCollection);
        this.errorRecordsCollection = this.db.collection(config.mongodb.errorRecordsCollection);
        this.jobsCollection = this.db.collection(config.mongodb.jobsCollection);

        // Set up topology event listeners for connection monitoring
        this.setupConnectionMonitoring();

        await this.createIndexes();

        this.isConnected = true;
        this.reconnectAttempts = 0;
        
        logger.info('MongoDB connected successfully', {
          uri: config.mongodb.uri.replace(/\/\/.*@/, '//*****@'), // Hide credentials
          database: config.mongodb.database,
          attempt: attempt + 1,
        });

        return;
      } catch (error) {
        attempt++;
        logger.error(`MongoDB connection attempt ${attempt}/${maxRetries} failed`, {
          error: error.message,
          attempt,
        });

        if (attempt >= maxRetries) {
          throw new Error(`Failed to connect to MongoDB after ${maxRetries} attempts: ${error.message}`);
        }

        // Exponential backoff
        const delay = Math.min(1000 * Math.pow(2, attempt), 30000);
        logger.info(`Retrying connection in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  /**
   * Set up connection monitoring to detect and handle disconnections
   */
  setupConnectionMonitoring() {
    if (!this.client) return;

    this.client.on('serverDescriptionChanged', (event) => {
      logger.debug('MongoDB server description changed', {
        address: event.address,
        newDescription: event.newDescription.type,
      });
    });

    this.client.on('topologyDescriptionChanged', (event) => {
      logger.debug('MongoDB topology changed', {
        type: event.newDescription.type,
      });
    });

    this.client.on('close', () => {
      logger.warn('MongoDB connection closed');
      this.isConnected = false;
      this.scheduleReconnect();
    });

    this.client.on('error', (error) => {
      logger.error('MongoDB connection error', { error: error.message });
      this.isConnected = false;
    });
  }

  /**
   * Schedule automatic reconnection
   */
  scheduleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      logger.error('Max reconnection attempts reached. Manual intervention required.');
      return;
    }

    this.reconnectAttempts++;
    const delay = Math.min(this.reconnectDelay * this.reconnectAttempts, 60000);

    logger.info(`Scheduling reconnection attempt ${this.reconnectAttempts} in ${delay}ms`);

    setTimeout(async () => {
      try {
        await this.connect();
      } catch (error) {
        logger.error('Reconnection failed', { error: error.message });
      }
    }, delay);
  }

  /**
   * Verify connection is alive before operations
   */
  async ensureConnection() {
    if (!this.isConnected || !this.client) {
      logger.warn('Connection lost, attempting to reconnect...');
      await this.connect();
    }

    try {
      // Ping to verify connection is actually working
      await this.db.admin().ping();
    } catch (error) {
      logger.error('Connection health check failed', { error: error.message });
      this.isConnected = false;
      await this.connect();
    }
  }

  /**
   * Create indexes with error handling
   */
  async createIndexes() {
    try {
      // Create only essential indexes to avoid overhead
      await this.jobsCollection.createIndex({ createdAt: 1 });
      logger.debug('MongoDB indexes created');
    } catch (error) {
      logger.warn('Failed to create indexes', { error: error.message });
    }
  }

  /**
   * Insert valid records with connection verification
   */
  async insertValidRecords(documents, jobId,fileName, fileId, options = {}) {
    if (!documents || documents.length === 0) {
      return { insertedCount: 0, success: true };
    }

    // Ensure connection is healthy
    await this.ensureConnection();

    const { ordered = false } = options;

    try {
      const operations = documents.map((doc) => ({
        insertOne: {
          document: {
            ...doc,
            fileName,
            insertedAt: new Date(),
          },
        },
      }));

      const result = await this.validRecordsCollection.bulkWrite(operations, {
        ordered,
        writeConcern: { w: 1 },
      });

      logger.debug('Valid records inserted', {
        jobId,
        fileId,
        fileName,
        insertedCount: result.insertedCount,
        batchSize: documents.length,
      });

      return {
        insertedCount: result.insertedCount,
        success: true,
        result,
      };
    } catch (error) {
      logger.error('Failed to insert valid records', {
        jobId,
        fileId,
        batchSize: documents.length,
        error: error.message,
        code: error.code,
      });

      // Check if it's a connection error and mark as disconnected
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        this.scheduleReconnect();
      }

      throw error;
    }
  }

  /**
   * Insert error records with connection verification
   */
  async insertErrorRecords(documents, jobId,fileName, fileId, options = {}) {
    if (!documents || documents.length === 0) {
      return { insertedCount: 0, success: true };
    }

    // Ensure connection is healthy
    await this.ensureConnection();

    const { ordered = false } = options;

    try {
      const operations = documents.map((doc) => ({
        insertOne: {
          document: {
            ...doc,
            fileName,
            createdAt: doc.createdAt || new Date(),
          },
        },
      }));

      const result = await this.errorRecordsCollection.bulkWrite(operations, {
        ordered,
        writeConcern: { w: 1 },
      });

      logger.debug('Error records inserted', {
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
      logger.error('Failed to insert error records', {
        jobId,
        fileId,
        batchSize: documents.length,
        error: error.message,
        code: error.code,
      });

      // Check if it's a connection error
      if (this.isConnectionError(error)) {
        this.isConnected = false;
        this.scheduleReconnect();
      }

      throw error;
    }
  }

  /**
   * Check if error is related to connection issues
   */
  isConnectionError(error) {
    const connectionErrorCodes = [
      'ENOTFOUND',
      'ECONNREFUSED',
      'ETIMEDOUT',
      'ECONNRESET',
      'EPIPE',
      'MongoNetworkError',
      'MongoTimeoutError',
    ];

    return connectionErrorCodes.some(code => 
      error.message.includes(code) || 
      error.name.includes(code) ||
      error.code === code
    );
  }

  /**
   * Graceful disconnect
   */
  async disconnect() {
    try {
      if (this.client) {
        await this.client.close();
        this.isConnected = false;
        logger.info('MongoDB disconnected gracefully');
      }
    } catch (error) {
      logger.error('Error disconnecting from MongoDB', {
        error: error.message,
      });
    }
  }

  // Getter methods
  getValidRecordsCollection() {
    if (!this.isConnected) {
      throw new Error('MongoDB not connected');
    }
    return this.validRecordsCollection;
  }

  getErrorRecordsCollection() {
    if (!this.isConnected) {
      throw new Error('MongoDB not connected');
    }
    return this.errorRecordsCollection;
  }

  getJobsCollection() {
    if (!this.isConnected) {
      throw new Error('MongoDB not connected');
    }
    return this.jobsCollection;
  }

  isConnectedToDB() {
    return this.isConnected;
  }
}

// Singleton instance
let mongoClientInstance = null;

export function getMongoDBClient() {
  if (!mongoClientInstance) {
    mongoClientInstance = new MongoDBClient();
  }
  return mongoClientInstance;
}

export default getMongoDBClient;