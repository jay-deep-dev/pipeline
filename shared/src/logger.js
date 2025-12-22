/**
 * Logger Module
 * 
 * Simple logging utility for the CSV ingestion pipeline.
 * Provides structured logging with different log levels.
 * 
 * @module logger
 */

/**
 * Log levels enum
 */
const LogLevel = {
  ERROR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3,
};

/**
 * Logger class for structured logging
 */
class Logger {
  constructor(serviceName = 'csv-pipeline', level = 'info') {
    this.serviceName = serviceName;
    this.level = LogLevel[level.toUpperCase()] || LogLevel.INFO;
  }

  /**
   * Format log message with timestamp and service name
   * @private
   */
  formatMessage(level, message, metadata = {}) {
    const timestamp = new Date().toISOString();
    return {
      timestamp,
      level,
      service: this.serviceName,
      message,
      ...metadata,
    };
  }

  /**
   * Log error message
   * @param {string} message - Error message
   * @param {Object} metadata - Additional metadata
   */
  error(message, metadata = {}) {
    if (this.level >= LogLevel.ERROR) {
      console.error(JSON.stringify(this.formatMessage('ERROR', message, metadata)));
    }
  }

  /**
   * Log warning message
   * @param {string} message - Warning message
   * @param {Object} metadata - Additional metadata
   */
  warn(message, metadata = {}) {
    if (this.level >= LogLevel.WARN) {
      console.warn(JSON.stringify(this.formatMessage('WARN', message, metadata)));
    }
  }

  /**
   * Log info message
   * @param {string} message - Info message
   * @param {Object} metadata - Additional metadata
   */
  info(message, metadata = {}) {
    if (this.level >= LogLevel.INFO) {
      console.info(JSON.stringify(this.formatMessage('INFO', message, metadata)));
    }
  }

  /**
   * Log debug message
   * @param {string} message - Debug message
   * @param {Object} metadata - Additional metadata
   */
  debug(message, metadata = {}) {
    if (this.level >= LogLevel.DEBUG) {
      console.debug(JSON.stringify(this.formatMessage('DEBUG', message, metadata)));
    }
  }
}

/**
 * Create a logger instance
 * @param {string} serviceName - Name of the service
 * @param {string} level - Log level (error, warn, info, debug)
 * @returns {Logger} Logger instance
 */
export function createLogger(serviceName, level = 'info') {
  return new Logger(serviceName, level);
}

export default createLogger;

