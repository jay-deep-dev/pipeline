/**
 * Storage Module
 * 
 * Provides file storage abstraction for CSV files.
 * Currently supports local filesystem, but designed to be easily
 * extensible to cloud storage (S3, Azure Blob, etc.).
 * 
 * @module storage
 */

import { promises as fs } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createLogger } from './logger.js';

const logger = createLogger('storage');

/**
 * Storage interface for file operations
 */
class Storage {
  constructor(uploadDir) {
    this.uploadDir = uploadDir;
  }

  /**
   * Initialize storage (create directories if needed)
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      await fs.mkdir(this.uploadDir, { recursive: true });
      logger.info('Storage initialized', { uploadDir: this.uploadDir });
    } catch (error) {
      logger.error('Failed to initialize storage', { 
        uploadDir: this.uploadDir, 
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * Save uploaded file to storage
   * @param {string} fileId - Unique file identifier
   * @param {Buffer|Stream} fileData - File data
   * @param {string} originalName - Original filename
   * @returns {Promise<string>} Path to saved file
   */
  async saveFile(fileId, fileData, originalName) {
    const fileName = `${fileId}_${originalName}`;
    const filePath = join(this.uploadDir, fileName);

    try {
      // If fileData is a Buffer, write directly
      if (Buffer.isBuffer(fileData)) {
        await fs.writeFile(filePath, fileData);
      } else {
        // If it's a stream, we need to handle it differently
        // For now, assume it's handled by multer or similar
        throw new Error('Stream handling not implemented in this version');
      }

      logger.info('File saved to storage', { fileId, filePath, originalName });
      return filePath;
    } catch (error) {
      logger.error('Failed to save file', { 
        fileId, 
        filePath, 
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * Get file path by file ID
   * @param {string} fileId - File identifier
   * @param {string} originalName - Original filename
   * @returns {string} File path
   */
  getFilePath(fileId, originalName) {
    const fileName = `${fileId}_${originalName}`;
    return join(this.uploadDir, fileName);
  }

  /**
   * Check if file exists
   * @param {string} filePath - Path to file
   * @returns {Promise<boolean>} True if file exists
   */
  async fileExists(filePath) {
    try {
      await fs.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Delete file from storage
   * @param {string} filePath - Path to file
   * @returns {Promise<void>}
   */
  async deleteFile(filePath) {
    try {
      await fs.unlink(filePath);
      logger.info('File deleted from storage', { filePath });
    } catch (error) {
      logger.error('Failed to delete file', { filePath, error: error.message });
      throw error;
    }
  }
}

/**
 * Create storage instance
 * @param {string} uploadDir - Upload directory path
 * @returns {Storage} Storage instance
 */
export function createStorage(uploadDir) {
  return new Storage(uploadDir);
}

export default createStorage;

