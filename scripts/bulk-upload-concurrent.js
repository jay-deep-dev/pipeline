#!/usr/bin/env node

/**
 * Optimized Bulk Upload Test Script (Node.js)
 * 
 * Tests the CSV ingestion API by uploading multiple CSV files.
 * Uses streaming for efficient CSV generation.
 * Supports concurrent uploads for better throughput.
 * 
 * Usage:
 *   node scripts/bulk-upload-test.js [file_count] [rows_per_file] [concurrency]
 * 
 * Examples:
 *   node scripts/bulk-upload-test.js 10 1000        # Sequential (1 at a time)
 *   node scripts/bulk-upload-test.js 10 1000 3      # 3 files at once
 *   node scripts/bulk-upload-test.js 100 10000 5    # 5 files at once
 */

import { createWriteStream, createReadStream } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { promises as fs } from 'fs';
import http from 'http';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const API_URL = process.env.API_URL || 'http://localhost:3000/api/v1/upload';
const fileCount = parseInt(process.argv[2] || '10', 10);
const rowsPerFile = parseInt(process.argv[3] || '1000', 10);
const concurrency = parseInt(process.argv[4] || '1', 10);

console.log('Optimized Bulk Upload Test (Node.js)');
console.log('=====================================');
console.log(`Files to upload: ${fileCount}`);
console.log(`Rows per file: ${rowsPerFile}`);
console.log(`Concurrency: ${concurrency} (${concurrency === 1 ? 'sequential' : `${concurrency} files at once`})`);
console.log(`API URL: ${API_URL}`);
console.log('');

// Create temp directory for test files
const tempDir = join(__dirname, '..', 'temp-test-files');
await fs.mkdir(tempDir, { recursive: true });

/**
 * Generate a CSV file using streams (fast and memory efficient)
 */
async function generateCSV(filePath, rows) {
  return new Promise((resolve, reject) => {
    const stream = createWriteStream(filePath, { encoding: 'utf8' });
    const headers = ['id', 'name', 'email', 'created_at'];
    
    stream.write(headers.join(',') + '\n');
    
    let written = 0;
    const timestamp = new Date().toISOString(); // Single timestamp for all rows (faster)
    
    function writeBatch() {
      let canContinue = true;
      
      while (written < rows && canContinue) {
        const id = written + 1;
        const row = `${id},User ${id},user${id}@example.com,${timestamp}\n`;
        
        canContinue = stream.write(row);
        written++;
      }
      
      if (written < rows) {
        stream.once('drain', writeBatch);
      } else {
        stream.end();
      }
    }
    
    stream.on('finish', resolve);
    stream.on('error', reject);
    writeBatch();
  });
}

/**
 * Upload a file to the API using http module (compatible with busboy/multer)
 */
async function uploadFile(filePath) {
  return new Promise(async (resolve, reject) => {
    try {
      const FormData = (await import('form-data')).default;
      const form = new FormData();
      
      form.append('file', createReadStream(filePath));
      
      // Parse URL
      const url = new URL(API_URL);
      
      const options = {
        hostname: url.hostname,
        port: url.port || 80,
        path: url.pathname,
        method: 'POST',
        headers: form.getHeaders(),
      };
      
      const req = http.request(options, (res) => {
        let data = '';
        
        res.on('data', (chunk) => {
          data += chunk;
        });
        
        res.on('end', () => {
          try {
            const jsonData = JSON.parse(data);
            resolve({
              success: res.statusCode >= 200 && res.statusCode < 300,
              status: res.statusCode,
              data: jsonData,
            });
          } catch (error) {
            resolve({
              success: false,
              status: res.statusCode,
              data: { message: data },
            });
          }
        });
      });
      
      req.on('error', (error) => {
        reject(error);
      });
      
      // Pipe the form data to the request
      form.pipe(req);
      
    } catch (error) {
      reject(error);
    }
  });
}

/**
 * Process a single file (generate and upload)
 */
async function processFile(fileNumber) {
  const fileName = `test-file-${fileNumber}.csv`;
  const filePath = join(tempDir, fileName);
  
  try {
    // Generate CSV
    process.stdout.write(`[${fileNumber}/${fileCount}] Generating ${fileName}... `);
    const genStart = Date.now();
    await generateCSV(filePath, rowsPerFile);
    const genTime = Date.now() - genStart;
    process.stdout.write(`✓ (${genTime}ms) `);
    
    // Upload file
    process.stdout.write('Uploading... ');
    const uploadStart = Date.now();
    const result = await uploadFile(filePath);
    const uploadTime = Date.now() - uploadStart;
    
    // Clean up file
    await fs.unlink(filePath);
    
    if (result.success) {
      const jobId = result.data?.jobId || result.data?.data?.jobId || 'unknown';
      console.log(`✓ (${uploadTime}ms) Success (Job: ${jobId})`);
      return { success: true, file: fileName, result };
    } else {
      console.log(`✗ Failed: ${result.data.message || result.status}`);
      return { success: false, file: fileName, error: result.data };
    }
  } catch (error) {
    console.log(`✗ Error: ${error.message}`);
    // Try to clean up file if it exists
    try {
      await fs.unlink(filePath);
    } catch {}
    return { success: false, file: fileName, error: error.message };
  }
}

/**
 * Process files with controlled concurrency
 */
async function processFilesWithConcurrency(fileCount, concurrency) {
  const results = {
    success: 0,
    failed: 0,
    errors: [],
  };
  
  // Create array of file numbers to process
  const fileNumbers = Array.from({ length: fileCount }, (_, i) => i + 1);
  
  // Process in batches
  for (let i = 0; i < fileNumbers.length; i += concurrency) {
    const batch = fileNumbers.slice(i, i + concurrency);
    
    if (concurrency > 1) {
      console.log(`\n--- Processing batch: files ${batch[0]} to ${batch[batch.length - 1]} ---`);
    }
    
    // Process batch concurrently
    const batchResults = await Promise.all(
      batch.map(fileNumber => processFile(fileNumber))
    );
    
    // Aggregate results
    batchResults.forEach(result => {
      if (result.success) {
        results.success++;
      } else {
        results.failed++;
        results.errors.push({ file: result.file, error: result.error });
      }
    });
    
    // Small delay between batches (not between files in same batch)
    if (i + concurrency < fileNumbers.length) {
      await new Promise(resolve => setTimeout(resolve, 50));
    }
  }
  
  return results;
}

// Main execution
const startTime = Date.now();

console.log('Generating and uploading files...\n');

const results = await processFilesWithConcurrency(fileCount, concurrency);

// Clean up temp directory
try {
  await fs.rmdir(tempDir);
} catch (error) {
  // Directory might not be empty or already deleted
}

// Print summary
const elapsed = Date.now() - startTime;
const totalRows = rowsPerFile * results.success;
const rowsPerSecond = Math.floor(totalRows / (elapsed / 1000));

console.log('\n' + '='.repeat(50));
console.log('Summary');
console.log('='.repeat(50));
console.log(`Total files: ${fileCount}`);
console.log(`Successful: ${results.success}`);
console.log(`Failed: ${results.failed}`);
console.log(`Concurrency: ${concurrency}`);
console.log(`Time elapsed: ${elapsed}ms (${(elapsed / 1000).toFixed(2)}s)`);
console.log(`Average time per file: ${(elapsed / fileCount).toFixed(2)}ms`);
console.log(`Total rows processed: ${totalRows.toLocaleString()}`);
console.log(`Throughput: ~${rowsPerSecond.toLocaleString()} rows/second`);

if (concurrency > 1) {
  const speedup = fileCount / (elapsed / 1000) / (fileCount / (elapsed / concurrency / 1000));
  console.log(`Speedup factor: ~${speedup.toFixed(2)}x vs sequential`);
}

if (results.errors.length > 0) {
  console.log('\nErrors:');
  results.errors.forEach(({ file, error }) => {
    const errorMsg = typeof error === 'object' ? JSON.stringify(error) : error;
    console.log(`  - ${file}: ${errorMsg}`);
  });
}

process.exit(results.failed > 0 ? 1 : 0);