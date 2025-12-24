#!/usr/bin/env node

/**
 * Batch Upload Script
 * 
 * Uploads multiple CSV files (up to 100) in a single request using the batch endpoint.
 * 
 * Usage:
 *   node scripts/batch-upload.js [file_count] [rows_per_file]
 * 
 * Examples:
 *   node scripts/batch-upload.js 10 1000      # Upload 10 files
 *   node scripts/batch-upload.js 100 10000    # Upload 100 files (max)
 */

import { createWriteStream, createReadStream } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { promises as fs } from 'fs';
import http from 'http';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const API_URL = process.env.API_URL || 'http://localhost:3000/api/v1/upload/batch';
const fileCount = Math.min(parseInt(process.argv[2] || '10', 10), 100); // Max 100 files
const rowsPerFile = parseInt(process.argv[3] || '1000', 10);

console.log('Batch Upload Test');
console.log('=================');
console.log(`Files to upload: ${fileCount} (max 100)`);
console.log(`Rows per file: ${rowsPerFile}`);
console.log(`API URL: ${API_URL}`);
console.log('');

// Create temp directory for test files
const tempDir = join(__dirname, '..', 'temp-test-files');
await fs.mkdir(tempDir, { recursive: true });

/**
 * Generate a CSV file using streams
 */
async function generateCSV(filePath, rows) {
  return new Promise((resolve, reject) => {
    const stream = createWriteStream(filePath, { encoding: 'utf8' });
    const headers = ['id', 'name', 'email', 'created_at'];
    
    stream.write(headers.join(',') + '\n');
    
    let written = 0;
    const timestamp = new Date().toISOString();
    
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
 * Upload multiple files in a single batch request
 */
async function uploadBatch(files) {
  return new Promise(async (resolve, reject) => {
    try {
      const FormData = (await import('form-data')).default;
      const form = new FormData();
      
      // Append all files to form
      files.forEach((filePath, index) => {
        form.append('files', createReadStream(filePath), {
          filename: `test-file-${index + 1}.csv`,
        });
      });
      
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

// Generate all CSV files
console.log('Generating CSV files...\n');
const filePaths = [];

for (let i = 1; i <= fileCount; i++) {
  const fileName = `test-file-${i}.csv`;
  const filePath = join(tempDir, fileName);
  
  process.stdout.write(`[${i}/${fileCount}] Generating ${fileName}... `);
  const genStart = Date.now();
  await generateCSV(filePath, rowsPerFile);
  const genTime = Date.now() - genStart;
  console.log(`✓ (${genTime}ms)`);
  
  filePaths.push(filePath);
}

// Upload all files in a single batch
console.log(`\nUploading ${fileCount} files in batch...`);
const uploadStart = Date.now();
const result = await uploadBatch(filePaths);
const uploadTime = Date.now() - uploadStart;

// Clean up files
console.log('Cleaning up files...');
for (const filePath of filePaths) {
  try {
    await fs.unlink(filePath);
  } catch (error) {
    // Ignore cleanup errors
  }
}

try {
  await fs.rmdir(tempDir);
} catch (error) {
  // Directory might not be empty
}

// Print results
console.log('\n' + '='.repeat(50));
console.log('Batch Upload Results');
console.log('='.repeat(50));

if (result.success) {
  const data = result.data;
  console.log(`Status: SUCCESS`);
  console.log(`Total files: ${data.totalFiles || fileCount}`);
  console.log(`Successful: ${data.successful || 0}`);
  console.log(`Failed: ${data.failed || 0}`);
  console.log(`Upload time: ${uploadTime}ms (${(uploadTime / 1000).toFixed(2)}s)`);
  console.log(`Average time per file: ${(uploadTime / fileCount).toFixed(2)}ms`);
  
  if (data.errors && data.errors.length > 0) {
    console.log('\nErrors:');
    data.errors.forEach((error, index) => {
      console.log(`  ${index + 1}. ${error.fileName}: ${error.error}`);
    });
  }
} else {
  console.log(`Status: FAILED`);
  console.log(`HTTP Status: ${result.status}`);
  console.log(`Error: ${result.data.message || JSON.stringify(result.data)}`);
  process.exit(1);
}

process.exit(0);

