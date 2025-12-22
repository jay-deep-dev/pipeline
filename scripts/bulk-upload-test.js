#!/usr/bin/env node

/**
 * Bulk Upload Test Script
 * 
 * Tests the CSV ingestion API by uploading multiple CSV files.
 * Useful for testing the pipeline with 100 files as specified in requirements.
 * 
 * Usage:
 *   node scripts/bulk-upload-test.js [file_count] [rows_per_file]
 * 
 * Examples:
 *   node scripts/bulk-upload-test.js 10 1000
 *   node scripts/bulk-upload-test.js 100 10000
 */

import { createWriteStream } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { promises as fs } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const API_URL = process.env.API_URL || 'http://localhost:3000/api/v1/upload';
const fileCount = parseInt(process.argv[2] || '10', 10);
const rowsPerFile = parseInt(process.argv[3] || '1000', 10);

console.log('Bulk Upload Test');
console.log('================');
console.log(`Files to upload: ${fileCount}`);
console.log(`Rows per file: ${rowsPerFile}`);
console.log(`API URL: ${API_URL}`);
console.log('');

// Create temp directory for test files
const tempDir = join(__dirname, '..', 'temp-test-files');
await fs.mkdir(tempDir, { recursive: true });

/**
 * Generate a CSV file
 */
async function generateCSV(filePath, rows) {
  return new Promise((resolve, reject) => {
    const stream = createWriteStream(filePath, { encoding: 'utf8' });
    const headers = ['id', 'name', 'email', 'created_at'];
    
    stream.write(headers.join(',') + '\n');
    
    let written = 0;
    
    function writeBatch() {
      let canContinue = true;
      
      while (written < rows && canContinue) {
        const id = written + 1;
        const name = `User ${id}`;
        const email = `user${id}@example.com`;
        const createdAt = new Date().toISOString();
        
        const row = [id, name, email, createdAt].join(',') + '\n';
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
 * Upload a file to the API
 */
async function uploadFile(filePath) {
  const FormData = (await import('form-data')).default;
  const form = new FormData();
  const { createReadStream } = await import('fs');
  
  form.append('file', createReadStream(filePath));
  
  const response = await fetch(API_URL, {
    method: 'POST',
    body: form,
    headers: form.getHeaders(),
  });
  
  const data = await response.json();
  
  return {
    success: response.ok,
    status: response.status,
    data,
  };
}

// Generate and upload files
const results = {
  success: 0,
  failed: 0,
  errors: [],
};

const startTime = Date.now();

console.log('Generating and uploading files...\n');

for (let i = 1; i <= fileCount; i++) {
  const fileName = `test-file-${i}.csv`;
  const filePath = join(tempDir, fileName);
  
  try {
    // Generate CSV
    process.stdout.write(`[${i}/${fileCount}] Generating ${fileName}... `);
    await generateCSV(filePath, rowsPerFile);
    process.stdout.write('✓ ');
    
    // Upload file
    process.stdout.write('Uploading... ');
    const result = await uploadFile(filePath);
    
    if (result.success) {
      results.success++;
      console.log('✓ Success');
    } else {
      results.failed++;
      results.errors.push({ file: fileName, error: result.data });
      console.log(`✗ Failed: ${result.data.message || result.status}`);
    }
    
    // Clean up file
    await fs.unlink(filePath);
  } catch (error) {
    results.failed++;
    results.errors.push({ file: fileName, error: error.message });
    console.log(`✗ Error: ${error.message}`);
  }
}

// Clean up temp directory
await fs.rmdir(tempDir);

// Print summary
const elapsed = Date.now() - startTime;
console.log('\n' + '='.repeat(50));
console.log('Summary');
console.log('='.repeat(50));
console.log(`Total files: ${fileCount}`);
console.log(`Successful: ${results.success}`);
console.log(`Failed: ${results.failed}`);
console.log(`Time elapsed: ${elapsed}ms`);
console.log(`Average time per file: ${(elapsed / fileCount).toFixed(2)}ms`);

if (results.errors.length > 0) {
  console.log('\nErrors:');
  results.errors.forEach(({ file, error }) => {
    console.log(`  - ${file}: ${error}`);
  });
}

process.exit(results.failed > 0 ? 1 : 0);

