#!/usr/bin/env node

/**
 * Sample CSV Generator
 * 
 * Generates sample CSV files for testing the ingestion pipeline.
 * Creates CSV files with configurable number of rows and columns.
 * 
 * Usage:
 *   node scripts/generate-sample-csv.js [rows] [filename]
 * 
 * Examples:
 *   node scripts/generate-sample-csv.js 1000 sample.csv
 *   node scripts/generate-sample-csv.js 1000000 large-sample.csv
 */

import { createWriteStream } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Parse command line arguments
const rows = parseInt(process.argv[2] || '1000', 10);
const filename = process.argv[3] || `sample-${rows}-rows.csv`;
const outputPath = join(__dirname, '..', filename);

console.log(`Generating CSV file: ${filename}`);
console.log(`Rows: ${rows}`);
console.log(`Output: ${outputPath}`);

// CSV headers
const headers = ['id', 'name', 'email', 'created_at'];

// Create write stream
const stream = createWriteStream(outputPath, { encoding: 'utf8' });

// Write headers
stream.write(headers.join(',') + '\n');

// Generate rows
let written = 0;
const startTime = Date.now();

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
    
    // Progress indicator
    if (written % 10000 === 0) {
      const progress = ((written / rows) * 100).toFixed(2);
      process.stdout.write(`\rProgress: ${progress}% (${written}/${rows} rows)`);
    }
  }
  
  if (written < rows) {
    // Buffer is full, wait for drain
    stream.once('drain', writeBatch);
  } else {
    // All rows written
    stream.end();
  }
}

stream.on('finish', () => {
  const elapsed = Date.now() - startTime;
  console.log(`\n\nCSV file generated successfully!`);
  console.log(`File: ${outputPath}`);
  console.log(`Rows: ${written}`);
  console.log(`Time: ${elapsed}ms`);
  console.log(`\nTo upload this file, run:`);
  console.log(`curl -X POST http://localhost:3000/api/v1/upload -F "file=@${outputPath}"`);
});

stream.on('error', (error) => {
  console.error('Error generating CSV:', error);
  process.exit(1);
});

// Start writing
writeBatch();

