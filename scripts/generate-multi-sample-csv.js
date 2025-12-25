#!/usr/bin/env node

/**
 * Multi CSV Generator (Optimized)
 *
 * Generates a single base CSV file and copies it N times.
 * This is significantly faster than generating each file independently
 * and is ideal for load / stress testing ingestion pipelines.
 *
 * Usage:
 *   node scripts/generate-multi-sample-csv.js <rows_per_file> <file_count> [file_prefix]
 *
 * Example:
 *   node scripts/generate-multi-sample-csv.js 100000 50 users
 */

import {
  createWriteStream,
  copyFileSync,
  existsSync,
  unlinkSync
} from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

// ----------------------------------------------------
// Path setup
// ----------------------------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ----------------------------------------------------
// CLI arguments
// ----------------------------------------------------
const rowsPerFile = parseInt(process.argv[2] || '1000', 10);
const fileCount = parseInt(process.argv[3] || '1', 10);
const filePrefix = process.argv[4] || 'sample';

if (isNaN(rowsPerFile) || isNaN(fileCount)) {
  console.error('❌ Invalid arguments');
  console.error(
    'Usage: node generate-multi-sample-csv.js <rows_per_file> <file_count> [file_prefix]'
  );
  process.exit(1);
}

// ----------------------------------------------------
// CSV schema
// ----------------------------------------------------
const headers = ['id', 'name', 'email', 'created_at'];

// Base file path
const baseFilePath = join(__dirname, './genearted-csv', `${filePrefix}-base.csv`);

// ----------------------------------------------------
// Generate base CSV (only once)
// ----------------------------------------------------
function generateBaseFile() {
  return new Promise((resolve, reject) => {
    if (existsSync(baseFilePath)) {
      console.log(`⚡ Base file already exists → ${baseFilePath}`);
      return resolve();
    }

    console.log('📄 Generating base CSV file...');
    console.log(`• Rows: ${rowsPerFile}`);

    const stream = createWriteStream(baseFilePath, { encoding: 'utf8' });
    const startTime = Date.now();

    stream.write(headers.join(',') + '\n');

    let written = 0;

    function writeBatch() {
      let canContinue = true;

      while (written < rowsPerFile && canContinue) {
        const id = written + 1;
        const row = [
          id,
          `User ${id}`,
          `user${id}@example.com`,
          new Date().toISOString()
        ].join(',') + '\n';

        canContinue = stream.write(row);
        written++;

        if (written % 10000 === 0) {
          const progress = ((written / rowsPerFile) * 100).toFixed(1);
          process.stdout.write(
            `\r📄 Base file progress: ${progress}% (${written}/${rowsPerFile})`
          );
        }
      }

      if (written < rowsPerFile) {
        stream.once('drain', writeBatch);
      } else {
        stream.end();
      }
    }

    stream.on('finish', () => {
      console.log(
        `\n✅ Base CSV generated in ${Date.now() - startTime} ms`
      );
      resolve();
    });

    stream.on('error', reject);

    writeBatch();
  });
}

// ----------------------------------------------------
// Copy base file N times
// ----------------------------------------------------
function copyFiles() {
  console.log(`📄 Creating ${fileCount} copies...`);

  for (let i = 1; i <= fileCount; i++) {
    const targetPath = join(__dirname, '../uploads', `${filePrefix}-${i}.csv`);
    copyFileSync(baseFilePath, targetPath);
    console.log(`✅ ${filePrefix}-${i}.csv`);
  }
}

// ----------------------------------------------------
// Main execution
// ----------------------------------------------------
(async () => {
  console.log('----------------------------------');
  console.log('🚀 Multi CSV Generation Started');
  console.log(`• Rows per file : ${rowsPerFile}`);
  console.log(`• File count   : ${fileCount}`);
  console.log(`• Prefix       : ${filePrefix}`);
  console.log('----------------------------------');

  const totalStart = Date.now();

  await generateBaseFile();
  copyFiles();

  console.log('----------------------------------');
  console.log('🎉 All CSV files ready!');
  console.log(`⏱ Total time: ${Date.now() - totalStart} ms`);
})();
