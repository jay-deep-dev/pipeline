# Scripts Directory

This directory contains utility scripts for testing and working with the CSV Ingestion Pipeline.

## Available Scripts

Each script has its own documentation file:

1. **[generate-sample-csv.js](generate-sample-csv.md)** - Generate sample CSV files for testing
2. **[upload-test.sh](upload-test.md)** - Upload a single CSV file to test the API
3. **[bulk-upload-test.sh](bulk-upload-test.md)** - Upload multiple CSV files (bash version)
4. **[bulk-upload-test.js](bulk-upload-test-js.md)** - Upload multiple CSV files (Node.js version)

## Quick Reference

### Generate Test CSV
```bash
node scripts/generate-sample-csv.js 1000 sample.csv
```

### Upload Single File
```bash
./scripts/upload-test.sh sample.csv
```

### Bulk Upload Test
```bash
./scripts/bulk-upload-test.sh 10 1000
```

## Documentation

For detailed documentation on each script, see:
- [generate-sample-csv.md](generate-sample-csv.md)
- [upload-test.md](upload-test.md)
- [bulk-upload-test.md](bulk-upload-test.md)
- [bulk-upload-test-js.md](bulk-upload-test-js.md)

## Quick Start

```bash
# 1. Generate a test CSV
node scripts/generate-sample-csv.js 1000 test.csv

# 2. Upload it
./scripts/upload-test.sh test.csv

# 3. Test with multiple files
./scripts/bulk-upload-test.sh 10 1000
```

## Notes

- Scripts are one-time use utilities for testing
- Each script has its own detailed documentation file
- See individual documentation files for usage, examples, and troubleshooting

