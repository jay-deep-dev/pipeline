
# Optimized Bulk Upload Concurrent Test Script (Node.js)

This script is designed to **test the CSV ingestion API** by generating and uploading multiple CSV files efficiently. It uses **streaming** for memory-efficient CSV creation and supports **concurrent uploads** for higher throughput.

---

## Features

* **Streaming CSV generation** — avoids loading large files fully into memory
* **Concurrent uploads** — configurable concurrency for multiple files
* **Automatic cleanup** — temporary CSV files are deleted after upload
* **Detailed reporting** — summary of success, failures, throughput, and errors

---

## Usage

```bash
node scripts/bulk-upload-test.js [file_count] [rows_per_file] [concurrency]
```

### Examples

* Sequential upload (1 file at a time)

```bash
node scripts/bulk-upload-test.js 10 1000
```

* Concurrent upload (3 files at a time)

```bash
node scripts/bulk-upload-test.js 10 1000 3
```

* Large batch upload (5 files at a time, 10k rows each)

```bash
node scripts/bulk-upload-test.js 100 10000 5
```

---

## Environment Variables

| Variable  | Default                               | Description                  |
| --------- | ------------------------------------- | ---------------------------- |
| `API_URL` | `http://localhost:3000/api/v1/upload` | URL of the CSV ingestion API |

---

## How It Works

### 1. Temporary Directory

* Creates a temporary folder `temp-test-files` in the project root
* All test CSV files are generated here

### 2. CSV Generation

* Generates CSV files using **streaming** for memory efficiency
* Each row contains:

  ```
  id,name,email,created_at
  ```
* A single timestamp is used for all rows in a file to speed up generation

### 3. Upload to API

* Uses `http` module and `form-data` to send files
* Handles API response and logs success or failure

### 4. Concurrency

* Supports configurable concurrency for batch uploads
* Files are processed in **batches**
* Small delay (`50ms`) between batches to reduce API overload

### 5. Cleanup

* Uploaded CSV files are automatically deleted
* Temporary folder is removed at the end

---

## Script Flow

```
Generate CSV file → Upload to API → Delete file → Record result
```

* **Concurrent batches** improve throughput
* Errors are logged individually per file

---

## Output Summary

At the end, the script prints:

* Total files attempted
* Successful uploads
* Failed uploads
* Concurrency used
* Time elapsed
* Average time per file
* Total rows processed
* Throughput (rows/sec)
* List of errors (if any)

Example:

```
==================================================
Summary
==================================================
Total files: 10
Successful: 10
Failed: 0
Concurrency: 3
Time elapsed: 4500ms (4.50s)
Average time per file: 450.00ms
Total rows processed: 10,000
Throughput: ~2222 rows/second
```

---

## Error Handling

* Any file generation or upload error is captured and logged
* Temporary CSV files are cleaned up even on errors
* Script exits with code `1` if any uploads fail, otherwise `0`

---

## Notes / Tips

* Use **streaming CSV generation** for large files (millions of rows)
* Adjust **concurrency** based on API capacity
* Ensure **API_URL** is reachable and supports `multipart/form-data` uploads
* Ideal for **performance testing**, **load testing**, or **integration tests**

---

This documentation can be saved as:

