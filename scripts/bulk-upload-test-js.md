# Bulk Upload Test Script (Node.js)

Node.js version of the bulk upload test script. Provides the same functionality as the bash script but with better error handling and cross-platform compatibility.

## Usage

```bash
node scripts/bulk-upload-test.js [file_count] [rows_per_file]
```

## Parameters

- `file_count` (optional): Number of files to upload (default: 10)
- `rows_per_file` (optional): Number of rows per file (default: 1000)

## Examples

```bash
# Upload 10 files with 1000 rows each
node scripts/bulk-upload-test.js 10 1000

# Upload 100 files with 10,000 rows each
node scripts/bulk-upload-test.js 100 10000
```

## Environment Variables

- `API_URL`: API endpoint URL (default: `http://localhost:3000/api/v1/upload`)

## Advantages over Bash Script

- ✅ Better error handling
- ✅ Cross-platform compatibility (Windows, macOS, Linux)
- ✅ More detailed error messages
- ✅ Uses Node.js fetch API (Node.js 18+)
- ✅ Better handling of async operations

## Requirements

- Node.js 18+ (for fetch API)
- API service running

## Output

Similar to bash script but with Node.js-specific formatting:

```
Bulk Upload Test
================
Files to upload: 10
Rows per file: 1000
API URL: http://localhost:3000/api/v1/upload

Generating and uploading files...

[1/10] Generating test-file-1.csv... ✓ Uploading... ✓ Success
[2/10] Generating test-file-2.csv... ✓ Uploading... ✓ Success
...

==================================================
Summary
==================================================
Total files: 10
Successful: 10
Failed: 0
Time elapsed: 45234ms
Average time per file: 4523.40ms
```

## When to Use

- **Use Node.js version if:**
  - You're on Windows
  - You prefer Node.js tooling
  - You need better error handling
  - You want cross-platform compatibility

- **Use Bash version if:**
  - You're on Unix-like system (macOS, Linux)
  - You prefer shell scripts
  - You want simpler dependencies

## Troubleshooting

### Node.js Version

Ensure Node.js 18+ is installed:
```bash
node --version  # Should be 18.0.0 or higher
```

### Fetch API Not Available

If using Node.js < 18, install `node-fetch`:
```bash
npm install node-fetch
```

Then modify the script to import node-fetch instead of using native fetch.

### Connection Errors

- Verify API is running: `docker-compose ps`
- Check API URL is correct
- Review error messages for details

## Notes

- Uses native fetch API (Node.js 18+)
- Better error messages than bash version
- Handles async operations more gracefully
- Temporary files are cleaned up automatically

