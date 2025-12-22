# Generate Sample CSV Script

Generates sample CSV files for testing the ingestion pipeline.

## Usage

```bash
node scripts/generate-sample-csv.js [rows] [filename]
```

## Parameters

- `rows` (optional): Number of rows to generate (default: 1000)
- `filename` (optional): Output filename (default: `sample-{rows}-rows.csv`)

## Examples

```bash
# Generate a CSV with 1000 rows
node scripts/generate-sample-csv.js 1000 sample.csv

# Generate a CSV with 10,000 rows
node scripts/generate-sample-csv.js 10000 large-sample.csv

# Generate a CSV with 1 million rows (for performance testing)
node scripts/generate-sample-csv.js 1000000 million-rows.csv

# Generate a CSV with 10 million rows (maximum requirement)
node scripts/generate-sample-csv.js 10000000 max-rows.csv
```

## Output Format

The script generates a CSV file with the following structure:

```csv
id,name,email,created_at
1,User 1,user1@example.com,2024-01-01T00:00:00.000Z
2,User 2,user2@example.com,2024-01-01T00:00:01.000Z
...
```

## Features

- **Streaming Generation:** Uses Node.js streams for memory efficiency
- **Progress Indicator:** Shows progress during generation (every 10,000 rows)
- **Performance Stats:** Displays generation time and statistics
- **Upload Command:** Prints curl command for easy upload after generation

## Output Location

Files are written to the project root directory (same level as `scripts/` folder).

## Notes

- Large files (millions of rows) may take several minutes to generate
- The script uses streaming to handle large files efficiently
- Progress is displayed every 10,000 rows
- After completion, the script prints a curl command for easy upload

## Example Output

```
Generating CSV file: sample.csv
Rows: 1000
Output: /path/to/project/sample.csv
Progress: 100.00% (1000/1000 rows)

CSV file generated successfully!
File: /path/to/project/sample.csv
Rows: 1000
Time: 45ms

To upload this file, run:
curl -X POST http://localhost:3000/api/v1/upload -F "file=@sample.csv"
```

## Requirements

- Node.js 20+ (or compatible version)
- Write permissions in project directory

