
✅ Full Markdown Content
# Multi CSV Sample Generator (Optimized)

This script generates large CSV datasets for testing high-volume ingestion pipelines.
It creates **one base CSV file** and then **copies it N times**, which is significantly
faster than regenerating each file.

This approach is commonly used in production systems for **load testing**,
**Kafka consumer benchmarking**, and **CI pipelines**.

---

## 📦 Location



scripts/generate-multi-sample-csv.js


---

## 🚀 Usage

```bash
node scripts/generate-multi-sample-csv.js <rows_per_file> <file_count> [file_prefix]

🔧 Arguments
Argument	Description	Required
rows_per_file	Number of rows per CSV file	✅ Yes
file_count	Number of CSV files to generate	✅ Yes
file_prefix	Prefix for generated files	❌ No
📄 Examples

Generate 10 files, each with 1,000 rows:

node scripts/generate-multi-sample-csv.js 1000 10


Generate 100 files, each with 100,000 rows:

node scripts/generate-multi-sample-csv.js 100000 100


Generate 5 files with a custom prefix:

node scripts/generate-multi-sample-csv.js 50000 5 users

📁 Output Structure
users-base.csv        # Base generated file
users-1.csv
users-2.csv
users-3.csv
...
users-5.csv

🧠 Why Copy Instead of Regenerate?
✅ Benefits

Extremely fast

Low CPU usage

Ideal for:

Kafka throughput testing

Consumer scaling

Database write benchmarking

CI / automation pipelines

⚠ Trade-offs

Files are identical

Same timestamps

Not suitable for validating:

Data correctness

Schema validation

Deduplication logic

DLQ behavior

🧪 Recommended Testing Strategy
Scenario	Recommended Mode
Load / stress testing	✅ Copy mode
Kafka consumer benchmarking	✅ Copy mode
Validation / DLQ testing	❌ Regenerate
Debugging bad data	❌ Regenerate
🔗 Upload Example
curl -X POST http://localhost:3000/api/v1/upload \
  -F "file=@users-1.csv"

🏗 Designed For

High-volume CSV ingestion pipelines

Kafka + consumer group testing

Batch database inserts

Distributed worker systems

Production-like load simulations
