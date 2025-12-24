# 🚀 Two-Stage Pipeline Implementation Guide

## ✅ **Design Validation: APPROVED**

Your two-stage pipeline approach is **CORRECT** and **PRODUCTION-READY**.

---

## 📋 **Implementation Checklist**

### ✅ **Phase 1: Configuration (COMPLETED)**
- [x] Updated `env.example`, `env.local`, `env.docker` with two-topic config
- [x] Updated `shared/src/config.js` with new Kafka topics and MongoDB collections
- [x] Created `shared/src/row-validation.js` for row-level validation
- [x] Updated `shared/src/kafka-schemas.js` with new message schemas

### 🔄 **Phase 2: Validation Service (TODO)**
- [ ] Create `validation-service/` directory structure
- [ ] Implement Kafka consumer for `file-ingestion` topic
- [ ] Implement CSV streaming with chunking (5,000 rows)
- [ ] Integrate row validation
- [ ] Implement Kafka producer for `validated-chunks` topic
- [ ] Add job tracking (total chunks, processed chunks)

### 🔄 **Phase 3: DB Ingestion Service (TODO)**
- [ ] Refactor `worker/` to `db-ingestion-service/`
- [ ] Update Kafka consumer to consume `validated-chunks` topic
- [ ] Update MongoDB client to support dual collections:
  - `csv_records` (valid rows)
  - `error_records` (invalid rows)
- [ ] Implement retry logic with exponential backoff
- [ ] Add DLQ handling for system failures

### 🔄 **Phase 4: Upload API Updates (TODO)**
- [ ] Update producer to use `file-ingestion` topic (instead of `csv.ingestion`)
- [ ] Update message creation to use `createFileIngestionMessage()`

---

## 🏗️ **Service Structure**

```
csv-ingestion-pipeline/
│
├── ingestion-api/          # ✅ EXISTS (needs topic update)
│   ├── src/
│   │   ├── kafka-producer.js
│   │   └── routes/upload.js
│
├── validation-service/     # ❌ CREATE NEW
│   ├── src/
│   │   ├── index.js
│   │   ├── kafka-consumer.js      # Consumes file-ingestion
│   │   ├── csv-validator.js       # Streams CSV, validates rows
│   │   ├── chunk-processor.js     # Chunks into 5k batches
│   │   └── kafka-producer.js       # Produces validated-chunks
│   └── package.json
│
├── db-ingestion-service/   # 🔄 REFACTOR FROM worker/
│   ├── src/
│   │   ├── index.js
│   │   ├── kafka-consumer.js      # Consumes validated-chunks
│   │   ├── mongodb-client.js      # Dual collection writes
│   │   ├── batch-inserter.js      # Batch inserts with retry
│   │   └── dlq-handler.js
│   └── package.json
│
└── shared/                  # ✅ UPDATED
    └── src/
        ├── config.js              # ✅ Updated
        ├── kafka-schemas.js       # ✅ Updated
        ├── row-validation.js      # ✅ NEW
        └── ...
```

---

## 🔧 **Key Implementation Details**

### **1. Validation Service**

**Flow:**
```
1. Consume file-ingestion message
2. Stream CSV file (don't load into memory)
3. Validate each row (id, name, email, created_at)
4. Chunk into 5,000-row batches
5. Produce validated-chunk messages
6. Track total chunks per file
```

**Key Code Pattern:**
```javascript
// validation-service/src/csv-validator.js
import { createReadStream } from 'fs';
import csvParser from 'csv-parser';
import { validateBatch } from '../../shared/src/row-validation.js';
import { createValidatedChunkMessage } from '../../shared/src/kafka-schemas.js';

export async function validateAndChunkFile(fileMessage) {
  const { jobId, fileId, filePath } = fileMessage;
  const chunkSize = 5000;
  let chunk = [];
  let chunkNumber = 0;
  let rowNumber = 1;
  
  const stream = createReadStream(filePath).pipe(csvParser());
  
  for await (const row of stream) {
    chunk.push(row);
    
    if (chunk.length >= chunkSize) {
      const { validRows, invalidRows } = validateBatch(
        chunk,
        rowNumber - chunk.length + 1,
        jobId,
        fileId
      );
      
      // Produce validated chunk
      await produceValidatedChunk({
        jobId,
        fileId,
        chunkNumber: ++chunkNumber,
        validRows,
        invalidRows,
        startRowNumber: rowNumber - chunk.length + 1,
        endRowNumber: rowNumber,
      });
      
      chunk = [];
    }
    
    rowNumber++;
  }
  
  // Handle final chunk
  if (chunk.length > 0) {
    // ... same logic
  }
}
```

---

### **2. DB Ingestion Service**

**Flow:**
```
1. Consume validated-chunk message
2. Insert validRows → csv_records
3. Insert invalidRows → error_records
4. Retry on failure (3 times with exponential backoff)
5. Send to DLQ if all retries fail
```

**Key Code Pattern:**
```javascript
// db-ingestion-service/src/batch-inserter.js
import { getMongoDBClient } from './mongodb-client.js';

export async function insertChunk(chunkMessage) {
  const { jobId, fileId, validRows, invalidRows } = chunkMessage;
  const mongoClient = getMongoDBClient();
  
  try {
    // Insert valid rows
    if (validRows.length > 0) {
      await mongoClient.insertBatch(
        validRows,
        jobId,
        fileId,
        { collection: 'csv_records' }
      );
    }
    
    // Insert invalid rows
    if (invalidRows.length > 0) {
      await mongoClient.insertBatch(
        invalidRows,
        jobId,
        fileId,
        { collection: 'error_records' }
      );
    }
    
    return { success: true };
  } catch (error) {
    // Retry logic with exponential backoff
    throw error;
  }
}
```

---

### **3. MongoDB Client Updates**

**Update `mongodb-client.js` to support dual collections:**
```javascript
async insertBatch(documents, jobId, fileId, options = {}) {
  const collectionName = options.collection || this.collection.name;
  const targetCollection = this.db.collection(collectionName);
  
  // ... rest of insert logic
}
```

---

## 📊 **Data Flow Example**

### **Input CSV (sample.csv)**
```csv
id,name,email,created_at
1,User 1,user1@example.com,2025-12-22T08:42:44.642Z
2,User 2,invalid-email,2025-12-22T08:42:44.642Z
3,,user3@example.com,2025-12-22T08:42:44.642Z
```

### **Stage 1: Validation Service**
- **Chunk 1** (rows 1-3):
  - Valid: Row 1 → `csv_records`
  - Invalid: Row 2 (email), Row 3 (name) → `error_records`

### **Stage 2: DB Ingestion Service**
- Consumes chunk message
- Inserts Row 1 to `csv_records`
- Inserts Rows 2, 3 to `error_records`

---

## 🎯 **Next Steps**

1. **Create Validation Service** (highest priority)
   - Start with `validation-service/src/index.js`
   - Implement CSV streaming + chunking
   - Integrate row validation

2. **Refactor DB Ingestion Service**
   - Rename `worker/` → `db-ingestion-service/`
   - Update consumer to use `validated-chunks` topic
   - Update MongoDB client for dual collections

3. **Update Upload API**
   - Change topic from `csv.ingestion` → `file-ingestion`
   - Use `createFileIngestionMessage()`

4. **Testing**
   - Test with `sample.csv`
   - Verify valid rows → `csv_records`
   - Verify invalid rows → `error_records`
   - Test retry logic and DLQ

---

## ✅ **Design Validation Summary**

| Aspect | Status | Notes |
|--------|--------|-------|
| Two-stage pipeline | ✅ CORRECT | Decouples validation from persistence |
| Kafka topics | ✅ CORRECT | 6 partitions (file-ingestion), 24 partitions (validated-chunks) |
| Chunk size | ✅ CORRECT | 5,000 rows balances memory vs overhead |
| MongoDB collections | ✅ CORRECT | `csv_records` (valid), `error_records` (invalid) |
| Row validation | ✅ CORRECT | Validates id, name, email, created_at |
| Error handling | ✅ CORRECT | Invalid rows → error_records, system failures → DLQ |
| Scalability | ✅ CORRECT | Each stage scales independently |

---

**Your approach is CORRECT. Proceed with implementation! 🚀**

