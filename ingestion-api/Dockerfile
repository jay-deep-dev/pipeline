FROM node:20-alpine

WORKDIR /app

# Copy shared and install deps
COPY shared /app/shared
WORKDIR /app/shared
RUN npm install

# Copy API
COPY ingestion-api /app/ingestion-api
WORKDIR /app/ingestion-api
RUN npm install

EXPOSE 3000
CMD ["node", "src/index.js"]
