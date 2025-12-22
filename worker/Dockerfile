FROM node:20-alpine

WORKDIR /app

# Copy shared first
COPY shared ./shared

# Install shared dependencies
WORKDIR /app/shared
RUN npm install

# Copy worker
WORKDIR /app
COPY worker ./worker

# Install worker dependencies
WORKDIR /app/worker
RUN npm install

# Create uploads directory
RUN mkdir -p /app/uploads

CMD ["node", "src/index.js"]
