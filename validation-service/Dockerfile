FROM node:20-alpine

WORKDIR /app

# Copy shared first
COPY shared ./shared

# Install shared dependencies
WORKDIR /app/shared
RUN npm install

# Copy validation-service
WORKDIR /app
COPY validation-service ./validation-service

# Install validation-service dependencies
WORKDIR /app/validation-service
RUN npm install

# Create uploads directory
RUN mkdir -p /app/uploads

CMD ["node", "src/index.js"]

