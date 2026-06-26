FROM node:18-slim

# Install Chromium for whatsapp-web.js (puppeteer)
RUN apt-get update && apt-get install -y \
    chromium \
    fonts-noto-color-emoji \
    fonts-noto-core \
    fonts-dejavu \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY . .

# Session data volume
VOLUME ["/app/.wwebjs_auth"]

EXPOSE 3000
CMD ["node", "src/index.js"]