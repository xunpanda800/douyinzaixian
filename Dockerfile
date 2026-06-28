FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev && npm cache clean --force

COPY server/ ./server/
COPY public/ ./public/

RUN mkdir -p data

EXPOSE 3000

CMD ["node", "server/index.js"]
