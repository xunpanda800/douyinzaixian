FROM node:22-alpine

ARG COMMIT_SHA=dev
ARG VERSION=dev

RUN apk add --no-cache docker-cli docker-cli-compose

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev && npm cache clean --force

COPY server/ ./server/
COPY public/ ./public/
COPY version.json docker-compose.yml ./
RUN node -e "const v=require('./version.json'); v.version='$VERSION'; v.commit='$COMMIT_SHA'; require('fs').writeFileSync('version.json',JSON.stringify(v))"

RUN mkdir -p data

EXPOSE 3000

CMD ["node", "server/index.js"]
