FROM node:18-slim

RUN apt-get update && apt-get install -y curl python3 git

RUN curl -fsSL https://raw.githubusercontent.com/arduino/arduino-cli/master/install.sh | sh
ENV PATH="${PATH}:/bin"

RUN arduino-cli config init && \
    arduino-cli core update-index --additional-urls https://dl.espressif.com/dl/package_esp32_index.json && \
    arduino-cli core install arduino:avr && \
    arduino-cli core install esp32:esp32 --additional-urls https://dl.espressif.com/dl/package_esp32_index.json

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install
COPY . .

EXPOSE 3000
CMD ["node", "server.js"]