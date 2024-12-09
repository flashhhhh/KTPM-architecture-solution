# KTPM-architecture-solution

## Project architecture
- Unzip service: This service is used for unzipping a zip to multiple files.
- Ocr service: This service is used for transforming from an image to text.
- Translate service: This service is used for translating text from English to Vietnamese.
- PDF service: This service is used for converting text to a pdf file.
- Zip service: This service is used for zipping multiple files to a zip.

### Flow
```
[Request] -> [API Server] -> [Message Queue] -> [Unzip Service] -> [Message Queue] -> [Ocr Service] -> [Message Queue] ->  [Translate Service] -> [Message Queue] -> [PDF Service] -> [Message Queue] -> [Zip Service] -> [API Server] -> [Response]
```

## Key features
- Handling multiple requests asynchronously.
- Message queue fault tolerance.
- Scaling.

## Prerequisites
- NodeJS
- Apache Kafka

## Installation
1. **Clone the repository**
```bash
git clone https://github.com/flashhhhh/KTPM-architecture-solution.git
cd KTPM-architecture-solution
git checkout kafka-node
```

2. **Install NodeJS library**
```bash
npm i node-tesseract-ocr fs perf_hooks kafka-node path pdfkit crypto open-google-translator unzipper archiver express multer
```

3. **Create env file**

Create .env file. Then configure your number of instances for each service.

```bash
NUM_UNZIP=
NUM_OCR=
NUM_TRANSLATE=
NUM_PDF=
NUM_ZIP=
```

This instruction will demo with 1 instance for all services:

```bash
NUM_UNZIP=
NUM_OCR=
NUM_TRANSLATE=
NUM_PDF=
NUM_ZIP=
```

## Running the application

### 1. Start kafka server
```bash
cd $KAFKA_DIR

bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

#### Create 6 topics standing for 6 pipes connecting between services
```bash
bin/kafka-topics.sh --create --topic unzip-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic ocr-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic translate-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic pdf-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic zip-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic app-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

### 2. Start all services
Create 5 separate terminals, running 5 command blocks below to start 5 services:

```bash
cd utils/
node unzip.js
```

```bash
cd utils/
node ocr.js
```

```bash
cd utils/
node translate.js
```

```bash
cd utils/
node pdf.js
```

```bash
cd utils/
node zip.js
```

### 3. Launch the server
Create another terminal, then launch the webserver through this command:
```bash
node index.js
```

Now you can access the webserver through: http://localhost:3000/