const PDFDocument = require('pdfkit');
const fs = require('fs');
const crypto = require('crypto');
const path = require("path");
const { performance } = require('perf_hooks');
const kafkaNode = require('kafka-node');

require("dotenv").config({ path: path.resolve(__dirname, "../.env") });
const NUM_ZIP = process.env.NUM_ZIP || 1;

const args = process.argv.slice(2);
const partitionId = parseInt(args[0]);

// Initialize Kafka client and producer
const client = new kafkaNode.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new kafkaNode.Producer(client);

// Consumer for the "pdf-topic"
const consumer = new kafkaNode.Consumer(
  client,
  [{ topic: "pdf-topic", partition: partitionId }],
  { autoCommit: true }
);

// Function to create a PDF
function createPDF(text, file, pdfFolder) {
    const pdfName = path.basename(file, path.extname(file)) + ".pdf";
    const pdfPath = path.join(pdfFolder, pdfName);

    if (!fs.existsSync(pdfFolder)) {
        fs.mkdirSync(pdfFolder);
    }

    const doc = new PDFDocument();
    doc.pipe(fs.createWriteStream(pdfPath));
    try {
        doc.font('font/Roboto-Regular.ttf');
    } catch (err) {
        doc.font('../font/Roboto-Regular.ttf');
    }
    doc.fontSize(14).text(text, 100, 100);
    doc.end();

    return pdfPath;
}

producer.on('ready', () => {
    console.log("Producer is ready");
});

producer.on('error', (err) => {
    console.error("Producer error:", err);
});

const map = new Map();
const timing = new Map();

consumer.on('message', async (message) => {
    try {
        const { requestId, text, file, pdfFolder, numFiles, isFailed } = JSON.parse(message.value);

        const startTime = performance.now();
        const pdfPath = createPDF(text, file, pdfFolder);

        if (map.has(requestId)) {
            map.set(requestId, map.get(requestId) + 1);
            timing.get(requestId).push(performance.now() - startTime);
        } else {
            map.set(requestId, 1);
            timing.set(requestId, [performance.now() - startTime]);
        }

        console.log(
            `Request ID: ${requestId} was received by PDF service. File ${file} was processed in ${
                performance.now() - startTime
            } ms.`
        );

        const payloads = [
            {
                topic: "zip-topic",
                messages: JSON.stringify({ requestId, pdfPath, numFiles, file, isFailed }),
                key: requestId
            }
        ];
        
        const hash = crypto.createHash('md5').update(requestId).digest('hex');
        const partition = parseInt(hash, 16) % NUM_ZIP;
        payloads[0].partition = partition;

        producer.send(payloads, (err, data) => {
            if (err) {
                console.error("Error sending message:", err);
            } else {
                console.log("Message sent to zip-topic:", data);
            }
        });

        if (map.get(requestId) === numFiles) {
            const avgTime = timing.get(requestId).reduce((a, b) => a + b, 0) / numFiles;
            console.log(`Average pdf processing time for request ID ${requestId}: ${avgTime} ms`);
        }

    } catch (error) {
        console.error("Error processing message:", error);
    }
});

consumer.on('error', (err) => {
    console.error("Consumer error:", err);
});
