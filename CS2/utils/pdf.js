const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require("path");
const { performance } = require('perf_hooks');
const kafkaNode = require('kafka-node');

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

consumer.on('message', async (message) => {
    try {
        const { requestId, text, file, pdfFolder, numFiles, isFailed } = JSON.parse(message.value);

        const startTime = performance.now();
        const pdfPath = createPDF(text, file, pdfFolder);

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

        producer.send(payloads, (err, data) => {
            if (err) {
                console.error("Error sending message:", err);
            } else {
                console.log("Message sent to zip-topic:", data);
            }
        });
    } catch (error) {
        console.error("Error processing message:", error);
    }
});

consumer.on('error', (err) => {
    console.error("Consumer error:", err);
});
