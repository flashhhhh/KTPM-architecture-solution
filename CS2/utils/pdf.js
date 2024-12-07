const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require("path");
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'pdf-group' });

function createPDF(text, file, pdfFolder) {
    // const outputDir = "output";
    const pdfName = path.basename(file, path.extname(file)) + ".pdf";
    // const pdfPath = `${outputDir}/${pdfName}`;
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

async function consumeMessage() {
    await consumer.connect();
    await consumer.subscribe({ topic: "pdf-topic" });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const { requestId, text, file, pdfFolder, numFiles } = JSON.parse(
                message.value.toString()
            );

            const pdfPath = createPDF(text, file, pdfFolder);

            console.log(`Request ID: ${requestId} was received by PDF service on process ${process.pid}`);

            await producer.connect();
            await producer.send({
                topic: "zip-topic",
                messages: [{ key: requestId, value: JSON.stringify({ requestId, pdfPath, numFiles }) }],
            });
        },
    });
}

consumeMessage().catch(console.error);