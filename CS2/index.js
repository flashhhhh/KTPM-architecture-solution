const express = require("express");
const multer = require("multer");
const crypto = require("crypto");
const path = require("path");
const fs = require("fs");
const { performance } = require("perf_hooks");
const kafka = require('kafka-node');

require("dotenv").config({ path: path.resolve(__dirname, "../.env") });
const NUM_UNZIP = process.env.NUM_UNZIP || 1;

const PORT = 3000;

const app = express();
const upload = multer({ dest: "uploads/" });

app.use(express.static("public"));

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092', autoConnect: true });
const producer = new kafka.Producer(client);

producer.on('ready', () => {
    console.log("Kafka Producer is ready.");
});

producer.on('error', (err) => {
    console.error("Producer error:", err);
});

const consumer = new kafka.Consumer(
    client,
    [{ topic: 'app-topic', partition: 0 }],
    { autoCommit: true, groupId: 'app-group' }
);

const isFinished = new Map();

app.post("/upload", upload.single("file"), async (req, res) => {
    const startTime = performance.now();

    const inputPath = req.file.path;
    const requestId = crypto.randomUUID();
    const pdfFolder = path.join(__dirname, "pdf", `pdf_${requestId}`);
    fs.mkdirSync(pdfFolder, { recursive: true });
    const fileType = path.extname(req.file.originalname).toLowerCase();

    console.log("Input path: ", inputPath);

    const messagePayload = JSON.stringify({
        requestId,
        inputPath: "../" + inputPath,
        pdfFolder,
        fileType,
    });
    messagePayload.partition = Math.floor(Math.random() * NUM_UNZIP);

    producer.send([{
        topic: 'unzip-topic',
        messages: messagePayload,
    }], (err) => {
        if (err) {
            console.error("Error sending message:", err);
            return res.status(500).json({ error: "Failed to send message to Kafka." });
        }
        console.log(`Request ID: ${requestId} was sent to Unzip service.`);
    });

    /*
        Wait for the message to be processed by the services
    */

    isFinished.set(requestId, false);
    while (!isFinished.get(requestId)) {
        await new Promise(resolve => setTimeout(resolve, 500));
    }

    isFinished.delete(requestId);

    // Delete file and folder
    fs.unlinkSync(inputPath);
    if (fs.existsSync(pdfFolder))
        fs.rm(pdfFolder, { recursive: true }, (err) => {
            if (err) {
                console.error("Error removing folder:", err);
            }
        });

    const extractFolder = path.join(__dirname, "uploads", `extracted_${requestId}`);

    if (fs.existsSync(extractFolder))
        fs.rm(extractFolder, { recursive: true }, (err) => {
            if (err) {
                console.error("Error removing folder:", err);
            }
        });

    console.log("Time taken for request ID: ", requestId, " is ", (performance.now() - startTime) / 1000, " seconds");

    res.json({ downloadLink: `/download/${requestId}` });
});

app.get("/downloadFile/:requestId/:filename", (req, res) => {
    const { requestId, filename } = req.params;
    const zipPath = path.join(__dirname, "pdf", `pdf_${requestId}`, filename);
    const pdfFolder = path.join(__dirname, "pdf", `pdf_${requestId}`);
    res.download(zipPath, (err) => {
        if (err) {
            console.error("Error sending PDF:", err);
        }
        fs.unlinkSync(zipPath);
        fs.rm(pdfFolder, { recursive: true }, (err) => {
            if (err) {
                console.error("Error removing folder:", err);
            } else {
                console.log(`Folder ${pdfFolder} removed.`);
            }
        });
    });
});

app.get("/download/:requestId", (req, res) => {
    const requestId = req.params.requestId;
    const zipPath = path.join(__dirname, "output", `${requestId}`, `processed_file.zip`);
    const outputPath = path.join(__dirname, "output", `${requestId}`);
    res.download(zipPath, (err) => {
        if (err) {
            console.error("Error sending PDF:", err);
        }
        
        // Delete the zip file
        fs.unlinkSync(zipPath);

        fs.rm(outputPath, { recursive: true }, (err) => {
            if (err) {
                console.error("Error removing folder:", err);
            } else {
                console.log(`Folder ${outputPath} removed.`);
            }
        });
    });
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});

consumer.on('message', (message) => {
    const { requestId, zipPath } = JSON.parse(message.value);
    console.log(`Request ID: ${requestId} was completed by App service on process ${process.pid}`);
    isFinished.set(requestId, true);
});

consumer.on('error', (err) => {
    console.error("Error in consumer:", err);
});
