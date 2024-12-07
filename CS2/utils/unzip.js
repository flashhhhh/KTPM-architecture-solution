const fs = require("fs");
const path = require("path");
const unzipper = require("unzipper");
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "unzip-group" });

async function connectProducer() {
    await producer.connect();
}

async function unzipFile(zipPath, extractFolder) {
    return new Promise((resolve, reject) => {
        if (!fs.existsSync(zipPath)) {
            return reject(new Error(`File not found: ${zipPath}`));
        }

        fs.createReadStream(zipPath)
            .pipe(unzipper.Extract({ path: extractFolder }))
            .on("close", () => resolve())
            .on("error", (err) => reject(err));
    });
}

async function consumeMessage() {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: "unzip-topic" });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const { requestId, inputPath, pdfFolder, fileType, startTime } = JSON.parse(
                message.value.toString()
            );

            console.log(`Request ID: ${requestId} was received by Unzip service on process ${process.pid}`);

            if (fileType === ".zip") {
                const extractFolder = path.join(__dirname, "../", "uploads", `extracted_${requestId}`);
                fs.mkdirSync(extractFolder, { recursive: true });

                await unzipFile(inputPath, extractFolder);
                const extractedFiles = fs.readdirSync(extractFolder);
                const numFiles = extractedFiles.length;

                for (const file of extractedFiles) {
                    const filePath = path.join(extractFolder, file);
                    if (fs.lstatSync(filePath).isFile()) {
                        await producer.send({
                            topic: "ocr-topic",
                            messages: [{ value: JSON.stringify({ requestId, filePath, file, pdfFolder, numFiles, startTime }) }],
                        });
                    }
                }
            }
        },
    });
}

// Infinitely consume message
consumeMessage().catch(console.error);