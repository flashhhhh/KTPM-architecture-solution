const fs = require("fs");
const path = require("path");
const unzipper = require("unzipper");
const kafkaNode = require("kafka-node");

require("dotenv").config({ path: path.resolve(__dirname, "../.env") });
const NUM_OCR = process.env.NUM_OCR || 1;

// Initialize Kafka client and producer
const client = new kafkaNode.KafkaClient({ kafkaHost: "localhost:9092" });
const producer = new kafkaNode.Producer(client);

// Consumer for the "unzip-topic"
const consumer = new kafkaNode.Consumer(
  client,
  [{ topic: "unzip-topic", partition: 0 }],
  { autoCommit: true }
);

consumer.on("error", (err) => {
    console.error("Consumer error:", err);
});

producer.on("ready", () => {
    console.log("Producer is ready");
});

producer.on("error", (err) => {
    console.error("Producer error:", err);
});


// Unzip function
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

// Handle messages from the consumer
consumer.on("message", async (message) => {
    console.log("Message received:", message);

    try {
        const { requestId, inputPath, pdfFolder, fileType } = JSON.parse(message.value);

        console.log(`Request ID: ${requestId} was received by Unzip service.`);

        if (fileType === ".zip") {
            const extractFolder = path.join(__dirname, "../", "uploads", `extracted_${requestId}`);
            fs.mkdirSync(extractFolder, { recursive: true });

            await unzipFile(inputPath, extractFolder);
            const extractedFiles = fs.readdirSync(extractFolder);
            const numFiles = extractedFiles.length;

            console.log("Number of files extracted:", numFiles);

            for (const file of extractedFiles) {
                const filePath = path.join(extractFolder, file);
                if (fs.lstatSync(filePath).isFile()) {
                    const payloads = [
                        {
                            topic: "ocr-topic",
                            messages: JSON.stringify({ requestId, filePath, file, pdfFolder, numFiles }),
                        },
                    ];
                    payloads[0].partition = Math.floor(Math.random() * NUM_OCR);

                    producer.send(payloads, (err, data) => {
                        if (err) {
                            console.error(`Error sending message for file ${file}:`, err);
                        } else {
                            console.log(`Request ID: ${requestId} was sent to OCR service, partition ${payloads[0].partition}. File ${file} was sent.`);
                        }
                    });
                }
            }
        }
    } catch (error) {
        console.error("Error processing message:", error);
    }
});