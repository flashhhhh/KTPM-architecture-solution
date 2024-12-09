const tesseract = require("node-tesseract-ocr");
const fs = require("fs");
const { performance } = require("perf_hooks");
const kafkaNode = require("kafka-node");
const path = require("path");

require("dotenv").config({ path: path.resolve(__dirname, "../.env") });
const NUM_TRANSLATE = process.env.NUM_TRANSLATE || 1;

const args = process.argv.slice(2);
const partitionId = parseInt(args[0]);

const client = new kafkaNode.KafkaClient({ kafkaHost: "localhost:9092" });
const producer = new kafkaNode.Producer(client);
const consumer = new kafkaNode.Consumer(
  client,
  [{ topic: "ocr-topic", partition: partitionId }],
  { autoCommit: true}
);

async function image2text(path) {
  return await tesseract.recognize(path, {
    lang: "eng",
  });
}

producer.on("ready", () => {
  console.log("Producer is ready");
});

producer.on("error", (err) => {
  console.error("Producer error:", err);
});

const map = new Map();
const timing = new Map();

const messageQueue = [];
let isProcessing = false;

consumer.on("message", (message) => {
  messageQueue.push(message); // Add message to the queue
  processQueue(); // Process the queue
});

async function processQueue() {
  if (isProcessing || messageQueue.length === 0) return;

  isProcessing = true;
  const message = messageQueue.shift(); // Get the next message from the queue

  try {
    const { requestId, filePath, file, pdfFolder, numFiles } = JSON.parse(
      message.value
    );

    const startTime = performance.now();

    console.log(`Request ID: ${requestId} is being processed...`);

    const text = await image2text(filePath);

    if (map.has(requestId)) {
      map.set(requestId, map.get(requestId) + 1);
      timing.set(requestId, timing.get(requestId) + performance.now() - startTime);
    } else {
      map.set(requestId, 1);
      timing.set(requestId, performance.now() - startTime);
    }

    const payloads = [
      {
        topic: "translate-topic",
        messages: JSON.stringify({
          requestId,
          text,
          file,
          pdfFolder,
          numFiles,
        }),
      },
    ];
    payloads[0].partition = Math.floor(Math.random() * NUM_TRANSLATE);

    producer.send(payloads, (err, data) => {
      if (err) {
        console.error("Error sending message:", err);
      } else {
        console.log("Message sent to translate-topic:", data);
      }
    });

    console.log(
      `Request ID: ${requestId} processed in ${
        performance.now() - startTime
      } ms.`
    );

    console.log("Total time taken:", timing.get(requestId));
  } catch (error) {
    console.error("Error processing message:", error);
  } finally {
    isProcessing = false; // Mark processing as complete
    processQueue(); // Process the next message in the queue
  }
}


consumer.on("error", (err) => {
  console.error("Consumer error:", err);
});
