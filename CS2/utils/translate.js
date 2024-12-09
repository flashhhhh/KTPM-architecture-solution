const translator = require("open-google-translator");
const { performance } = require("perf_hooks");
const kafkaNode = require("kafka-node");
const path = require("path");

require("dotenv").config({ path: path.resolve(__dirname, "../.env") });
const NUM_PDF = process.env.NUM_PDF || 1;

const args = process.argv.slice(2);
const partitionId = parseInt(args[0]);

// Initialize Kafka client and producer
const client = new kafkaNode.KafkaClient({ kafkaHost: "localhost:9092" });
const producer = new kafkaNode.Producer(client);

// Consumer for the "translate-topic"
const consumer = new kafkaNode.Consumer(
  client,
  [{ topic: "translate-topic", partition: partitionId }],
  { autoCommit: true }
);

// Translation function
function translate(text) {
  return new Promise((resolve) => {
    translator
      .TranslateLanguageData({
        listOfWordsToTranslate: [text],
        fromLanguage: "en",
        toLanguage: "vi",
      })
      .then((data) => {
        // console.log("Translation data attributes:", Object.keys(data));
        if (data[0].translation == data[0].original) {
          resolve(false); 
        } else {
          resolve(data[0].translation);
        }
      })
      .catch((err) => {
        resolve(false); // Return false in case of an error
      });
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

consumer.on("message", async (message) => {
  try {
    const { requestId, text, file, pdfFolder, numFiles } = JSON.parse(
      message.value
    );
    const startTime = performance.now();

    let translatedText = false;
    let attempts = 0;
    while (!translatedText && attempts < 3) {
      translatedText = await translate(text);
      attempts++;
    }

    console.log("Attempts:", attempts);

    isFailed = false;
    if (!translatedText) {
      console.error("Failed to translate file:", file);
      translatedText = text;``
      isFailed = true;
    }

    if (map.has(requestId)) {
      map.set(requestId, map.get(requestId) + 1);
      timing.get(requestId).push(performance.now() - startTime);
    } else {
      map.set(requestId, 1);
      timing.set(requestId, [performance.now() - startTime]);
    }

    console.log(
      `Request ID: ${requestId} was received by Translate service. File ${file} was processed in ${
        performance.now() - startTime
      } ms.`
    );

    const payloads = [
      {
        topic: "pdf-topic",
        messages: JSON.stringify({
          requestId,
          text: translatedText,
          file,
          pdfFolder,
          numFiles,
          isFailed,
        }),
      },
    ];
    payloads[0].partition = Math.floor(Math.random() * NUM_PDF);

    producer.send(payloads, (err, data) => {
      if (err) {
        console.error("Error sending message:", err);
      } else {
        console.log("Message sent to pdf-topic:", data);
      }
    });

    if (map.get(requestId) === numFiles) {
      const avgTime = timing.get(requestId).reduce((a, b) => a + b, 0) / numFiles;
      console.log(`Average translate processing time for request ID ${requestId}: ${avgTime} ms`);
    }
  } catch (error) {
    console.error("Error processing message:", error);
  }
});

consumer.on("error", (err) => {
  console.error("Consumer error:", err);
});
