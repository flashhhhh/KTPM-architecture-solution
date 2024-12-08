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
  return new Promise((resolve, reject) => {
    translator
      .TranslateLanguageData({
        listOfWordsToTranslate: [text],
        fromLanguage: "en",
        toLanguage: "vi",
      })
      .then((data) => {
        resolve(data[0].translation);
      })
      .catch((err) => {
        reject(err);
      });
  });
}

producer.on("ready", () => {
  console.log("Producer is ready");
});

producer.on("error", (err) => {
  console.error("Producer error:", err);
});

consumer.on("message", async (message) => {
  try {
    const { requestId, text, file, pdfFolder, numFiles } = JSON.parse(
      message.value
    );

    const startTime = performance.now();
    const translatedText = await translate(text);

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
  } catch (error) {
    console.error("Error processing message:", error);
  }
});

consumer.on("error", (err) => {
  console.error("Consumer error:", err);
});
