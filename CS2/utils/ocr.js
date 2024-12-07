const tesseract = require("node-tesseract-ocr")
const fs = require("fs");
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "ocr-group" });

async function image2text(path){
  return await tesseract.recognize(path, {
    lang: "eng"
  })
}

// module.exports = {
//   image2text
// }

async function consumeMessage() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "ocr-topic" });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { requestId, filePath, file, pdfFolder, numFiles, startTime } = JSON.parse(
        message.value.toString()
      );
      
      const text = await image2text(filePath);
      fs.unlinkSync(filePath);

      console.log(`Request ID: ${requestId} was received by OCR service on process ${process.pid}`);

      await producer.send({
        topic: "translate-topic",
        messages: [{ value: JSON.stringify({ requestId, text, file, pdfFolder, numFiles, startTime }) }],
      });
    },
  });
}

// Infinitely consume message
consumeMessage().catch(console.error);