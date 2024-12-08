const tesseract = require("node-tesseract-ocr")
const fs = require("fs");
const { performance } = require("perf_hooks");
const { Kafka } = require("kafkajs");

// const args = process.argv.slice(2);
// const partitionId = args[0];
// console.log("Partition ID: ", partitionId);

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
  await consumer.connect();
  await consumer.subscribe({ topic: "ocr-topic" });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { requestId, filePath, file, pdfFolder, numFiles} = JSON.parse(
        message.value.toString()
      );
      
      startTime = performance.now();
      const text = await image2text(filePath);
      fs.unlinkSync(filePath);

      console.log(`Request ID: ${requestId} was received by OCR service in partition ${partition} on process ${process.pid}. File ${file} was processed in ${performance.now() - startTime} ms.`);

      await producer.connect();
      await producer.send({
        topic: "translate-topic",
        messages: [{ value: JSON.stringify({ requestId, text, file, pdfFolder, numFiles}) }],
      });
    },
  });
}

// Infinitely consume message
consumeMessage().catch(console.error);