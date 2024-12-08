const translator = require("open-google-translator");
const { performance } = require("perf_hooks");
const { Kafka } = require("kafkajs");

translator.supportedLanguages();

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'translate-group' });

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
            }).catch((err) => {
                reject(err)
            });
    });
}

// module.exports = {
//     translate
// }

async function consumeMessage() {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: "translate-topic" });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const { requestId, text, file, pdfFolder, numFiles } = JSON.parse(
                message.value.toString()
            );

            startTime = performance.now();
            const translatedText = await translate(text);

            console.log(`Request ID: ${requestId} was received by Translate service on process ${process.pid}. File ${file} was processed in ${performance.now() - startTime} ms.`);

            await producer.send({
                topic: "pdf-topic",
                messages: [{ value: JSON.stringify({ requestId, text: translatedText, file, pdfFolder, numFiles }) }],
            });
        },
    });
}

// Infinitely consume message
consumeMessage().catch(console.error);