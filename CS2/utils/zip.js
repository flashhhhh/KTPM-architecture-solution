const fs = require("fs");
const archiver = require("archiver");
const path = require("path");
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'zip-group' });

async function createZipFile(outputPath, files) {
    return new Promise((resolve, reject) => {
        const output = fs.createWriteStream(outputPath);
        const archive = archiver("zip", { zlib: { level: 9 } });

        output.on("close", () => resolve(outputPath));
        archive.on("error", (err) => reject(err));

        archive.pipe(output);

        if (files.length === 1) {
            archive.file(files[0], { name: path.basename(files[0]) });
        } else {
            files.forEach((file) => {
                archive.file(file, { name: path.basename(file) });
            });
        }
        // files.forEach((file) => {
        //     archive.file(file, { name: path.basename(file) });
        // });

        archive.finalize();
    });
}

const map = new Map();

async function consumeMessage() {
    await consumer.connect();
    await consumer.subscribe({ topic: "zip-topic" });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const { requestId, pdfPath, numFiles } = JSON.parse(
                message.value.toString()
            );

            console.log(`Request ID: ${requestId} was received by Zip service on process ${process.pid}`);

            if (!fs.existsSync(path.join(__dirname, "../", "output", `${requestId}`))) {
                fs.mkdirSync(path.join(__dirname, "../", "output", `${requestId}`), { recursive: true });
            }
            const zipPath = path.join(__dirname, "../", "output", `${requestId}`, `processed_file.zip`);

            if (map.has(requestId)) {
                map.get(requestId).push(pdfPath);
            } else {
                map.set(requestId, [pdfPath]);
            }
            
            if (map.get(requestId).length === numFiles) {
                await createZipFile(zipPath, map.get(requestId));
                map.delete(requestId);

                console.log(`Request ID: ${requestId} was completed by Zip service on process ${process.pid}`);

                await producer.connect();
                await producer.send({
                    topic: "app-topic",
                    messages: [{ key: requestId, value: JSON.stringify({ requestId, zipPath }) }],
                });
            }
        },
    });
}

consumeMessage().catch(console.error);