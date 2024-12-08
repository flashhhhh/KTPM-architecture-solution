const fs = require("fs");
const archiver = require("archiver");
const path = require("path");
const kafka = require('kafka-node');

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new kafka.Producer(client);
const consumer = new kafka.Consumer(
    client,
    [{ topic: 'zip-topic', partition: 0 }],
    { autoCommit: true }
);

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

        archive.finalize();
    });
}

const map = new Map();

consumer.on('message', async (message) => {
    const { requestId, pdfPath, numFiles } = JSON.parse(message.value);

    if (!fs.existsSync(path.join(__dirname, "../", "output", `${requestId}`))) {
        fs.mkdirSync(path.join(__dirname, "../", "output", `${requestId}`), { recursive: true });
    }
    const zipPath = path.join(__dirname, "../", "output", `${requestId}`, `processed_file.zip`);

    if (map.has(requestId)) {
        map.get(requestId).push(pdfPath);
    } else {
        map.set(requestId, [pdfPath]);
    }

    console.log(`Received PDF path: ${pdfPath}, numFiles: ${numFiles}, current files: ${map.get(requestId).length}`);

    if (map.get(requestId).length === numFiles) {
        await createZipFile(zipPath, map.get(requestId));
        map.delete(requestId);

        producer.send([{
            topic: 'app-topic',
            messages: JSON.stringify({ requestId, zipPath })
        }], (err, data) => {
            if (err) console.error(err);
        });
    }
});

consumer.on('error', (err) => {
    console.error('Error:', err);
});

producer.on('ready', () => {
    console.log('Producer is ready');
});

producer.on('error', (err) => {
    console.error('Error:', err);
});
