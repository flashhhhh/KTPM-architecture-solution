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

async function createZipFile(outputPath, files, failList) {
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

            if (failList.length > 0) {
                // Create a file with the list of failed files
                const failFile = path.join(path.dirname(files[0]), "failed_files.txt");
                console.log(failFile)
                fs.writeFileSync(failFile, failList.join("\n"));
                archive.file(failFile, { name: "failed_files.txt" });
            }
        }

        archive.finalize();
    });
}

const map = new Map();
const failFiles = new Map();

consumer.on('message', async (message) => {
    const { requestId, pdfPath, numFiles, file, isFailed } = JSON.parse(message.value);

    if (!fs.existsSync(path.join(__dirname, "../", "output", `${requestId}`))) {
        fs.mkdirSync(path.join(__dirname, "../", "output", `${requestId}`), { recursive: true });
    }
    const zipPath = path.join(__dirname, "../", "output", `${requestId}`, `processed_file.zip`);

    if (map.has(requestId)) {
        map.get(requestId).push(pdfPath);
    } else {
        map.set(requestId, [pdfPath]);
    }

    if (isFailed) {
        if (failFiles.has(requestId)) {
            failFiles.get(requestId).push(file);
        } else {
            failFiles.set(requestId, [file]);
        }
    }

    console.log(`Received PDF path: ${pdfPath}, numFiles: ${numFiles}, current files: ${map.get(requestId).length}`);

    if (map.get(requestId).length === numFiles) {
        failList = []
        if (failFiles.has(requestId)) {
            failList = failFiles.get(requestId);
            failFiles.delete(requestId);
        }

        await createZipFile(zipPath, map.get(requestId), failList);
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
