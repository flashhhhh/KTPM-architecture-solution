const express = require("express");
const multer = require("multer");
const crypto = require("crypto");
const path = require("path");
const fs = require("fs");
const { performance } = require("perf_hooks");
const { Kafka } = require('kafkajs');

const PORT = 3000;

const app = express();
const upload = multer({ dest: "uploads/" });

app.use(express.static("public"));

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'app-group' });

isFinished = new Map();

app.post("/upload", upload.single("file"), async (req, res) => {
    const startTime = performance.now();

    const inputPath = req.file.path;
    const requestId =  crypto.randomUUID();
    const pdfFolder = path.join(__dirname, "pdf", `pdf_${requestId}`);
    fs.mkdirSync(pdfFolder, { recursive: true });
    const fileType = path.extname(req.file.originalname).toLowerCase();

    await producer.connect();
    await producer.send({
        topic: "unzip-topic",
        messages: [
            { value: JSON.stringify({ requestId, inputPath: "../" + inputPath, pdfFolder, fileType }) },
        ],
    });

    /*
        Wait for the message to be processed by the services
    */

    isFinished.set(requestId, false);
    while (!isFinished.get(requestId)) {
        await new Promise(resolve => setTimeout(resolve, 500));
    }

    isFinished.delete(requestId);

    // Delete file and folder
    fs.unlinkSync(inputPath);
    if (fs.existsSync(pdfFolder))
        fs.rm(pdfFolder, { recursive: true }, (err) => {
            if (err) {
                console.error("Error removing folder:", err);
            }
        });

    const extractFolder = path.join(__dirname, "uploads", `extracted_${requestId}`);

    if (fs.existsSync(extractFolder))
        fs.rm(extractFolder, { recursive: true }, (err) => {
            if (err) {
                console.error("Error removing folder:", err);
            }
        });

    console.log("Time taken for request ID: ", requestId, " is ", (performance.now() - startTime) / 1000, " seconds");

    res.json({ downloadLink: `/download/${requestId}` });

    // await consumer.connect();
    // await consumer.subscribe({ topic: "app-topic" });

    // const messageHandler = async ({ message }) => {
    //     const { requestId, zipPath } = JSON.parse(message.value.toString());
    //     console.log(zipPath);

    //     if (receivedRequestId === requestId) {
    //         res.json({ downloadLink: `/download/${requestId}` });
    //         await consumer.disconnect();
    //     }
    // };

    // await consumer.run({
    //     eachMessage: messageHandler,
    // });

    // try {
    //     const inputPath = req.file.path;
    //     const requestId =  crypto.randomUUID();
    //     const pdfFolder = path.join(__dirname, "pdf", `pdf_${requestId}`);       
    //     fs.mkdirSync(pdfFolder, { recursive: true });
    //     const fileType = path.extname(req.file.originalname).toLowerCase();

    //     if (fileType === ".zip") {
    //         const extractFolder = path.join(__dirname, "uploads", `extracted_${requestId}`);
    //         const outputFolder = path.join(__dirname, "output", `output_${requestId}`);
    //         fs.mkdirSync(extractFolder, { recursive: true });
    //         fs.mkdirSync(outputFolder, { recursive: true });
    //         await unzipFile(inputPath, extractFolder);
    //         const extractedFiles = fs.readdirSync(extractFolder);
    //         const pdfFiles = [];

    //         for (const file of extractedFiles) {
    //             const filePath = path.join(extractFolder, file);
    //             if (fs.lstatSync(filePath).isFile()) {
    //                 const text = await ocr.image2text(filePath);
    //                 const translatedText = await translate(text);
    //                 const pdfPath = createPDF(translatedText, file, pdfFolder);
    //                 pdfFiles.push(pdfPath);
    //             }
    //         }
    //         console.log(pdfFiles.length);
    //         const zipOutputPath = path.join(outputFolder, "processed_file.zip");          
    //         await createZipFile(zipOutputPath, pdfFiles);
    //         fs.unlinkSync(inputPath);
    //         fs.rmSync(extractFolder, { recursive: true, force: true });
    //         fs.rmSync(pdfFolder, { recursive: true, force: true });
    //         res.json({ downloadLink: `/download/${requestId}` });
    //     } else if (fileType === ".jpg" || fileType === ".jpeg" || fileType === ".png") {
            
    //         const text = await ocr.image2text(inputPath);
    //         const translatedText = await translate(text);
    //         const pdfPath = createPDF(translatedText, req.file.originalname, pdfFolder);
    //         fs.unlinkSync(inputPath);
    //         res.json({ downloadLink: `/downloadFile/${requestId}/${path.basename(pdfPath)}` });

    //     } else {
    //         fs.unlinkSync(inputPath);
    //         res.status(400).send("Chỉ hỗ trợ file ảnh (JPG, PNG) hoặc file ZIP.");
    //     }
    // } catch (err) {
    //     console.error("Error:", err);
    //     res.status(500).send("Internal Server Error");
    // }
    // const endTime = performance.now(); // Kết thúc đo thời gian
    // const executionTime = endTime - startTime; // Tính thời gian thực thi
    // console.log(`Execution time: ${executionTime.toFixed(2) / 1000} ms`);
});
app.get("/downloadFile/:requestId/:filename", (req, res) => {
    const { requestId, filename } = req.params;
    const zipPath = path.join(__dirname, "pdf", `pdf_${requestId}`, filename);
    const pdfFolder = path.join(__dirname, "pdf", `pdf_${requestId}`);
    res.download(zipPath, (err) => {
        if (err) {
            console.error("Error sending PDF:", err);
        }
        fs.unlinkSync(zipPath);
        fs.rm(pdfFolder, { recursive: true }, (err) => {
            if (err) {
                console.error("Error removing folder:", err);
            } else {
                console.log(`Folder ${pdfFolder} removed.`);
            }
        });
    });
});
app.get("/download/:requestId", (req, res) => {
    const requestId = req.params.requestId;
    const zipPath = path.join(__dirname, "output", `${requestId}`, `processed_file.zip`);
    const outputPath = path.join(__dirname, "output", `${requestId}`);
    res.download(zipPath, (err) => {
        if (err) {
            console.error("Error sending PDF:", err);
        }
        
        // Delete the zip file
        fs.unlinkSync(zipPath);

        fs.rm(outputPath, { recursive: true }, (err) => {
            if (err) {
                console.error("Error removing folder:", err);
            } else {
                console.log(`Folder ${outputPath} removed.`);
            }
        });
    });
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});     

async function consumeMessage() {
    await consumer.connect();
    await consumer.subscribe({ topic: "app-topic" });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const { requestId, zipPath } = JSON.parse(
                message.value.toString()
            );

            console.log(`Request ID: ${requestId} was completed by App service on process ${process.pid}`);

            isFinished.set(requestId, true);
        },
    });
}

consumeMessage().catch(console.error);