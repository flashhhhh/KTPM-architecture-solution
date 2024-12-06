const express = require("express");
const multer = require("multer");
const crypto = require("crypto");

const { unzipFile } = require("./utils/unzip");
const { createZipFile } = require("./utils/zip");
const ocr = require("./utils/ocr");
const { createPDF } = require("./utils/pdf");
const { translate } = require("./utils/translate");
const path = require("path");
const fs = require("fs");
const app = express();
const upload = multer({ dest: "uploads/" });
const PORT = 3000;
app.use(express.static("public"));

app.post("/upload", upload.single("file"), async (req, res) => {
    try {
        const inputPath = req.file.path;
        const requestId =  crypto.randomUUID();
        const pdfFolder = path.join(__dirname, "pdf", `pdf_${requestId}`);       
        fs.mkdirSync(pdfFolder, { recursive: true });
        const fileType = path.extname(req.file.originalname).toLowerCase();

        if (fileType === ".zip") {
            const extractFolder = path.join(__dirname, "uploads", `extracted_${requestId}`);
            const outputFolder = path.join(__dirname, "output", `output_${requestId}`);
            fs.mkdirSync(extractFolder, { recursive: true });
            fs.mkdirSync(outputFolder, { recursive: true });
            await unzipFile(inputPath, extractFolder);
            const extractedFiles = fs.readdirSync(extractFolder);
            const pdfFiles = [];

            for (const file of extractedFiles) {
                const filePath = path.join(extractFolder, file);
                if (fs.lstatSync(filePath).isFile()) {
                    const text = await ocr.image2text(filePath);
                    const translatedText = await translate(text);
                    const pdfPath = createPDF(translatedText, file, pdfFolder);
                    pdfFiles.push(pdfPath);
                }
            }
            console.log(pdfFiles.length);
            const zipOutputPath = path.join(outputFolder, "processed_file.zip");          
            await createZipFile(zipOutputPath, pdfFiles);
            fs.unlinkSync(inputPath);
            fs.rmSync(extractFolder, { recursive: true, force: true });
            fs.rmSync(pdfFolder, { recursive: true, force: true });
            res.json({ downloadLink: `/download/${requestId}` });
        } else if (fileType === ".jpg" || fileType === ".jpeg" || fileType === ".png") {
            
            const text = await ocr.image2text(inputPath);
            const translatedText = await translate(text);
            const pdfPath = createPDF(translatedText, req.file.originalname, pdfFolder);
            fs.unlinkSync(inputPath);
            res.json({ downloadLink: `/downloadFile/${requestId}/${path.basename(pdfPath)}` });

        } else {
            fs.unlinkSync(inputPath);
            res.status(400).send("Chỉ hỗ trợ file ảnh (JPG, PNG) hoặc file ZIP.");
        }
    } catch (err) {
        console.error("Error:", err);
        res.status(500).send("Internal Server Error");
    }
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
            fs.rmdir(pdfFolder, { recursive: true }, (err) => {
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
    const zipPath = path.join(__dirname, "output", `output_${requestId}`, "processed_file.zip");
    const outputPath = path.join(__dirname, "output", `output_${requestId}`);
    res.download(zipPath, (err) => {
            if (err) {
                console.error("Error sending PDF:", err);
            }
            fs.unlinkSync(zipPath);
            fs.rmdir(outputPath, { recursive: true }, (err) => {
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
