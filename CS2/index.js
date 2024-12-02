const express = require("express");
const multer = require("multer");

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
        const outputFolder = path.join(__dirname, "output");
        const extractFolder = path.join(__dirname, "uploads", "extracted");

        const fileType = path.extname(req.file.originalname).toLowerCase();

        if (fileType === ".zip") {
            await unzipFile(inputPath, extractFolder);
            const extractedFiles = fs.readdirSync(extractFolder);
            const pdfFiles = [];

            for (const file of extractedFiles) {
                const filePath = path.join(extractFolder, file);
                if (fs.lstatSync(filePath).isFile()) {
                    const text = await ocr.image2text(filePath);
                    const translatedText = await translate(text);
                    const pdfPath = createPDF(translatedText, file);
                    pdfFiles.push(pdfPath);
                }
            }

            const zipOutputPath = path.join(outputFolder, "processed_files.zip");
            await createZipFile(zipOutputPath, pdfFiles);

            fs.unlinkSync(inputPath);
            fs.rmSync(extractFolder, { recursive: true, force: true });

            res.json({ downloadLink: `/download/${path.basename(zipOutputPath)}` });

        } else if (fileType === ".jpg" || fileType === ".jpeg" || fileType === ".png") {
        
            const text = await ocr.image2text(inputPath);
            const translatedText = await translate(text);
            const pdfPath = createPDF(translatedText, req.file.originalname);

            fs.unlinkSync(inputPath);

            res.json({ downloadLink: `/download/${path.basename(pdfPath)}` });

        } else {
            fs.unlinkSync(inputPath);
            res.status(400).send("Chỉ hỗ trợ file ảnh (JPG, PNG) hoặc file ZIP.");
        }
    } catch (err) {
        console.error("Error:", err);
        res.status(500).send("Internal Server Error");
    }
});

app.get("/download/:filename", (req, res) => {
    const zipPath = path.join(__dirname, "output", req.params.filename);
    
    res.download(zipPath, (err) => {
            if (err) {
                console.error("Error sending PDF:", err);
            }
            fs.unlinkSync(zipPath);
        });
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
