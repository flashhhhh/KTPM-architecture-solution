const express = require("express");
const multer = require("multer");
const unzipper = require("unzipper"); 
const archiver = require("archiver"); 
const ocr = require("./utils/ocr");
const { createPDF } = require("./utils/pdf");
const { translate } = require("./utils/translate");
const path = require("path");
const fs = require("fs");
const app = express();
const upload = multer({ dest: "uploads/" });
const PORT = 3000;
app.use(express.static("public"));

app.post("/upload", upload.single("zipfile"), async(req, res) => {
    try {
        const zipPath = req.file.path;
        const extractFolder = path.join(__dirname, "uploads", "extracted");

        fs.createReadStream(zipPath)
            .pipe(unzipper.Extract({path: extractFolder}))
            .on('close', async () => {
                console.log("Tệp zip đã được giải nén");
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

                const zipOutputPath = path.join(__dirname, "output", "processed_files.zip");
                const output = fs.createWriteStream(zipOutputPath);
                const archive = archiver("zip", {
                    zlib: { level: 9 }
                });

                archive.pipe(output);

                pdfFiles.forEach(pdfFile => {
                    archive.file(pdfFile, { name: path.basename(pdfFile) });
                });

                archive.finalize();
                output.on("close", () => {
                    fs.unlinkSync(zipPath);
                    fs.rmSync(extractFolder, { recursive: true, force: true });

                    res.json({
                        downloadLink: `/download/${path.basename(zipOutputPath)}`
                    });
                });

            })
            .on("error", (err) => {
                console.error("Lỗi giải nén tệp:", err);
                res.status(500).send("Lỗi xử lý tệp upload");
            }); 
    } catch {
        console.error("Error:", err);
        res.status(500).send("Internal Server Error");
    }
});

app.get("/download/:filename", (req, res) => {
    const zipPath = path.join(__dirname, "output", req.params.filename);
    // res.download(pdfPath);
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
