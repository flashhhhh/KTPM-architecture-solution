const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require("path");



function createPDF(text, file, pdfFolder) {
    // const outputDir = "output";
    const pdfName = path.basename(file, path.extname(file)) + ".pdf";
    // const pdfPath = `${outputDir}/${pdfName}`;
    const pdfPath = path.join(pdfFolder, pdfName);

    if (!fs.existsSync(pdfFolder)) {
        fs.mkdirSync(pdfFolder);
    }
    const doc = new PDFDocument();
    doc.pipe(fs.createWriteStream(pdfPath));
    doc.font('font/Roboto-Regular.ttf')
        .fontSize(14)
        .text(text, 100, 100);
    doc.end();
    return pdfPath;
}

module.exports = {
    createPDF
}