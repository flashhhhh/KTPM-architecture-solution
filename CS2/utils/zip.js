const fs = require("fs");
const archiver = require("archiver");
const path = require("path");

async function createZipFile(outputPath, files) {
    return new Promise((resolve, reject) => {
        const output = fs.createWriteStream(outputPath);
        const archive = archiver("zip", { zlib: { level: 9 } });

        output.on("close", () => resolve(outputPath));
        archive.on("error", (err) => reject(err));

        archive.pipe(output);

        files.forEach((file) => {
            archive.file(file, { name: path.basename(file) });
        });

        archive.finalize();
    });
}

module.exports = { createZipFile };
