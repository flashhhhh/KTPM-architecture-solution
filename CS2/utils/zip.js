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

module.exports = { createZipFile };
