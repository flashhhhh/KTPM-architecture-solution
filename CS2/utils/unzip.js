const fs = require("fs");
const path = require("path");
const unzipper = require("unzipper");

async function unzipFile(zipPath, extractFolder) {
    return new Promise((resolve, reject) => {
        fs.createReadStream(zipPath)
            .pipe(unzipper.Extract({ path: extractFolder }))
            .on("close", () => resolve())
            .on("error", (err) => reject(err));
    });
}

module.exports = { unzipFile };
