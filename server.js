const express = require('express');
const cors = require('cors');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Compile Endpoint
app.post('/compile', (req, res) => {
    const { code, board } = req.body;
    if (!code) return res.status(400).send("No code provided.");

    const buildDir = path.join(__dirname, 'build_' + Date.now());
    fs.mkdirSync(buildDir, { recursive: true });
    
    const sketchPath = path.join(buildDir, 'sketch.ino');
    fs.writeFileSync(sketchPath, code);

    let fqbn = "arduino:avr:uno";
    if (board === "esp32devkit") fqbn = "esp32:esp32:esp32";

    const command = `arduino-cli compile --fqbn ${fqbn} "${buildDir}" --output-dir "${buildDir}"`;

    exec(command, (error, stdout, stderr) => {
        if (error) {
            fs.rmSync(buildDir, { recursive: true, force: true });
            return res.status(500).send(stderr || stdout || error.message);
        }

        const files = fs.readdirSync(buildDir);
        const binFile = files.find(f => f.endsWith('.bin') || f.endsWith('.hex'));

        if (binFile) {
            const filePath = path.join(buildDir, binFile);
            res.download(filePath, () => {
                fs.rmSync(buildDir, { recursive: true, force: true });
            });
        } else {
            fs.rmSync(buildDir, { recursive: true, force: true });
            res.status(500).send("Binary output not found.");
        }
    });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Demby Software Systems running on port ${PORT}`));