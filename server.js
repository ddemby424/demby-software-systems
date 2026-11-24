const express = require('express');
const cors = require('cors');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');
const http = require('http');
const { attach } = require('./lib/mp-server');

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
const server = http.createServer(app);
const wss = attach(server, { path: '/ws' });

server.listen(PORT, () => {
    console.log(`Demby Software Systems running on port ${PORT}`);
    console.log('Multiplayer WebSocket at /ws');
});

// Render sends SIGTERM before a deploy or shutdown. Close sockets so
// clients auto-reconnect instead of hanging.
function shutdown() {
    wss.clients.forEach(function (ws) {
        try { ws.close(1012, 'server restart'); } catch (e) { /* ignore */ }
    });
    if (typeof wss.flushClose === 'function') wss.flushClose();
    else wss.close();
    server.close(function () { process.exit(0); });
    setTimeout(function () { process.exit(0); }, 4000);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
