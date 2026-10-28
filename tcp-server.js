#!/usr/bin/env node
/**
 * Socket.IO server - receives data emitted by tcp-client.js
 *
 * Install:  npm install socket.io
 * Run:      node socket-server.js [port]      (default port 3000)
 *
 * Listens for these events from the TCP client:
 *   "tcp-data"       { source, hex, bytes, values, timestamp }
 *   "tcp-connected"  { source, timestamp }
 *   "tcp-closed"     { source, totalBytes, timestamp }
 */

const { Server } = require('socket.io');

const PORT = parseInt(process.argv[2], 10) || 3000;

const io = new Server(PORT, {
    cors: { origin: '*' }, // allow browser pages to connect too (handy for testing)
});

const time = (ts) => new Date(ts || Date.now()).toLocaleTimeString();

io.on('connection', (client) => {
    console.log(`[server] Client connected: ${client.id} (${client.handshake.address})`);

    client.on('tcp-connected', (msg) => {
        console.log(`[${time(msg.timestamp)}] TCP connected to ${msg.source}`);
    });

    client.on('modbus', (msg) => {
        const v = msg.values || {};
        console.log(msg)

        // Optional: relay to every other connected client (e.g. a web dashboard)
        client.broadcast.emit('modbus', v);
    });

    client.on('tcp-data', (msg) => {
        const v = msg.values || {};
        console.log(`[${time(msg.timestamp)}] DATA from ${msg.source} (${msg.bytes} bytes)`);
        console.log(`    hex      : ${msg.hex.match(/../g).join(' ')}`);
        console.log(`    uint8    : [${(v.uint8 || []).join(', ')}]`);
        console.log(`    uint16BE : [${(v.uint16BE || []).join(', ')}]`);
        console.log(`    uint16LE : [${(v.uint16LE || []).join(', ')}]`);
        console.log(`    uint32BE : [${(v.uint32BE || []).join(', ')}]`);
        console.log(`    text     : ${v.text}`);

        // Optional: relay to every other connected client (e.g. a web dashboard)
        client.broadcast.emit('tcp-data', msg);
    });

    client.on('tcp-closed', (msg) => {
        console.log(`[${time(msg.timestamp)}] TCP closed ${msg.source} (total ${msg.totalBytes} bytes)`);
    });

    client.on('disconnect', (reason) => {
        console.log(`[server] Client disconnected: ${client.id} (${reason})`);
    });
});

console.log(`[server] Socket.IO server listening on http://localhost:${PORT}`);
console.log('[server] Waiting for tcp-client.js to connect...\n');