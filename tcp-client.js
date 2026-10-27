#!/usr/bin/env node
/**
 * TCP client -> Socket.IO forwarder (Node.js)
 *
 * Connects to a TCP server, and every chunk of data received from it
 * is emitted to your Socket.IO server URL.
 *
 * Install:
 *   npm install socket.io-client
 *
 * Usage:
 *   node tcp-client.js [tcpHost] [tcpPort] [--url=http://localhost:3000] [--event=tcp-data] [--reconnect]
 *
 * Example:
 *   node tcp-client.js 127.0.0.1 9000 --url=http://localhost:3000 --reconnect
 *
 * Events emitted to your socket server:
 *   "tcp-data"       { source, hex, bytes, values, timestamp }   (name changeable with --event)
 *        hex    : raw data as a hex string, e.g. "aa550001012c"
 *        bytes  : number of bytes in this chunk
 *        values : { uint8[], uint16BE[], uint16LE[], uint32BE[], uint32LE[], text }
 *   "tcp-connected"  { source, timestamp }
 *   "tcp-closed"     { source, totalBytes, timestamp }
 *
 * Anything typed in this terminal is sent to the TCP server. Ctrl+C to quit.
 */

const net = require('net');
const readline = require('readline');
const { io } = require('socket.io-client');

// ---------- args ----------
const args = process.argv.slice(2);
const flags = Object.fromEntries(
  args
    .filter((a) => a.startsWith('--'))
    .map((a) => {
      const [k, v] = a.slice(2).split('=');
      return [k, v === undefined ? true : v];
    })
);
const positional = args.filter((a) => !a.startsWith('--'));

const TCP_HOST = positional[0] || '127.0.0.1';
const TCP_PORT = parseInt(positional[1], 10) || 9000;
const SOCKET_URL = flags.url || 'http://localhost:3000';
const DATA_EVENT = flags.event || 'tcp-data';
const RECONNECT = Boolean(flags.reconnect);
const RECONNECT_DELAY_MS = 3000;
const SOURCE = `${TCP_HOST}:${TCP_PORT}`;

// ---------- Socket.IO connection (to your socket URL) ----------
const out = io(SOCKET_URL, {
  reconnection: true,          // auto-reconnect to your socket server
  reconnectionDelay: 1000,
  reconnectionDelayMax: 5000,
});

out.on('connect', () => console.log(`[socket] Connected to ${SOCKET_URL} (id: ${out.id})`));
out.on('disconnect', (reason) => console.log(`[socket] Disconnected: ${reason}`));
out.on('connect_error', (err) => console.error(`[socket] Connect error: ${err.message}`));

// Note: socket.io-client buffers emits while disconnected and
// flushes them when the connection comes back.
function forward(event, payload) {
  out.emit(event, { source: SOURCE, ...payload, timestamp: Date.now() });
}

// ---------- hex + value conversion ----------
function readAll(buf, size, method) {
  const out = [];
  for (let i = 0; i + size <= buf.length; i += size) out.push(buf[method](i));
  return out;
}

// Convert a raw Buffer into a hex string and numeric values
function decode(buf) {
  const hex = buf.toString('hex');                       // "aa550001012c"
  const values = {
    uint8: [...buf],                                     // every byte as 0-255
    uint16BE: readAll(buf, 2, 'readUInt16BE'),           // 2-byte numbers, big-endian
    uint16LE: readAll(buf, 2, 'readUInt16LE'),           // 2-byte numbers, little-endian
    uint32BE: readAll(buf, 4, 'readUInt32BE'),           // 4-byte numbers, big-endian
    uint32LE: readAll(buf, 4, 'readUInt32LE'),           // 4-byte numbers, little-endian
    text: buf.toString('utf8').replace(/[^\x20-\x7E]/g, '.'), // printable ASCII, "." for others
  };
  return { hex, values };
}

// ---------- TCP client ----------
let tcp = null;
let totalBytes = 0;
let quitting = false;

function connectTcp() {
  console.log(`[tcp] Connecting to ${SOURCE} ...`);
  tcp = new net.Socket();
  tcp.setKeepAlive(true, 10000);

  tcp.connect(TCP_PORT, TCP_HOST, () => {
    console.log(`[tcp] Connected to ${SOURCE}`);
    console.log('[tcp] Type a message and press Enter to send it to the TCP server.\n');
    forward('tcp-connected', {});
  });

  tcp.on('data', (chunk) => {            // chunk is a raw Buffer
    const bytes = chunk.length;
    totalBytes += bytes;
    const { hex, values } = decode(chunk);
    const spaced = hex.match(/../g).join(' ');
    console.log(`[tcp -> socket] ${bytes} bytes | hex: ${spaced} | uint8: [${values.uint8.join(', ')}]`);
    forward(DATA_EVENT, { hex, bytes, values });
  });

  tcp.on('end', () => console.log('[tcp] Server closed the connection.'));
  tcp.on('error', (err) => console.error(`[tcp] Error: ${err.code || err.message}`));

  tcp.on('close', () => {
    console.log(`[tcp] Closed. Total received: ${totalBytes} bytes.`);
    forward('tcp-closed', { totalBytes });

    if (RECONNECT && !quitting) {
      console.log(`[tcp] Reconnecting in ${RECONNECT_DELAY_MS / 1000}s ...`);
      setTimeout(connectTcp, RECONNECT_DELAY_MS);
    } else {
      // give the last emit a moment to flush, then exit
      setTimeout(() => process.exit(0), 300);
    }
  });
}

// ---------- stdin -> TCP ----------
const rl = readline.createInterface({ input: process.stdin });
rl.on('line', (line) => {
  if (tcp && !tcp.destroyed && tcp.writable) tcp.write(line + '\n');
  else console.log('[tcp] Not connected, message not sent.');
});

// ---------- clean exit ----------
process.on('SIGINT', () => {
  quitting = true;
  console.log('\n[client] Bye.');
  if (tcp) tcp.destroy();
  out.close();
  process.exit(0);
});

connectTcp();