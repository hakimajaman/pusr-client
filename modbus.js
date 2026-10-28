#!/usr/bin/env node
'use strict';

/**
 * Modbus TCP client / poller (no external dependencies)
 *
 * Usage:
 *   node modbus-client.js --host 192.168.1.10 --port 502 --start 0 --count 10 \
 *                         --format float --interval 2
 *
 * Options:
 *   --host      Slave IP / hostname              (default 127.0.0.1)
 *   --port      TCP port                         (default 502)
 *   --unit      Unit / slave ID                  (default 1)
 *   --start     Start address (0-based)          (default 0)
 *   --count     Number of registers (or coils)   (default 10)
 *   --format    hex | float                      (default hex)
 *   --interval  Scan every N seconds             (default 1)
 *   --fc        Function code: 1,2,3,4           (default 3)
 *                 1=Coils 2=Discrete inputs 3=Holding regs 4=Input regs
 *   --order     Float word/byte order            (default ABCD)
 *                 ABCD (big-endian) | CDAB (word swap) | BADC | DCBA
 *   --timeout   Response timeout in ms           (default 3000)
 *   --once      Read once and exit
 */

const net = require('net');
const { io } = require('socket.io-client');

// ---------------------------------------------------------------- arguments
function parseArgs(argv) {
    const opts = {
        host: '127.0.0.1',
        port: 502,
        unit: 1,
        start: 0,
        count: 10,
        format: 'hex',
        interval: 1,
        fc: 3,
        order: 'ABCD',
        timeout: 3000,
        once: false,
        socketUrl: ''
    };
    for (let i = 0; i < argv.length; i++) {
        const a = argv[i];
        if (!a.startsWith('--')) continue;
        const key = a.slice(2);
        if (key === 'once') { opts.once = true; continue; }
        if (key === 'help') { printHelp(); process.exit(0); }
        const val = argv[++i];
        if (a.includes('socketUrl')) continue;
        if (val === undefined) fail(`Missing value for --${key}`);
        switch (key) {
            case 'host': opts.host = val; break;
            case 'order': opts.order = val.toUpperCase(); break;
            case 'format': opts.format = val.toLowerCase(); break;
            case 'port': case 'unit': case 'start': case 'count':
            case 'fc': case 'timeout':
                opts[key] = parseInt(val, 10); break;
            case 'interval': opts.interval = parseFloat(val); break;
            default: fail(`Unknown option --${key}`);
        }
    }
    validate(opts);
    return opts;
}

function printHelp() {
    const src = require('fs').readFileSync(__filename, 'utf8');
    console.log(src.split('*/')[0].replace(/^[#!].*\n/, '').replace(/^\/\*\*|^ \* ?/gm, ''));
}

function fail(msg) { console.error('Error: ' + msg); process.exit(1); }

function validate(o) {
    const inRange = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
    if (!inRange(o.port, 1, 65535)) fail('port must be 1-65535');
    if (!inRange(o.unit, 0, 255)) fail('unit must be 0-255');
    if (!inRange(o.start, 0, 65535)) fail('start must be 0-65535');
    if (!inRange(o.count, 1, 65536 - o.start)) fail('count out of range for given start address');
    if (!['hex', 'float'].includes(o.format)) fail('format must be hex or float');
    if (!(o.interval > 0)) fail('interval must be > 0');
    if (![1, 2, 3, 4].includes(o.fc)) fail('fc must be 1, 2, 3 or 4');
    if (!['ABCD', 'CDAB', 'BADC', 'DCBA'].includes(o.order)) fail('order must be ABCD, CDAB, BADC or DCBA');
}


const args = process.argv.slice(2);
const flags = Object.fromEntries(
  args
    .filter((a) => a.startsWith('--'))
    .map((a) => {
      const [k, v] = a.slice(2).split('=');
      return [k, v === undefined ? true : v];
    })
);
const SOCKET_URL = flags.socketUrl || 'http://localhost:3000'
const out = io(SOCKET_URL, {
  reconnection: true,          // auto-reconnect to your socket server
  reconnectionDelay: 1000,
  reconnectionDelayMax: 5000,
})
out.on('connect', () => console.log(`[socket] Connected to ${SOCKET_URL} (id: ${out.id})`));
out.on('disconnect', (reason) => console.log(`[socket] Disconnected: ${reason}`));
out.on('connect_error', (err) => console.error(`[socket] Connect error: ${err.message}`));
function forward(event, payload) {
  let data = {
    ...payload, timestamp: Date.now()
  }
  out.emit(event, data);
}

// ------------------------------------------------------------- modbus client
class ModbusTcpClient {
    constructor({ host, port, timeout }) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.socket = null;
        this.rxBuf = Buffer.alloc(0);
        this.tid = 0;
        this.pending = new Map();
    }

    get connected() {
        return !!this.socket && !this.socket.destroyed;
    }

    connect() {
        return new Promise((resolve, reject) => {
            const sock = new net.Socket();
            sock.setNoDelay(true);
            sock.setTimeout(this.timeout);

            const onConnectError = (err) => { sock.destroy(); reject(err); };
            sock.once('error', onConnectError);
            sock.once('timeout', () => onConnectError(new Error('Connection timeout')));

            sock.connect(this.port, this.host, () => {
                sock.removeListener('error', onConnectError);
                sock.setTimeout(0);
                this.socket = sock;
                this.rxBuf = Buffer.alloc(0);
                sock.on('data', (d) => this._onData(d));
                sock.on('error', (e) => this._onClose(e));
                sock.on('close', () => this._onClose(new Error('Connection closed')));
                forward('modbus-connected', { 'text': 'modbus connected' })
                resolve();
            });
        });
    }

    close() {
        if (this.socket) this.socket.destroy();
        this._onClose(new Error('Closed by client'));
    }

    _onClose(err) {
        this.socket = null;
        for (const [, p] of this.pending) { clearTimeout(p.timer); p.reject(err); }
        this.pending.clear();
    }

    _onData(chunk) {
        this.rxBuf = Buffer.concat([this.rxBuf, chunk]);
        // Frame by MBAP length field (handles partial / merged TCP segments)
        while (this.rxBuf.length >= 7) {
            const total = 6 + this.rxBuf.readUInt16BE(4);
            if (this.rxBuf.length < total) break;
            const frame = this.rxBuf.subarray(0, total);
            this.rxBuf = this.rxBuf.subarray(total);
            this._onFrame(frame);
        }
    }

    _onFrame(frame) {
        const tid = frame.readUInt16BE(0);
        const p = this.pending.get(tid);
        if (!p) return; // stale / unknown response
        this.pending.delete(tid);
        clearTimeout(p.timer);

        const fc = frame[7];
        if (fc & 0x80) {
            const code = frame[8];
            return p.reject(new Error(`Modbus exception ${code}: ${EXCEPTIONS[code] || 'Unknown'}`));
        }
        const byteCount = frame[8];
        p.resolve(Buffer.from(frame.subarray(9, 9 + byteCount)));
    }

    /** Send a read request (FC 1-4) and resolve with the raw data bytes. */
    read(unit, fc, address, quantity) {
        return new Promise((resolve, reject) => {
            if (!this.connected) return reject(new Error('Not connected'));
            this.tid = (this.tid + 1) & 0xffff;
            const tid = this.tid;

            const req = Buffer.alloc(12);
            req.writeUInt16BE(tid, 0);       // transaction id
            req.writeUInt16BE(0, 2);         // protocol id
            req.writeUInt16BE(6, 4);         // remaining length
            req.writeUInt8(unit, 6);         // unit id
            req.writeUInt8(fc, 7);           // function code
            req.writeUInt16BE(address, 8);   // start address
            req.writeUInt16BE(quantity, 10); // quantity

            const timer = setTimeout(() => {
                this.pending.delete(tid);
                reject(new Error(`Response timeout (${this.timeout} ms)`));
            }, this.timeout);

            this.pending.set(tid, { resolve, reject, timer });
            this.socket.write(req);
        });
    }
}

const EXCEPTIONS = {
    1: 'Illegal function',
    2: 'Illegal data address',
    3: 'Illegal data value',
    4: 'Slave device failure',
    5: 'Acknowledge',
    6: 'Slave device busy',
    8: 'Memory parity error',
    10: 'Gateway path unavailable',
    11: 'Gateway target failed to respond',
};

// ------------------------------------------------------------------ decoding
function bytesToRegisters(buf) {
    const regs = [];
    for (let i = 0; i + 1 < buf.length; i += 2) regs.push(buf.readUInt16BE(i));
    return regs;
}

function bytesToBits(buf, count) {
    const bits = [];
    for (let i = 0; i < count; i++) bits.push((buf[i >> 3] >> (i & 7)) & 1);
    return bits;
}

function regsToFloat(r0, r1, order) {
    const b = Buffer.alloc(4);
    const [r0h, r0l, r1h, r1l] = [r0 >> 8, r0 & 0xff, r1 >> 8, r1 & 0xff];
    const maps = {
        ABCD: [r0h, r0l, r1h, r1l],
        CDAB: [r1h, r1l, r0h, r0l],
        BADC: [r0l, r0h, r1l, r1h],
        DCBA: [r1l, r1h, r0l, r0h],
    };
    maps[order].forEach((v, i) => b.writeUInt8(v, i));
    return b.readFloatBE(0);
}

const hex16 = (v) => '0x' + v.toString(16).toUpperCase().padStart(4, '0');

// ---------------------------------------------------------------------- main
async function readAll(client, o) {
    const isBit = o.fc === 1 || o.fc === 2;
    const maxPerReq = isBit ? 2000 : 125; // Modbus protocol limits
    const values = [];

    for (let done = 0; done < o.count; done += maxPerReq) {
        const qty = Math.min(maxPerReq, o.count - done);
        const data = await client.read(o.unit, o.fc, o.start + done, qty);
        if (isBit) values.push(...bytesToBits(data, qty));
        else values.push(...bytesToRegisters(data));
    }
    return values;
}

function printResult(values, o) {
    const ts = new Date().toISOString();
    const fcName = { 1: 'Coils', 2: 'Discrete inputs', 3: 'Holding registers', 4: 'Input registers' }[o.fc];
    console.log(`\n[${ts}] ${o.host}:${o.port} unit=${o.unit} ${fcName} start=${o.start} count=${o.count}`);

    if (o.fc === 1 || o.fc === 2) {
        values.forEach((v, i) => console.log(`  ${String(o.start + i).padStart(5)} : ${v}`));
        return;
    }

    if (o.format === 'hex') {
        values.forEach((v, i) =>
            console.log(`  ${String(o.start + i).padStart(5)} : ${hex16(v)}  (${v})`));
        forward('modbus', {
            type: 'hex',
            value: `  ${String(o.start + i).padStart(5)} : ${hex16(v)}  (${v})`
        });
    } else {
        for (let i = 0; i + 1 < values.length; i += 2) {
            const f = regsToFloat(values[i], values[i + 1], o.order);
            console.log(`  ${String(o.start + i).padStart(5)}-${String(o.start + i + 1).padEnd(5)} : ${f}`);
            forward('modbus', {
                type: 'regsToFloat',
                value: `  ${String(o.start + i).padStart(5)}-${String(o.start + i + 1).padEnd(5)} : ${f}`
            });
        }
        if (values.length % 2) {
            console.log(`  (register ${o.start + values.length - 1} ignored: float needs 2 registers)`);
            forward('modbus', {
                type: 'regsToFloat',
                value: `  (register ${o.start + values.length - 1} ignored: float needs 2 registers)`
            });
        }
    }
}

async function main() {
    const o = parseArgs(process.argv.slice(2));
    const client = new ModbusTcpClient(o);
    let stopping = false;

    const stop = () => { stopping = true; client.close(); console.log('\nStopped.'); process.exit(0); };
    process.on('SIGINT', stop);
    process.on('SIGTERM', stop);

    console.log(`Modbus TCP client -> ${o.host}:${o.port}, scan every ${o.interval}s, format=${o.format}` +
        (o.format === 'float' ? ` (${o.order})` : ''));

    while (!stopping) {
        const cycleStart = Date.now();
        try {
            if (!client.connected) {
                await client.connect();
                console.log(`Connected to ${o.host}:${o.port}`);
            }
            const values = await readAll(client, o);
            printResult(values, o);
            if (o.once) { client.close(); return; }
        } catch (err) {
            console.error(`[${new Date().toISOString()}] ${err.message}`);
            client.close(); // force reconnect on next cycle
            if (o.once) process.exit(1);
        }
        const wait = Math.max(0, o.interval * 1000 - (Date.now() - cycleStart));
        await new Promise((r) => setTimeout(r, wait));
    }
}

main();