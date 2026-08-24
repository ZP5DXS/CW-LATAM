const net = require("net");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 3000);
const RBN_HOST = "telnet.reversebeacon.net";
const RBN_PORT = 7000;
const RBN_LOGIN = String(process.env.RBN_LOGIN || "ZP5DXS").trim();

const clients = new Set();

let rbn = null;
let buffer = "";
let reconnectTimer = null;

const SA_PREFIXES = [
  "LU", "LW", "AY", "AZ", "LO", "LP", "LQ", "LR", "LS", "LT", "LV",
  "CX",
  "ZP",
  "PY", "PP", "PQ", "PR", "PS", "PT", "PU", "PV", "PW", "PX", "ZY", "ZZ",
  "CE", "CA", "CB", "CC", "CD", "XQ",
  "OA", "OB",
  "CP",
  "HC", "HD",
  "YV", "YW", "YY",
  "HK", "HJ",
  "FY",
  "8R",
  "PZ",
  "9Y", "9Z",
  "P4",
  "PJ2", "PJ4", "PJ9",
  "VP8"
];

function cleanCall(call) {
  return String(call || "")
    .toUpperCase()
    .replace(/-#$/, "")
    .trim();
}

function isSouthAmericaSpotter(call) {
  const c = cleanCall(call);

  return SA_PREFIXES.some(prefix =>
    c.startsWith(prefix)
  );
}

function broadcast(obj) {
  const data = JSON.stringify(obj);

  for (const ws of clients) {
    if (ws.readyState === 1) {
      ws.send(data);
    }
  }
}

function parseRbnLine(raw) {
  const line = String(raw || "")
    .replace(/\r/g, "")
    .trim();

  /*
   * Formato REAL observado en Reverse Beacon Network:
   *
   * DX de WC2L-#:  7031.50  N9FGC
   * CW  5 dB  18 WPM  CQ  2319Z
   */
  const match = line.match(
    /^DX de\s+([^:]+):\s+([0-9.]+)\s+(\S+)\s+(CW)\s+(-?\d+)\s+dB\s+(\d+)\s+WPM\s+(.+?)\s+([0-2]\d[0-5]\d)Z$/i
  );

  if (!match) {
    return null;
  }

  const spotter = cleanCall(match[1]);
  const actualFreq = Number(match[2]);
  const dx = cleanCall(match[3]);
  const mode = match[4].toUpperCase();
  const snr = Number(match[5]);
  const wpm = Number(match[6]);
  const activity = match[7].trim().toUpperCase();
  const rbnUtc = match[8];

  /*
   * FILTROS CW LATAM
   */

  // CW únicamente
  if (mode !== "CW") {
    return null;
  }

  // Solamente llamados CQ
  if (activity !== "CQ") {
    return null;
  }

  // Banda de 40 metros
  if (
    actualFreq < 7000 ||
    actualFreq > 7300
  ) {
    return null;
  }

  // El receptor debe estar en Sudamérica.
  // La estación escuchada puede ser de cualquier lugar.
  if (!isSouthAmericaSpotter(spotter)) {
    return null;
  }

  /*
   * CANAL DE ENCUENTRO CW LATAM
   *
   * ±100 Hz alrededor de 7033 kHz.
   *
   * 7032.9 → 7033.1
   *
   * Visualmente se normaliza a 7033.0,
   * pero conservamos la frecuencia real
   * reportada por Reverse Beacon.
   */
  const isChannel =
    actualFreq >= 7032.9 &&
    actualFreq <= 7033.1;

  const freq =
    isChannel
      ? 7033.0
      : actualFreq;

  return {
    source: "RBN",

    ts: Date.now(),

    spotter: spotter,

    dx: dx,

    freq: freq,

    actualFreq: actualFreq,

    isChannel: isChannel,

    snr: snr,

    wpm: wpm,

    type: "CQ",

    mode: "CW",

    rbnUtc: rbnUtc
  };
}

function connectRbn() {
  clearTimeout(reconnectTimer);

  buffer = "";

  console.log(
    `Connecting RBN ${RBN_HOST}:${RBN_PORT} as ${RBN_LOGIN}...`
  );

  rbn = net.createConnection({
    host: RBN_HOST,
    port: RBN_PORT
  });

  rbn.setKeepAlive(
    true,
    30000
  );

  rbn.on("connect", () => {
    console.log(
      "RBN TCP connected"
    );

    /*
     * RBN solicita un indicativo para entrar
     * al stream Telnet.
     */
    setTimeout(() => {
      if (
        rbn &&
        !rbn.destroyed
      ) {
        rbn.write(
          RBN_LOGIN + "\r\n"
        );
      }
    }, 700);
  });

  rbn.on("data", chunk => {
    buffer += chunk.toString("utf8");

    const lines = buffer.split(/\n/);

    buffer = lines.pop() || "";

    for (const line of lines) {
      const spot = parseRbnLine(line);

      if (!spot) {
        continue;
      }

      const channelMark =
        spot.isChannel
          ? " *** 7033 CHANNEL ***"
          : "";

      console.log(
        `MATCH ${spot.spotter} -> ${spot.dx} ` +
        `${spot.actualFreq.toFixed(2)} kHz ` +
        `${spot.snr} dB ` +
        `${spot.wpm} WPM` +
        channelMark
      );

      broadcast(spot);
    }
  });

  const reconnect = () => {
    if (rbn) {
      try {
        rbn.destroy();
      } catch (error) {
        // Ignore
      }

      rbn = null;
    }

    clearTimeout(reconnectTimer);

    reconnectTimer = setTimeout(
      connectRbn,
      5000
    );
  };

  rbn.on(
    "error",
    error => {
      console.error(
        "RBN:",
        error.message
      );
    }
  );

  rbn.on(
    "close",
    reconnect
  );
}

/*
 * Servidor HTTP necesario para Render.
 */
const server = http.createServer(
  (req, res) => {

    if (req.url === "/health") {
      res.writeHead(
        200,
        {
          "content-type":
            "application/json"
        }
      );

      res.end(
        JSON.stringify({
          ok: true,

          source:
            "Reverse Beacon Network",

          telnet:
            `${RBN_HOST}:${RBN_PORT}`,

          websocketClients:
            clients.size,

          filter:
            "CW / CQ / 40m / South America spotters",

          channel:
            "7032.9-7033.1 normalized to 7033.0"
        })
      );

      return;
    }

    res.writeHead(
      200,
      {
        "content-type":
          "text/plain; charset=utf-8"
      }
    );

    res.end(
      "CW LATAM RBN relay\n"
    );
  }
);

/*
 * WebSocket hacia index.html
 */
const wss = new WebSocketServer({
  server
});

wss.on(
  "connection",
  ws => {

    clients.add(ws);

    ws.send(
      JSON.stringify({
        type: "status",
        rbn: "connected"
      })
    );

    ws.on(
      "close",
      () => {
        clients.delete(ws);
      }
    );
  }
);

/*
 * Render proporciona PORT automáticamente.
 */
server.listen(
  PORT,
  () => {

    console.log(
      `CW LATAM relay listening on port ${PORT}`
    );

    connectRbn();
  }
);
