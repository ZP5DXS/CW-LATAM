const net = require("net");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 3000);

const RBN_HOST = "telnet.reversebeacon.net";
const RBN_PORT = 7000;

// Indicativo utilizado únicamente para identificarnos
// al conectar con el servidor Telnet de RBN.
const RBN_LOGIN = String(
  process.env.RBN_LOGIN || "ZP5DXS"
).trim();

const clients = new Set();

let rbn = null;
let reconnectTimer = null;
let buffer = "";

// Prefijos utilizados para determinar si el SPOTTER
// pertenece a Sudamérica.
const SA_PREFIXES = [
  "LU", "LW", "AY", "AZ", "LO", "LP", "LQ", "LR",
  "LS", "LT", "LV",

  "CX",
  "ZP",

  "PY", "PP", "PQ", "PR", "PS", "PT", "PU",
  "PV", "PW", "PX", "ZY", "ZZ",

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

function stripAnsi(text) {
  return text.replace(
    /\x1B\[[0-?]*[ -/]*[@-~]/g,
    ""
  );
}

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

/*
 * Ejemplo típico del flujo RBN:
 *
 * DX de W3LPL-#: 7025.0 K1ABC
 * 18 dB 25 WPM CQ 2359Z
 *
 * El formato puede contener campos adicionales.
 */
function parseRbnLine(raw) {

  const line = stripAnsi(raw)
    .replace(/\r/g, "")
    .trim();

  if (!line.startsWith("DX de ")) {
    return null;
  }

  const head = line.match(
    /^DX de\s+([^:]+):\s+([0-9.]+)\s+(\S+)\s+(.*)$/i
  );

  if (!head) {
    return null;
  }

  const spotter = cleanCall(head[1]);
  const freq = Number(head[2]);
  const dx = cleanCall(head[3]);
  const tail = head[4];

  const db = tail.match(
    /(-?\d+)\s*dB\b/i
  );

  const wpm = tail.match(
    /(\d+)\s*WPM\b/i
  );

  const type = tail.match(
    /\b(CQ|DX|BCN|BEACON)\b/i
  );

  const utc = tail.match(
    /\b([0-2]\d[0-5]\d)Z\b/i
  );

  // Queremos spots de CW.
  // Los spots RBN de CW incluyen velocidad WPM.
  if (!db || !wpm || !type) {
    return null;
  }

  // Solamente estaciones llamando CQ.
  if (type[1].toUpperCase() !== "CQ") {
    return null;
  }

  // Solamente banda de 40 metros.
  if (!(freq >= 7000 && freq <= 7300)) {
    return null;
  }

  // El receptor/spotter debe estar en Sudamérica.
  // La estación escuchada puede estar en cualquier lugar.
  if (!isSouthAmericaSpotter(spotter)) {
    return null;
  }

  return {
    source: "RBN",

    ts: Date.now(),

    spotter: spotter,

    dx: dx,

    freq: freq,

    snr: Number(db[1]),

    wpm: Number(wpm[1]),

    type: "CQ",

    mode: "CW",

    rbnUtc: utc
      ? utc[1]
      : null,

    raw: line
  };
}

function broadcast(object) {

  const payload = JSON.stringify(object);

  for (const ws of clients) {

    if (ws.readyState === 1) {
      ws.send(payload);
    }

  }
}

/*
 * Conexión permanente al Telnet
 * del Reverse Beacon Network.
 */
function connectRbn() {

  clearTimeout(reconnectTimer);

  buffer = "";

  console.log(
    `Connecting to ${RBN_HOST}:${RBN_PORT}...`
  );

  rbn = net.createConnection({
    host: RBN_HOST,
    port: RBN_PORT
  });

  rbn.setKeepAlive(
    true,
    30000
  );

  rbn.setTimeout(
    120000
  );

  rbn.on("connect", () => {

    console.log(
      "Connected to Reverse Beacon Network"
    );

    /*
     * El servidor Telnet solicita un indicativo.
     * Lo enviamos poco después de establecer
     * la conexión.
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

    }, 500);

    broadcast({
      type: "status",
      rbn: "connected"
    });

  });

  /*
   * Datos recibidos continuamente desde RBN.
   */
  rbn.on("data", chunk => {

    buffer += chunk.toString("utf8");

    const lines = buffer.split(/\n/);

    buffer = lines.pop() || "";

    for (const line of lines) {

      const spot = parseRbnLine(line);

      if (spot) {

        console.log(
          `${spot.spotter} -> ${spot.dx}`,
          spot.freq,
          `${spot.snr} dB`,
          `${spot.wpm} WPM`
        );

        broadcast(spot);

      }

    }

  });

  const reconnect = () => {

    console.log(
      "RBN disconnected. Reconnecting..."
    );

    broadcast({
      type: "status",
      rbn: "disconnected"
    });

    if (rbn) {

      try {
        rbn.destroy();
      } catch (error) {
        // Ignore
      }

      rbn = null;
    }

    clearTimeout(
      reconnectTimer
    );

    reconnectTimer = setTimeout(
      connectRbn,
      5000
    );

  };

  rbn.on(
    "timeout",
    reconnect
  );

  rbn.on(
    "error",
    error => {

      console.error(
        "RBN error:",
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
 * HTTP server requerido por Render.
 *
 * /health permite comprobar fácilmente
 * que el relay está funcionando.
 */
const server = http.createServer(
  (req, res) => {

    if (req.url === "/health") {

      res.writeHead(
        200,
        {
          "content-type":
            "application/json",

          "access-control-allow-origin":
            "*"
        }
      );

      res.end(
        JSON.stringify({
          ok: true,

          source:
            "Reverse Beacon Network",

          telnet:
            `${RBN_HOST}:${RBN_PORT}`,

          clients:
            clients.size
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
 * WebSocket que consumirá index.html.
 */
const wss = new WebSocketServer({
  server
});

wss.on(
  "connection",
  ws => {

    console.log(
      "Browser connected"
    );

    clients.add(ws);

    ws.send(
      JSON.stringify({
        type: "status",

        rbn:
          rbn &&
          !rbn.destroyed
            ? "connected"
            : "connecting"
      })
    );

    ws.on(
      "close",
      () => {

        clients.delete(ws);

        console.log(
          "Browser disconnected"
        );

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
