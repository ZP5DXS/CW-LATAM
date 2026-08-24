const net = require("net");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 3000);

const RBN_HOST = "telnet.reversebeacon.net";
const RBN_PORT = 7000;
const RBN_LOGIN = String(process.env.RBN_LOGIN || "ZP5DXS").trim();

const clients = new Set();

let rbn = null;
let reconnectTimer = null;
let buffer = "";

function broadcast(object) {
  const payload = JSON.stringify(object);

  for (const ws of clients) {
    if (ws.readyState === 1) {
      ws.send(payload);
    }
  }
}

function connectRbn() {
  clearTimeout(reconnectTimer);

  buffer = "";

  console.log("========================================");
  console.log("Connecting to Reverse Beacon Network...");
  console.log(`${RBN_HOST}:${RBN_PORT}`);
  console.log(`Login: ${RBN_LOGIN}`);
  console.log("========================================");

  rbn = net.createConnection({
    host: RBN_HOST,
    port: RBN_PORT
  });

  rbn.setKeepAlive(true, 30000);

  rbn.on("connect", () => {
    console.log("TCP CONNECTED TO RBN");

    broadcast({
      type: "status",
      rbn: "tcp-connected"
    });

    setTimeout(() => {
      if (rbn && !rbn.destroyed) {
        console.log(`>>> SENDING LOGIN: ${RBN_LOGIN}`);
        rbn.write(RBN_LOGIN + "\r\n");
      }
    }, 1000);
  });

  rbn.on("data", chunk => {
    const rawChunk = chunk.toString("utf8");

    console.log("");
    console.log("========== RAW RBN CHUNK ==========");
    console.log(JSON.stringify(rawChunk));
    console.log("===================================");

    buffer += rawChunk;

    const lines = buffer.split(/\n/);

    buffer = lines.pop() || "";

    for (let rawLine of lines) {
      rawLine = rawLine.replace(/\r/g, "");

      const line = rawLine.trim();

      if (!line) {
        continue;
      }

      console.log("RBN LINE:");
      console.log(line);

      /*
       * Enviamos la línea cruda al navegador
       * únicamente para diagnóstico.
       */
      broadcast({
        type: "raw",
        line: line
      });
    }
  });

  rbn.on("error", error => {
    console.error("RBN ERROR:", error.message);

    broadcast({
      type: "status",
      rbn: "error",
      message: error.message
    });
  });

  rbn.on("close", () => {
    console.log("RBN CONNECTION CLOSED");

    broadcast({
      type: "status",
      rbn: "disconnected"
    });

    clearTimeout(reconnectTimer);

    reconnectTimer = setTimeout(() => {
      console.log("Trying RBN reconnect...");
      connectRbn();
    }, 5000);
  });

  rbn.on("end", () => {
    console.log("RBN CONNECTION ENDED");
  });

  rbn.on("timeout", () => {
    console.log("RBN SOCKET TIMEOUT");
  });
}

/*
 * HTTP server
 */
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, {
      "content-type": "application/json",
      "access-control-allow-origin": "*"
    });

    res.end(
      JSON.stringify({
        ok: true,
        mode: "debug",
        source: "Reverse Beacon Network",
        host: RBN_HOST,
        port: RBN_PORT,
        clients: clients.size,
        rbnSocketExists: !!rbn,
        rbnDestroyed: rbn ? rbn.destroyed : null
      })
    );

    return;
  }

  res.writeHead(200, {
    "content-type": "text/plain; charset=utf-8"
  });

  res.end(
    [
      "CW LATAM RBN DEBUG RELAY",
      "",
      `RBN: ${RBN_HOST}:${RBN_PORT}`,
      `Login: ${RBN_LOGIN}`,
      "",
      "Open Render logs to inspect raw Telnet data."
    ].join("\n")
  );
});

/*
 * WebSocket
 */
const wss = new WebSocketServer({
  server
});

wss.on("connection", ws => {
  console.log("BROWSER CONNECTED");

  clients.add(ws);

  ws.send(
    JSON.stringify({
      type: "status",
      rbn:
        rbn && !rbn.destroyed
          ? "connected"
          : "connecting"
    })
  );

  ws.on("close", () => {
    clients.delete(ws);

    console.log("BROWSER DISCONNECTED");
  });
});

server.listen(PORT, () => {
  console.log(`CW LATAM DEBUG relay listening on port ${PORT}`);

  connectRbn();
});
