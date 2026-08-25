const net = require("net");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 3000);

const RBN_HOST = "telnet.reversebeacon.net";
const RBN_PORT = 7000;
const RBN_LOGIN = String(
  process.env.RBN_LOGIN || "ZP5DXS"
).trim();

const HISTORY_MS = 10 * 60 * 1000;

const RBN_WATCHDOG_MS = 90 * 1000;
const WS_HEARTBEAT_MS = 20 * 1000;
const APP_HEARTBEAT_MS = 15 * 1000;

const clients = new Set();
const history = [];

let rbn = null;
let buffer = "";
let reconnectTimer = null;

let lastRbnDataAt = 0;
let rbnConnected = false;

/*
 * Número secuencial de cada spot.
 * Permite al navegador pedir:
 *
 * "dame solamente lo posterior al spot 1234"
 */
let spotSequence = 0;


/*
 * SPOTTERS SUDAMÉRICA
 */
const SA_PREFIXES = [
  "LU", "LW", "AY", "AZ",
  "LO", "LP", "LQ", "LR",
  "LS", "LT", "LV",

  "CX",
  "ZP",

  "PY", "PP", "PQ", "PR",
  "PS", "PT", "PU", "PV",
  "PW", "PX", "ZY", "ZZ",

  "CE", "CA", "CB", "CC",
  "CD", "XQ",

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

  return SA_PREFIXES.some(
    prefix => c.startsWith(prefix)
  );
}


/*
 * BROADCAST WEBSOCKET
 */
function broadcast(obj) {
  const data = JSON.stringify(obj);

  for (const ws of clients) {
    if (ws.readyState === 1) {
      try {
        ws.send(data);
      } catch (error) {
        console.error(
          "WebSocket send:",
          error.message
        );
      }
    }
  }
}


/*
 * HISTORIAL 10 MIN
 */
function pruneHistory() {
  const cutoff =
    Date.now() - HISTORY_MS;

  while (
    history.length &&
    history[0].ts < cutoff
  ) {
    history.shift();
  }
}


function rememberSpot(spot) {
  history.push(spot);
  pruneHistory();
}


/*
 * PARSER RBN
 */
function parseRbnLine(raw) {
  const line = String(raw || "")
    .replace(/\r/g, "")
    .trim();

  const match = line.match(
    /^DX de\s+([^:]+):\s+([0-9.]+)\s+(\S+)\s+(CW)\s+(-?\d+)\s+dB\s+(\d+)\s+WPM\s+(.+?)\s+([0-2]\d[0-5]\d)Z$/i
  );

  if (!match) {
    return null;
  }

  const spotter =
    cleanCall(match[1]);

  const actualFreq =
    Number(match[2]);

  const dx =
    cleanCall(match[3]);

  const mode =
    match[4].toUpperCase();

  const snr =
    Number(match[5]);

  const wpm =
    Number(match[6]);

  const activity =
    match[7]
      .trim()
      .toUpperCase();

  const rbnUtc =
    match[8];


  /*
   * FILTROS
   */
  if (mode !== "CW") {
    return null;
  }

  if (activity !== "CQ") {
    return null;
  }

  if (
    actualFreq < 7000 ||
    actualFreq > 7300
  ) {
    return null;
  }

  if (
    !isSouthAmericaSpotter(spotter)
  ) {
    return null;
  }


  /*
   * CANAL 7033
   */
  const isChannel =
    actualFreq >= 7032.9 &&
    actualFreq <= 7033.1;

  const freq =
    isChannel
      ? 7033.0
      : actualFreq;


  /*
   * Secuencia monotónica.
   */
  spotSequence++;


  return {
    seq:
      spotSequence,

    source:
      "RBN",

    ts:
      Date.now(),

    spotter,

    dx,

    freq,

    actualFreq,

    isChannel,

    snr,

    wpm,

    type:
      "CQ",

    mode:
      "CW",

    rbnUtc
  };
}


/*
 * RECONEXIÓN RBN
 */
function scheduleRbnReconnect() {
  clearTimeout(reconnectTimer);

  reconnectTimer =
    setTimeout(
      connectRbn,
      3000
    );
}


/*
 * TELNET
 */
function connectRbn() {
  clearTimeout(reconnectTimer);

  buffer = "";
  rbnConnected = false;

  console.log(
    `Conectando ${RBN_HOST}:${RBN_PORT} como ${RBN_LOGIN}...`
  );


  rbn = net.createConnection({
    host:
      RBN_HOST,

    port:
      RBN_PORT
  });


  rbn.setNoDelay(true);

  rbn.setKeepAlive(
    true,
    20000
  );


  rbn.on(
    "connect",
    () => {

      rbnConnected =
        true;

      lastRbnDataAt =
        Date.now();


      console.log(
        "RBN TCP conectado"
      );


      setTimeout(
        () => {

          if (
            rbn &&
            !rbn.destroyed
          ) {
            rbn.write(
              RBN_LOGIN +
              "\r\n"
            );
          }

        },
        700
      );

    }
  );


  /*
   * STREAM RBN
   */
  rbn.on(
    "data",
    chunk => {

      lastRbnDataAt =
        Date.now();


      buffer +=
        chunk.toString(
          "utf8"
        );


      const lines =
        buffer.split(/\n/);


      buffer =
        lines.pop() ||
        "";


      for (
        const line
        of lines
      ) {

        const spot =
          parseRbnLine(line);


        if (!spot) {
          continue;
        }


        /*
         * Primero memoria.
         */
        rememberSpot(
          spot
        );


        /*
         * Después WebSocket
         * inmediatamente.
         */
        broadcast(
          spot
        );


        const mark =
          spot.isChannel
            ? " *** 7033 ***"
            : "";


        console.log(
          `LIVE #${spot.seq} ` +
          `${spot.spotter} -> ` +
          `${spot.dx} ` +
          `${spot.actualFreq.toFixed(2)} ` +
          `${spot.snr} dB ` +
          `${spot.wpm} WPM` +
          mark
        );

      }

    }
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
    () => {

      console.log(
        "RBN desconectado"
      );


      rbnConnected =
        false;


      scheduleRbnReconnect();

    }
  );

}


/*
 * WATCHDOG RBN
 */
setInterval(
  () => {

    if (
      !rbn ||
      rbn.destroyed ||
      !rbnConnected
    ) {
      return;
    }


    const silentFor =
      Date.now() -
      lastRbnDataAt;


    if (
      silentFor >
      RBN_WATCHDOG_MS
    ) {

      console.warn(
        "RBN sin datos 90 s. Reconectando..."
      );


      try {
        rbn.destroy();
      } catch (_) {}

    }

  },
  15000
);


/*
 * CORS
 */
function corsHeaders(extra = {}) {
  return {
    "access-control-allow-origin":
      "*",

    "cache-control":
      "no-store, no-cache, must-revalidate",

    ...extra
  };
}


/*
 * HTTP
 */
const server =
  http.createServer(
    (req, res) => {

      const url =
        new URL(
          req.url,
          `http://${req.headers.host || "localhost"}`
        );


      /*
       * ENDPOINT DE SPOTS
       *
       * Ej:
       *
       * /spots?after=123
       *
       * Devuelve todo lo posterior
       * al spot #123.
       */
      if (
        url.pathname ===
        "/spots"
      ) {

        pruneHistory();


        let after =
          Number(
            url.searchParams.get(
              "after"
            ) ||
            0
          );


        if (
          !Number.isFinite(after) ||
          after < 0
        ) {
          after = 0;
        }


        const result =
          history.filter(
            spot =>
              spot.seq >
              after
          );


        res.writeHead(
          200,
          corsHeaders({
            "content-type":
              "application/json; charset=utf-8"
          })
        );


        res.end(
          JSON.stringify({

            ok:
              true,

            spots:
              result,

            lastSeq:
              spotSequence,

            serverTime:
              Date.now()

          })
        );


        return;
      }


      /*
       * HEALTH
       */
      if (
        url.pathname ===
        "/health"
      ) {

        pruneHistory();


        res.writeHead(
          200,
          corsHeaders({
            "content-type":
              "application/json; charset=utf-8"
          })
        );


        res.end(
          JSON.stringify({

            ok:
              true,

            live:
              rbnConnected,

            websocketClients:
              clients.size,

            historySpots:
              history.length,

            lastSeq:
              spotSequence,

            historyMinutes:
              10,

            secondsSinceRbnData:
              lastRbnDataAt
                ? Math.round(
                    (
                      Date.now() -
                      lastRbnDataAt
                    ) /
                    1000
                  )
                : null,

            channel:
              "7032.9-7033.1"

          })
        );


        return;
      }


      res.writeHead(
        200,
        corsHeaders({
          "content-type":
            "text/plain; charset=utf-8"
        })
      );


      res.end(
        "CW LATAM relay LIVE\n"
      );

    }
  );


/*
 * WEBSOCKET
 */
const wss =
  new WebSocketServer({
    server,

    perMessageDeflate:
      false
  });


wss.on(
  "connection",
  ws => {

    ws.isAlive =
      true;


    ws.on(
      "pong",
      () => {

        ws.isAlive =
          true;

      }
    );


    clients.add(
      ws
    );


    console.log(
      `Navegador conectado. Total: ${clients.size}`
    );


    /*
     * Estado.
     */
    ws.send(
      JSON.stringify({

        type:
          "status",

        live:
          rbnConnected,

        ts:
          Date.now()

      })
    );


    /*
     * Historial.
     */
    pruneHistory();


    ws.send(
      JSON.stringify({

        type:
          "history",

        spots:
          history,

        lastSeq:
          spotSequence

      })
    );


    ws.on(
      "close",
      () => {

        clients.delete(
          ws
        );


        console.log(
          `Navegador desconectado. Total: ${clients.size}`
        );

      }
    );


    ws.on(
      "error",
      () => {

        clients.delete(
          ws
        );

      }
    );

  }
);


/*
 * HEARTBEAT PROTOCOLO
 */
const websocketHeartbeat =
  setInterval(
    () => {

      for (
        const ws
        of clients
      ) {

        if (
          ws.isAlive ===
          false
        ) {

          clients.delete(
            ws
          );


          try {
            ws.terminate();
          } catch (_) {}


          continue;
        }


        ws.isAlive =
          false;


        try {

          ws.ping();

        } catch (_) {

          clients.delete(
            ws
          );


          try {
            ws.terminate();
          } catch (_) {}

        }

      }

    },
    WS_HEARTBEAT_MS
  );


/*
 * HEARTBEAT DE APLICACIÓN
 */
const applicationHeartbeat =
  setInterval(
    () => {

      broadcast({

        type:
          "heartbeat",

        ts:
          Date.now(),

        live:
          rbnConnected,

        lastSeq:
          spotSequence

      });

    },
    APP_HEARTBEAT_MS
  );


wss.on(
  "close",
  () => {

    clearInterval(
      websocketHeartbeat
    );

    clearInterval(
      applicationHeartbeat
    );

  }
);


/*
 * ARRANQUE
 */
server.listen(
  PORT,
  () => {

    console.log(
      `CW LATAM relay activo en puerto ${PORT}`
    );


    connectRbn();

  }
);
