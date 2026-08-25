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

/*
 * Heartbeat visible para JavaScript.
 *
 * Este mensaje SÍ llega a onmessage()
 * del navegador.
 */
const APP_HEARTBEAT_MS = 15 * 1000;

const clients = new Set();

const history = [];

let rbn = null;
let buffer = "";
let reconnectTimer = null;

let lastRbnDataAt = 0;
let rbnConnected = false;


/*
 * Spotters de Sudamérica.
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

  const c =
    cleanCall(call);

  return SA_PREFIXES.some(
    prefix =>
      c.startsWith(prefix)
  );

}


/*
 * Broadcast a todos los navegadores.
 */
function broadcast(obj) {

  const data =
    JSON.stringify(obj);


  for (const ws of clients) {

    if (
      ws.readyState === 1
    ) {

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
 * HISTORIAL
 */
function pruneHistory() {

  const cutoff =
    Date.now() -
    HISTORY_MS;


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
 * PARSER
 *
 * Formato real:
 *
 * DX de WC2L-#: 7031.50 N9FGC
 * CW 5 dB 18 WPM CQ 2319Z
 */
function parseRbnLine(raw) {

  const line =
    String(raw || "")
      .replace(/\r/g, "")
      .trim();


  const match =
    line.match(
      /^DX de\s+([^:]+):\s+([0-9.]+)\s+(\S+)\s+(CW)\s+(-?\d+)\s+dB\s+(\d+)\s+WPM\s+(.+?)\s+([0-2]\d[0-5]\d)Z$/i
    );


  if (!match) {

    return null;

  }


  const spotter =
    cleanCall(
      match[1]
    );


  const actualFreq =
    Number(
      match[2]
    );


  const dx =
    cleanCall(
      match[3]
    );


  const mode =
    match[4]
      .toUpperCase();


  const snr =
    Number(
      match[5]
    );


  const wpm =
    Number(
      match[6]
    );


  const activity =
    match[7]
      .trim()
      .toUpperCase();


  const rbnUtc =
    match[8];


  /*
   * CW únicamente.
   */
  if (
    mode !== "CW"
  ) {

    return null;

  }


  /*
   * Solamente CQ.
   */
  if (
    activity !== "CQ"
  ) {

    return null;

  }


  /*
   * Banda 40 metros.
   */
  if (
    actualFreq < 7000 ||
    actualFreq > 7300
  ) {

    return null;

  }


  /*
   * Spotter Sudamérica.
   */
  if (
    !isSouthAmericaSpotter(
      spotter
    )
  ) {

    return null;

  }


  /*
   * Canal LXCW QRS
   *
   * ±100 Hz alrededor
   * de 7033.
   */
  const isChannel =
    actualFreq >= 7032.9 &&
    actualFreq <= 7033.1;


  const freq =
    isChannel
      ? 7033.0
      : actualFreq;


  return {

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
 * Programa reconexión RBN.
 */
function scheduleRbnReconnect() {

  clearTimeout(
    reconnectTimer
  );


  reconnectTimer =
    setTimeout(
      connectRbn,
      3000
    );

}


/*
 * CONEXIÓN TELNET
 */
function connectRbn() {

  clearTimeout(
    reconnectTimer
  );


  buffer = "";

  rbnConnected = false;


  console.log(
    `Conectando ${RBN_HOST}:${RBN_PORT} como ${RBN_LOGIN}...`
  );


  rbn =
    net.createConnection({

      host:
        RBN_HOST,

      port:
        RBN_PORT

    });


  /*
   * Evitamos buffering innecesario.
   */
  rbn.setNoDelay(
    true
  );


  rbn.setKeepAlive(
    true,
    20000
  );


  rbn.on(
    "connect",
    () => {

      rbnConnected = true;

      lastRbnDataAt =
        Date.now();


      console.log(
        "RBN TCP conectado"
      );


      /*
       * Login.
       */
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
   * DATOS RBN
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
        buffer.split(
          /\n/
        );


      /*
       * Posible línea
       * incompleta.
       */
      buffer =
        lines.pop() ||
        "";


      for (
        const line
        of lines
      ) {

        const spot =
          parseRbnLine(
            line
          );


        if (!spot) {

          continue;

        }


        /*
         * Guardamos.
         */
        rememberSpot(
          spot
        );


        /*
         * Enviamos INMEDIATAMENTE
         * a todos los navegadores.
         */
        broadcast(
          spot
        );


        const mark =
          spot.isChannel
            ? " *** 7033 ***"
            : "";


        console.log(

          `LIVE ` +

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
 *
 * Si la conexión TCP queda
 * abierta pero no entrega
 * absolutamente ningún dato
 * durante 90 segundos,
 * forzamos reconexión.
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
        "RBN sin datos durante 90 s. Reconectando..."
      );


      try {

        rbn.destroy();

      } catch (error) {

        // Ignorar.

      }

    }

  },
  15000
);


/*
 * SERVIDOR HTTP
 */
const server =
  http.createServer(
    (req, res) => {


      if (
        req.url ===
        "/health"
      ) {

        pruneHistory();


        res.writeHead(
          200,
          {

            "content-type":
              "application/json",

            "access-control-allow-origin":
              "*",

            "cache-control":
              "no-store"

          }
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

            filter:
              "CW / CQ / 40m / South America",

            channel:
              "7032.9-7033.1"

          })
        );


        return;

      }


      res.writeHead(
        200,
        {

          "content-type":
            "text/plain; charset=utf-8",

          "cache-control":
            "no-store"

        }
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

    /*
     * Mensajes diminutos.
     * No necesitamos compresión.
     */
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
     * Estado inicial.
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
     * Historial 10 minutos.
     */
    pruneHistory();


    ws.send(
      JSON.stringify({

        type:
          "history",

        spots:
          history

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
 * HEARTBEAT WEBSOCKET
 *
 * Este es ping/pong
 * a nivel protocolo.
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

          console.warn(
            "WebSocket congelado. Terminando."
          );


          clients.delete(
            ws
          );


          ws.terminate();

          continue;

        }


        ws.isAlive =
          false;


        try {

          ws.ping();

        } catch (error) {

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
 *
 * MUY IMPORTANTE.
 *
 * Esto sí llega al
 * JavaScript del navegador
 * mediante onmessage().
 *
 * La web espera recibir
 * algo al menos cada 40 s.
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
          rbnConnected

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
