const net = require("net");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 3000);

const RBN_HOST = "telnet.reversebeacon.net";
const RBN_PORT = 7000;

const RBN_LOGIN = String(
  process.env.RBN_LOGIN || "ZP5DXS"
).trim();

const clients = new Set();

const HISTORY_MS = 10 * 60 * 1000;
const history = [];

let rbn = null;
let buffer = "";
let reconnectTimer = null;


/*
 * Prefijos considerados Sudamérica.
 *
 * El filtro se aplica al SPOTTER.
 * La estación escuchada puede ser
 * de cualquier parte del mundo.
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

  return SA_PREFIXES.some(prefix =>
    c.startsWith(prefix)
  );
}


/*
 * Enviar un objeto JSON a todos
 * los navegadores conectados.
 */
function broadcast(obj) {
  const data = JSON.stringify(obj);

  for (const ws of clients) {
    if (ws.readyState === 1) {
      ws.send(data);
    }
  }
}


/*
 * El historial solamente conserva
 * los últimos 10 minutos.
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
 * Parser basado en el formato REAL
 * observado en Reverse Beacon Network:
 *
 * DX de WC2L-#:  7031.50  N9FGC
 * CW  5 dB  18 WPM  CQ  2319Z
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
   * FILTROS CW LATAM
   */


  /*
   * CW únicamente.
   */
  if (mode !== "CW") {
    return null;
  }


  /*
   * Solamente estaciones detectadas
   * llamando CQ.
   */
  if (activity !== "CQ") {
    return null;
  }


  /*
   * Banda completa de 40 metros.
   */
  if (
    actualFreq < 7000 ||
    actualFreq > 7300
  ) {
    return null;
  }


  /*
   * El receptor / spotter debe estar
   * en Sudamérica.
   */
  if (
    !isSouthAmericaSpotter(spotter)
  ) {
    return null;
  }


  /*
   * CANAL DE ENCUENTRO
   *
   * ±100 Hz alrededor de 7033 kHz.
   *
   * 7032.9 a 7033.1
   *
   * Si entra ahí, visualmente lo
   * normalizamos a 7033.0.
   *
   * Conservamos además actualFreq
   * para saber exactamente dónde
   * lo reportó RBN.
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

    /*
     * El timestamp local del servidor
     * permite manejar correctamente
     * los 10 minutos de vida.
     */
    ts: Date.now(),

    spotter: spotter,

    dx: dx,

    /*
     * Frecuencia usada por la interfaz.
     */
    freq: freq,

    /*
     * Frecuencia original recibida.
     */
    actualFreq: actualFreq,

    /*
     * Indica si pertenece al
     * canal prioritario 7033.
     */
    isChannel: isChannel,

    snr: snr,

    wpm: wpm,

    type: "CQ",

    mode: "CW",

    rbnUtc: rbnUtc
  };
}


/*
 * Conexión persistente con
 * Reverse Beacon Network.
 */
function connectRbn() {
  clearTimeout(
    reconnectTimer
  );

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


  rbn.on(
    "connect",
    () => {

      console.log(
        "RBN TCP connected"
      );


      /*
       * El servidor Telnet pide
       * un indicativo como login.
       */
      setTimeout(
        () => {

          if (
            rbn &&
            !rbn.destroyed
          ) {
            rbn.write(
              RBN_LOGIN + "\r\n"
            );
          }

        },
        700
      );

    }
  );


  /*
   * Flujo continuo del RBN.
   */
  rbn.on(
    "data",
    chunk => {

      buffer +=
        chunk.toString("utf8");


      /*
       * Un chunk puede contener
       * varias líneas.
       */
      const lines =
        buffer.split(/\n/);


      /*
       * Si la última línea quedó
       * incompleta, la conservamos
       * hasta el próximo chunk.
       */
      buffer =
        lines.pop() || "";


      for (const line of lines) {

        const spot =
          parseRbnLine(line);


        if (!spot) {
          continue;
        }


        /*
         * Guardamos el spot antes
         * de transmitirlo.
         *
         * Así un navegador que entre
         * después recibe el historial
         * reciente.
         */
        rememberSpot(spot);


        const channelMark =
          spot.isChannel
            ? " *** 7033 CHANNEL ***"
            : "";


        console.log(
          `MATCH ` +
          `${spot.spotter} -> ` +
          `${spot.dx} ` +
          `${spot.actualFreq.toFixed(2)} kHz ` +
          `${spot.snr} dB ` +
          `${spot.wpm} WPM` +
          channelMark
        );


        /*
         * Envío LIVE a los navegadores.
         */
        broadcast(spot);
      }

    }
  );


  /*
   * Reconexión automática.
   */
  const reconnect = () => {

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


    reconnectTimer =
      setTimeout(
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
 * Servidor HTTP requerido
 * por Render.
 */
const server =
  http.createServer(
    (req, res) => {


      /*
       * Endpoint de diagnóstico.
       */
      if (
        req.url === "/health"
      ) {

        pruneHistory();


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

            websocketClients:
              clients.size,

            historySpots:
              history.length,

            historyMinutes:
              10,

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
        "CW LATAM relay\n"
      );

    }
  );


/*
 * WebSocket utilizado por
 * index.html.
 */
const wss =
  new WebSocketServer({
    server
  });


wss.on(
  "connection",
  ws => {

    clients.add(ws);


    /*
     * Primero indicamos que
     * la conexión está activa.
     */
    ws.send(
      JSON.stringify({
        type: "status",
        rbn: "connected"
      })
    );


    /*
     * Después enviamos inmediatamente
     * todos los spots válidos de
     * los últimos 10 minutos.
     */
    pruneHistory();


    ws.send(
      JSON.stringify({
        type: "history",
        spots: history
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
 * Render define PORT
 * automáticamente.
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
