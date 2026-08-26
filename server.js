const net = require("net");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 3000);

const RBN_HOST = "telnet.reversebeacon.net";
const RBN_PORT = 7000;
const RBN_LOGIN = String(
  process.env.RBN_LOGIN || "ZP5DXS"
).trim();

/*
 * GREEN API / WHATSAPP
 *
 * IMPORTANTE:
 * Las credenciales viven únicamente en Render.
 * Nunca deben ir al HTML ni al repositorio.
 */
const GREEN_API_URL = String(
  process.env.GREEN_API_URL || ""
).trim().replace(/\/+$/, "");

const GREEN_ID_INSTANCE = String(
  process.env.GREEN_ID_INSTANCE || ""
).trim();

const GREEN_API_TOKEN = String(
  process.env.GREEN_API_TOKEN || ""
).trim();

const WHATSAPP_CHAT_ID = String(
  process.env.WHATSAPP_CHAT_ID || ""
).trim();

const WHATSAPP_ENABLED =
  String(process.env.WHATSAPP_ENABLED || "false")
    .trim()
    .toLowerCase() === "true";

const CW_LATAM_URL = String(
  process.env.CW_LATAM_URL ||
  "https://zp5dxs.github.io/CW-LATAM/"
).trim();

const WHATSAPP_ALERT_COOLDOWN_MS =
  10 * 60 * 1000;

const WHATSAPP_AGGREGATION_MS =
  5 * 1000;

const WHATSAPP_DIGEST_MS =
  60 * 60 * 1000;

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
 * ESTADO WHATSAPP
 */
const whatsappAlertedCalls = new Map();
const pendingChannelAlerts = new Map();

const whatsappHour = {
  startedAt: Date.now(),
  spots: 0,
  calls: new Set(),
  channelCalls: new Set(),
  spotters: new Set(),
  countries: new Set()
};

let whatsappQueue = Promise.resolve();
let whatsappLastAlertAt = 0;
let whatsappLastDigestAt = 0;
let whatsappLastError = "";
let whatsappSentAlerts = 0;
let whatsappSentDigests = 0;


/*
 * País aproximado para digest.
 */
function roughCountry(call) {
  const c = cleanCall(call);

  const rules = [
    [/^(ZP)/, "PY"],
    [/^(LU|LW|AY|AZ|LO|LP|LQ|LR|LS|LT|LV)/, "AR"],
    [/^(CX)/, "UY"],
    [/^(PY|PP|PQ|PR|PS|PT|PU|PV|PW|PX|ZY|ZZ)/, "BR"],
    [/^(CE|CA|CB|CC|CD|XQ)/, "CL"],
    [/^(OA|OB)/, "PE"],
    [/^(CP)/, "BO"],
    [/^(HC|HD)/, "EC"],
    [/^(YV|YW|YY)/, "VE"],
    [/^(HK|HJ)/, "CO"],
    [/^(FY)/, "GF"],
    [/^(8R)/, "GY"],
    [/^(PZ)/, "SR"]
  ];

  const found =
    rules.find(([re]) =>
      re.test(c)
    );

  return found
    ? found[1]
    : "DX";
}


function countryLabel(call) {
  const code =
    roughCountry(call);

  const countries = {
    PY: "🇵🇾 Paraguay",
    AR: "🇦🇷 Argentina",
    UY: "🇺🇾 Uruguay",
    BR: "🇧🇷 Brasil",
    CL: "🇨🇱 Chile",
    PE: "🇵🇪 Perú",
    BO: "🇧🇴 Bolivia",
    EC: "🇪🇨 Ecuador",
    VE: "🇻🇪 Venezuela",
    CO: "🇨🇴 Colombia",
    GF: "🇬🇫 Guayana Francesa",
    GY: "🇬🇾 Guyana",
    SR: "🇸🇷 Surinam",
    DX: "🌎 DX"
  };

  return countries[code] ||
    "🌎 DX";
}


/*
 * GREEN API
 */
function whatsappConfigured() {
  return Boolean(
    WHATSAPP_ENABLED &&
    GREEN_API_URL &&
    GREEN_ID_INSTANCE &&
    GREEN_API_TOKEN &&
    WHATSAPP_CHAT_ID
  );
}


function safeDestinationLabel() {
  if (!WHATSAPP_CHAT_ID) {
    return "not configured";
  }

  if (
    WHATSAPP_CHAT_ID.endsWith(
      "@g.us"
    )
  ) {
    return "group configured";
  }

  if (
    WHATSAPP_CHAT_ID.endsWith(
      "@c.us"
    )
  ) {
    const number =
      WHATSAPP_CHAT_ID.replace(
        "@c.us",
        ""
      );

    return number.length >= 4
      ? `***${number.slice(-4)}@c.us`
      : "private chat configured";
  }

  return "configured";
}


async function greenApiSend(message) {
  if (!whatsappConfigured()) {
    return {
      ok: false,
      skipped: true,
      reason:
        "WHATSAPP_NOT_CONFIGURED"
    };
  }

  const endpoint =
    `${GREEN_API_URL}` +
    `/waInstance${GREEN_ID_INSTANCE}` +
    `/sendMessage/${GREEN_API_TOKEN}`;

  const response =
    await fetch(
      endpoint,
      {
        method: "POST",

        headers: {
          "content-type":
            "application/json; charset=utf-8"
        },

        body:
          JSON.stringify({
            chatId:
              WHATSAPP_CHAT_ID,

            message,

            linkPreview:
              true
          })
      }
    );

  const text =
    await response.text();

  let body = null;

  try {
    body =
      JSON.parse(text);
  } catch (_) {
    body = {
      raw: text
    };
  }

  if (!response.ok) {
    throw new Error(
      `GREEN API HTTP ${response.status}: ` +
      `${text.slice(0, 300)}`
    );
  }

  whatsappLastError = "";

  return {
    ok: true,
    response: body
  };
}


/*
 * Cola secuencial WhatsApp.
 */
function enqueueWhatsapp(
  message,
  kind
) {
  whatsappQueue =
    whatsappQueue
      .then(
        async () => {

          const result =
            await greenApiSend(
              message
            );

          if (result.ok) {

            if (
              kind ===
              "alert"
            ) {
              whatsappSentAlerts++;

              whatsappLastAlertAt =
                Date.now();
            }

            if (
              kind ===
              "digest"
            ) {
              whatsappSentDigests++;

              whatsappLastDigestAt =
                Date.now();
            }
          }

          return result;
        }
      )
      .catch(
        error => {

          whatsappLastError =
            error.message;

          console.error(
            "WhatsApp:",
            error.message
          );

          return {
            ok: false,
            error:
              error.message
          };
        }
      );

  return whatsappQueue;
}


/*
 * ALERTA 7033 AGRUPADA
 *
 * Esperamos 5 segundos para juntar
 * todos los receptores del mismo CALL.
 */
function formatChannelAlert(alert) {

  const bestSnr =
    Number.isFinite(
      Number(alert.bestSnr)
    )
      ? `${
          Number(alert.bestSnr) >= 0
            ? "+"
            : ""
        }${alert.bestSnr} dB`
      : "SNR —";

  const receivers =
    alert.spotters.size === 1
      ? "1 receptor lo escucha"
      : `${alert.spotters.size} receptores lo escuchan`;

  return [
    "🚨 *CQ EN 7.033 · CW LATAM*",
    "",
    `📡 *${alert.dx}* llamando CQ`,
    `🌎 ${countryLabel(alert.dx)}`,
    `👂 *${receivers}*`,
    `⚡ ${alert.wpm} WPM · señal máx. ${bestSnr}`,
    "",
    `🔗 ${CW_LATAM_URL}`
  ].join("\n");
}


function flushChannelWhatsapp(call) {

  const alert =
    pendingChannelAlerts.get(
      call
    );

  if (!alert) {
    return;
  }

  pendingChannelAlerts.delete(
    call
  );

  /*
   * El cooldown empieza al generar
   * la alerta agrupada.
   */
  whatsappAlertedCalls.set(
    call,
    Date.now()
  );

  console.log(
    `WHATSAPP 7033 -> ${call} · ` +
    `${alert.spotters.size} receptores`
  );

  enqueueWhatsapp(
    formatChannelAlert(
      alert
    ),
    "alert"
  );
}


function maybeSendChannelWhatsapp(
  spot
) {

  if (
    !spot ||
    !spot.isChannel
  ) {
    return;
  }

  const call =
    cleanCall(
      spot.dx
    );

  const last =
    Number(
      whatsappAlertedCalls.get(
        call
      ) || 0
    );

  /*
   * Cooldown:
   * una alerta por CALL cada 10 minutos.
   */
  if (
    Date.now() - last <
    WHATSAPP_ALERT_COOLDOWN_MS
  ) {
    return;
  }

  let alert =
    pendingChannelAlerts.get(
      call
    );

  /*
   * Primer spot de este CALL.
   * Abrimos ventana de 5 segundos.
   */
  if (!alert) {

    alert = {
      dx:
        call,

      actualFreq:
        Number(
          spot.actualFreq
        ),

      wpm:
        Number(spot.wpm) || 0,

      bestSnr:
        Number.isFinite(
          Number(spot.snr)
        )
          ? Number(spot.snr)
          : null,

      spotters:
        new Set(),

      startedAt:
        Date.now(),

      timer:
        null
    };

    alert.timer =
      setTimeout(
        () =>
          flushChannelWhatsapp(
            call
          ),
        WHATSAPP_AGGREGATION_MS
      );

    pendingChannelAlerts.set(
      call,
      alert
    );
  }

  /*
   * Agregar receptor único.
   */
  alert.spotters.add(
    cleanCall(
      spot.spotter
    )
  );

  /*
   * Conservar mejor SNR.
   */
  if (
    Number.isFinite(
      Number(spot.snr)
    ) &&
    (
      alert.bestSnr === null ||
      Number(spot.snr) >
        alert.bestSnr
    )
  ) {
    alert.bestSnr =
      Number(
        spot.snr
      );
  }

  /*
   * Último WPM válido.
   */
  if (
    Number.isFinite(
      Number(spot.wpm)
    ) &&
    Number(spot.wpm) > 0
  ) {
    alert.wpm =
      Number(
        spot.wpm
      );
  }

  if (
    Number.isFinite(
      Number(spot.actualFreq)
    )
  ) {
    alert.actualFreq =
      Number(
        spot.actualFreq
      );
  }
}


/*
 * ESTADÍSTICAS PARA DIGEST
 */
function recordWhatsappHour(
  spot
) {

  if (!spot) {
    return;
  }

  whatsappHour.spots++;

  whatsappHour.calls.add(
    cleanCall(
      spot.dx
    )
  );

  whatsappHour.spotters.add(
    cleanCall(
      spot.spotter
    )
  );

  whatsappHour.countries.add(
    roughCountry(
      spot.dx
    )
  );

  if (
    spot.isChannel
  ) {
    whatsappHour.channelCalls.add(
      cleanCall(
        spot.dx
      )
    );
  }
}


function resetWhatsappHour() {

  whatsappHour.startedAt =
    Date.now();

  whatsappHour.spots = 0;

  whatsappHour.calls.clear();

  whatsappHour.channelCalls.clear();

  whatsappHour.spotters.clear();

  whatsappHour.countries.clear();
}


/*
 * DIGEST HORARIO
 */
function formatWhatsappDigest() {

  return [
    "📊 *CW LATAM · ÚLTIMA HORA*",
    "",
    `📡 ${whatsappHour.calls.size} estaciones`,
    `📶 ${whatsappHour.spots} spots válidos`,
    `🚨 ${whatsappHour.channelCalls.size} llamadas en 7.033`,
    `👂 ${whatsappHour.spotters.size} receptores SA`,
    `🌎 ${whatsappHour.countries.size} países / regiones DX`,
    "",
    `🔗 ${CW_LATAM_URL}`
  ].join("\n");
}


async function sendWhatsappDigest() {

  /*
   * No mandar digest vacío.
   */
  if (
    whatsappHour.spots <= 0
  ) {
    resetWhatsappHour();
    return;
  }

  const message =
    formatWhatsappDigest();

  resetWhatsappHour();

  console.log(
    "WHATSAPP digest horario"
  );

  await enqueueWhatsapp(
    message,
    "digest"
  );
}


/*
 * Limpieza de cooldown.
 */
setInterval(
  () => {

    const now =
      Date.now();

    for (
      const [call, ts]
      of whatsappAlertedCalls
    ) {

      if (
        now - ts >
        WHATSAPP_ALERT_COOLDOWN_MS * 2
      ) {
        whatsappAlertedCalls.delete(
          call
        );
      }
    }
  },
  5 * 60 * 1000
);


/*
 * Digest cada 60 minutos.
 */
setInterval(
  sendWhatsappDigest,
  WHATSAPP_DIGEST_MS
);


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


function isSouthAmericaSpotter(
  call
) {

  const c =
    cleanCall(call);

  return SA_PREFIXES.some(
    prefix =>
      c.startsWith(prefix)
  );
}


/*
 * BROADCAST WEBSOCKET
 */
function broadcast(obj) {

  const data =
    JSON.stringify(obj);

  for (
    const ws
    of clients
  ) {

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
 * HISTORIAL 10 MIN
 */
function pruneHistory() {

  const cutoff =
    Date.now() -
    HISTORY_MS;

  while (
    history.length &&
    history[0].ts <
      cutoff
  ) {
    history.shift();
  }
}


function rememberSpot(spot) {

  history.push(
    spot
  );

  pruneHistory();
}


/*
 * PARSER RBN
 */
function parseRbnLine(raw) {

  const line =
    String(raw || "")
      .replace(
        /\r/g,
        ""
      )
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
   * FILTROS
   */
  if (
    mode !== "CW"
  ) {
    return null;
  }


  if (
    activity !== "CQ"
  ) {
    return null;
  }


  if (
    actualFreq < 7000 ||
    actualFreq > 7300
  ) {
    return null;
  }


  if (
    !isSouthAmericaSpotter(
      spotter
    )
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
 * TELNET RBN
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


  rbn.setNoDelay(
    true
  );


  rbn.setKeepAlive(
    true,
    20000
  );


  /*
   * CONECTADO
   */
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
        buffer.split(
          /\n/
        );

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


        /*
         * Estadísticas digest.
         */
        recordWhatsappHour(
          spot
        );


        /*
         * WhatsApp:
         * exclusivamente 7033.
         *
         * Esta función agrupa durante
         * 5 segundos antes de enviar.
         */
        maybeSendChannelWhatsapp(
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


  /*
   * ERROR
   */
  rbn.on(
    "error",
    error => {

      console.error(
        "RBN error:",
        error.message
      );
    }
  );


  /*
   * CIERRE / RECONEXIÓN
   */
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
function corsHeaders(
  extra = {}
) {

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
       * /spots?after=123
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
            ) || 0
          );

        if (
          !Number.isFinite(
            after
          ) ||
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
       * /health
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
                    ) / 1000
                  )
                : null,

            channel:
              "7032.9-7033.1",

            whatsapp: {

              enabled:
                WHATSAPP_ENABLED,

              configured:
                whatsappConfigured(),

              destination:
                safeDestinationLabel(),

              provider:
                "GREEN API",

              alertCooldownMinutes:
                10,

              aggregationSeconds:
                5,

              pendingAlerts:
                pendingChannelAlerts.size,

              digestMinutes:
                60,

              sentAlerts:
                whatsappSentAlerts,

              sentDigests:
                whatsappSentDigests,

              lastAlertAt:
                whatsappLastAlertAt ||
                null,

              lastDigestAt:
                whatsappLastDigestAt ||
                null,

              lastError:
                whatsappLastError ||
                null
            }
          })
        );

        return;
      }


      /*
       * ROOT
       */
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

    console.log(
      "WhatsApp:",
      whatsappConfigured()
        ? `ACTIVO -> ${safeDestinationLabel()}`
        : "DESACTIVADO / incompleto"
    );

    connectRbn();
  }
);
