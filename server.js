const net = require("net");
const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 10000;

/*
 * ============================================================
 * CW LATAM — RBN RELAY + GREEN API WHATSAPP
 * ============================================================
 */

/*
 * RBN
 */
const RBN_HOST = "telnet.reversebeacon.net";
const RBN_PORT = 7000;

const RBN_LOGIN = String(
  process.env.RBN_LOGIN || "ZP5DXS"
).trim();


/*
 * ============================================================
 * GREEN API / WHATSAPP
 * ============================================================
 *
 * CONFIGURAR EN RENDER:
 *
 * GREEN_API_URL=https://7107.api.greenapi.com
 * GREEN_ID_INSTANCE=710722718606
 * GREEN_API_TOKEN=TU_TOKEN
 *
 * WHATSAPP_CHAT_ID=120363409458161118@g.us
 * WHATSAPP_ENABLED=true
 *
 * CW_LATAM_URL=https://zp5dxs.github.io/CW-LATAM/
 *
 * IMPORTANTE:
 * El token NO debe escribirse dentro de este archivo.
 */

const GREEN_API_URL = String(
  process.env.GREEN_API_URL || ""
)
  .trim()
  .replace(/\/+$/, "");

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
  String(
    process.env.WHATSAPP_ENABLED || "false"
  )
    .trim()
    .toLowerCase() === "true";

const CW_LATAM_URL = String(
  process.env.CW_LATAM_URL ||
    "https://zp5dxs.github.io/CW-LATAM/"
).trim();


/*
 * ============================================================
 * CONFIGURACIÓN WHATSAPP
 * ============================================================
 */

const WHATSAPP_ALERT_COOLDOWN_MS =
  10 * 60 * 1000;

const WHATSAPP_DIGEST_MS =
  60 * 60 * 1000;


/*
 * ============================================================
 * EXPRESS / HTTP / WEBSOCKET
 * ============================================================
 */

const app = express();
const server = http.createServer(app);

const wss = new WebSocketServer({
  server
});


/*
 * ============================================================
 * ESTADO GENERAL
 * ============================================================
 */

let rbnSocket = null;
let rbnConnected = false;

let lastRbnDataAt = 0;
let lastAcceptedSpotAt = 0;

let acceptedSpots = 0;
let rejectedSpots = 0;

let spotSequence = 0;


/*
 * ============================================================
 * ESTADO WHATSAPP
 * ============================================================
 */

const whatsappAlertedCalls =
  new Map();

const whatsappHour = {
  startedAt: Date.now(),

  spots: 0,

  calls: new Set(),

  channelCalls: new Set(),

  spotters: new Set(),

  countries: new Set()
};

let whatsappQueue =
  Promise.resolve();

let whatsappLastAlertAt = 0;
let whatsappLastDigestAt = 0;

let whatsappLastError = "";

let whatsappSentAlerts = 0;
let whatsappSentDigests = 0;


/*
 * ============================================================
 * UTILIDADES
 * ============================================================
 */

function cleanCall(call) {
  return String(call || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9/]/g, "");
}


function roughCountry(call) {
  const c = cleanCall(call);

  const rules = [

    [/^(ZP)/, "PY"],

    [
      /^(LU|LW|AY|AZ|LO|LP|LQ|LR|LS|LT|LV)/,
      "AR"
    ],

    [/^(CX)/, "UY"],

    [
      /^(PY|PP|PQ|PR|PS|PT|PU|PV|PW|PX|ZY|ZZ)/,
      "BR"
    ],

    [
      /^(CE|CA|CB|CC|CD|XQ)/,
      "CL"
    ],

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


/*
 * ============================================================
 * GREEN API
 * ============================================================
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


/*
 * ENVÍO DIRECTO GREEN API
 */

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

        body: JSON.stringify({

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
 * ============================================================
 * COLA WHATSAPP
 * ============================================================
 *
 * Si aparecen dos estaciones casi simultáneamente,
 * los mensajes se envían secuencialmente.
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
              kind === "alert"
            ) {

              whatsappSentAlerts++;

              whatsappLastAlertAt =
                Date.now();

            }


            if (
              kind === "digest"
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
 * ============================================================
 * MENSAJE ALERTA 7033
 * ============================================================
 */

function formatChannelAlert(
  spot
) {

  const snr =

    Number.isFinite(
      Number(spot.snr)
    )

      ? `${
          Number(spot.snr) >= 0
            ? "+"
            : ""
        }${spot.snr} dB`

      : "SNR —";


  return [

    "🚨 *CW LATAM · 7.033 MHz*",

    "",

    `📡 *${spot.dx}* llamando CQ`,

    `⚡ ${spot.actualFreq.toFixed(2)} kHz · ${spot.wpm} WPM · ${snr}`,

    `👂 Detectado por ${spot.spotter}`,

    "",

    `🔗 ${CW_LATAM_URL}`

  ].join("\n");

}


/*
 * ============================================================
 * ALERTA 7033
 * ============================================================
 */

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
   * MISMO CALL:
   * máximo una alerta cada 10 minutos.
   */

  if (
    Date.now() - last <
    WHATSAPP_ALERT_COOLDOWN_MS
  ) {

    return;

  }


  whatsappAlertedCalls.set(

    call,

    Date.now()

  );


  console.log(

    `WHATSAPP 7033 -> ${call}`

  );


  enqueueWhatsapp(

    formatChannelAlert(
      spot
    ),

    "alert"

  );

}


/*
 * ============================================================
 * ESTADÍSTICAS HORARIAS
 * ============================================================
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


  whatsappHour.spots =
    0;


  whatsappHour.calls.clear();

  whatsappHour.channelCalls.clear();

  whatsappHour.spotters.clear();

  whatsappHour.countries.clear();

}


/*
 * ============================================================
 * DIGEST HORARIO
 * ============================================================
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
   * No mandamos mensajes si
   * no hubo actividad.
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
 * ============================================================
 * LIMPIEZA COOLDOWN
 * ============================================================
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
 * ============================================================
 * DIGEST CADA HORA
 * ============================================================
 */

setInterval(

  sendWhatsappDigest,

  WHATSAPP_DIGEST_MS

);


/*
 * ============================================================
 * WEBSOCKET
 * ============================================================
 */

function broadcast(data) {

  const payload =
    JSON.stringify(data);


  for (
    const client
    of wss.clients
  ) {

    if (
      client.readyState === 1
    ) {

      client.send(
        payload
      );

    }

  }

}


wss.on(
  "connection",

  ws => {

    console.log(
      "WS cliente conectado"
    );


    ws.send(

      JSON.stringify({

        type:
          "status",

        status:
          "connected",

        rbnConnected,

        timestamp:
          Date.now()

      })

    );


    ws.on(
      "close",

      () => {

        console.log(
          "WS cliente desconectado"
        );

      }

    );

  }

);


/*
 * ============================================================
 * PARSER RBN
 * ============================================================
 *
 * Reverse Beacon Network entrega líneas DX cluster.
 *
 * Ejemplo aproximado:
 *
 * DX de W3LPL-#: 7033.0 ZP5DXS CW 15 dB 18 WPM CQ
 */

function parseRbnLine(line) {

  if (
    !line ||
    !line.includes("DX de")
  ) {

    return null;

  }


  try {

    const normalized =
      line
        .replace(/\s+/g, " ")
        .trim();


    /*
     * Extraemos spotter
     */

    const spotterMatch =
      normalized.match(
        /DX de\s+([A-Z0-9\/\-#]+):/i
      );


    if (!spotterMatch) {

      return null;

    }


    let spotter =
      cleanCall(
        spotterMatch[1]
          .replace(/-#$/, "")
      );


    /*
     * Parte posterior a :
     */

    const afterColon =
      normalized.split(":")
        .slice(1)
        .join(":")
        .trim();


    const parts =
      afterColon.split(" ");


    if (
      parts.length < 2
    ) {

      return null;

    }


    const freq =
      Number(
        parts[0]
      );


    const dx =
      cleanCall(
        parts[1]
      );


    if (
      !Number.isFinite(freq) ||
      !dx
    ) {

      return null;

    }


    /*
     * Solamente 40 metros
     */

    if (
      freq < 7000 ||
      freq > 7060
    ) {

      return null;

    }


    /*
     * Buscar SNR
     */

    let snr = null;

    const snrMatch =
      normalized.match(
        /(-?\d+)\s+dB/i
      );


    if (snrMatch) {

      snr =
        Number(
          snrMatch[1]
        );

    }


    /*
     * Buscar WPM
     */

    let wpm = null;

    const wpmMatch =
      normalized.match(
        /(\d+)\s+WPM/i
      );


    if (wpmMatch) {

      wpm =
        Number(
          wpmMatch[1]
        );

    }


    /*
     * CQ
     */

    const isCQ =
      /\bCQ\b/i.test(
        normalized
      );


    /*
     * Canal especial 7033
     */

    const isChannel =

      freq >= 7032.9 &&

      freq <= 7033.1;


    return {

      type:
        "spot",

      seq:
        ++spotSequence,

      dx,

      spotter,

      freq,

      actualFreq:
        freq,

      snr,

      wpm:
        Number.isFinite(wpm)
          ? wpm
          : 0,

      isCQ,

      isChannel,

      timestamp:
        Date.now(),

      raw:
        normalized

    };

  } catch (error) {

    console.error(

      "parseRbnLine:",

      error.message

    );


    return null;

  }

}


/*
 * ============================================================
 * CONEXIÓN RBN
 * ============================================================
 */

function connectRbn() {

  if (rbnSocket) {

    try {

      rbnSocket.destroy();

    } catch (_) {}

  }


  console.log(

    `Conectando RBN ${RBN_HOST}:${RBN_PORT}...`

  );


  const socket =
    net.createConnection({

      host:
        RBN_HOST,

      port:
        RBN_PORT

    });


  rbnSocket =
    socket;


  let buffer = "";


  socket.setEncoding(
    "utf8"
  );


  socket.on(
    "connect",

    () => {

      rbnConnected =
        true;


      console.log(
        "RBN conectado"
      );


      /*
       * Login
       */

      socket.write(
        `${RBN_LOGIN}\r\n`
      );


      broadcast({

        type:
          "status",

        status:
          "rbn_connected",

        timestamp:
          Date.now()

      });

    }

  );


  socket.on(
    "data",

    chunk => {

      lastRbnDataAt =
        Date.now();


      buffer +=
        chunk;


      const lines =
        buffer.split(
          /\r?\n/
        );


      buffer =
        lines.pop() || "";


      for (
        const line
        of lines
      ) {

        const spot =
          parseRbnLine(
            line
          );


        if (!spot) {

          rejectedSpots++;

          continue;

        }


        acceptedSpots++;

        lastAcceptedSpotAt =
          Date.now();


        /*
         * WEB
         */

        broadcast(
          spot
        );


        /*
         * DIGEST
         */

        recordWhatsappHour(
          spot
        );


        /*
         * WHATSAPP
         *
         * EXCLUSIVAMENTE
         * 7032.9–7033.1
         */

        maybeSendChannelWhatsapp(
          spot
        );

      }

    }

  );


  socket.on(
    "error",

    error => {

      console.error(

        "RBN error:",

        error.message

      );

    }

  );


  socket.on(
    "close",

    () => {

      rbnConnected =
        false;


      console.log(

        "RBN desconectado. Reconectando..."

      );


      broadcast({

        type:
          "status",

        status:
          "rbn_disconnected",

        timestamp:
          Date.now()

      });


      setTimeout(

        connectRbn,

        5000

      );

    }

  );

}


/*
 * ============================================================
 * HEALTH
 * ============================================================
 */

app.get(

  "/health",

  (req, res) => {

    res.json({

      ok:
        true,

      service:
        "CW LATAM",

      timestamp:
        Date.now(),

      rbn: {

        connected:
          rbnConnected,

        login:
          RBN_LOGIN,

        acceptedSpots,

        rejectedSpots,

        lastDataAt:
          lastRbnDataAt || null,

        lastAcceptedSpotAt:
          lastAcceptedSpotAt || null,

        band:
          "7000-7060",

        channel:
          "7032.9-7033.1"

      },

      websocket: {

        clients:
          wss.clients.size

      },

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

        digestMinutes:
          60,

        sentAlerts:
          whatsappSentAlerts,

        sentDigests:
          whatsappSentDigests,

        lastAlertAt:
          whatsappLastAlertAt || null,

        lastDigestAt:
          whatsappLastDigestAt || null,

        lastError:
          whatsappLastError || null

      }

    });

  }

);


/*
 * ============================================================
 * ROOT
 * ============================================================
 */

app.get(

  "/",

  (req, res) => {

    res.send(
      "CW LATAM relay online"
    );

  }

);


/*
 * ============================================================
 * ARRANQUE
 * ============================================================
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
