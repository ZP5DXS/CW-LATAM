const http = require("http");
const net = require("net");

const PORT = Number(process.env.PORT || 3000);

const SDR_UPSTREAM_HOST = String(
  process.env.SDR_UPSTREAM_HOST || "pardinho.websdr.com.br"
).trim();

const SDR_UPSTREAM_PORT = Number(
  process.env.SDR_UPSTREAM_PORT || 8073
);

function upstreamHeaders(headers) {
  const out = { ...headers };

  out.host = `${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`;

  if (out.origin) {
    out.origin = `http://${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`;
  }

  delete out["content-length"];

  return out;
}

function proxyHttp(req, res) {
  const up = http.request(
    {
      hostname: SDR_UPSTREAM_HOST,
      port: SDR_UPSTREAM_PORT,
      method: req.method,
      path: req.url,
      headers: upstreamHeaders(req.headers)
    },
    upRes => {
      const headers = { ...upRes.headers };

      // Evita que el upstream bloquee el iframe.
      delete headers["x-frame-options"];
      delete headers["content-security-policy"];
      delete headers["content-security-policy-report-only"];

      res.writeHead(upRes.statusCode || 200, headers);

      upRes.pipe(res);
    }
  );

  up.setTimeout(15000, () => {
    up.destroy(new Error("upstream timeout"));
  });

  up.on("error", err => {
    console.error("SDR HTTP:", err.message);

    if (!res.headersSent) {
      res.writeHead(502, {
        "content-type": "text/plain; charset=utf-8",
        "cache-control": "no-store"
      });
    }

    res.end("SDR unavailable");
  });

  req.pipe(up);
}

function proxyUpgrade(req, clientSocket, head) {
  const headers = upstreamHeaders(req.headers);

  headers.connection = "Upgrade";
  headers.upgrade = "websocket";

  const upReq = http.request({
    hostname: SDR_UPSTREAM_HOST,
    port: SDR_UPSTREAM_PORT,
    method: "GET",
    path: req.url,
    headers
  });

  upReq.on("upgrade", (upRes, upSocket, upHead) => {
    let response =
      `HTTP/1.1 ${upRes.statusCode || 101} ` +
      `${upRes.statusMessage || "Switching Protocols"}\r\n`;

    for (const [key, value] of Object.entries(upRes.headers)) {
      if (value == null) continue;

      if (Array.isArray(value)) {
        for (const item of value) {
          response += `${key}: ${item}\r\n`;
        }
      } else {
        response += `${key}: ${value}\r\n`;
      }
    }

    response += "\r\n";

    clientSocket.write(response);

    if (head && head.length) {
      upSocket.write(head);
    }

    if (upHead && upHead.length) {
      clientSocket.write(upHead);
    }

    clientSocket.pipe(upSocket);
    upSocket.pipe(clientSocket);

    const closeBoth = () => {
      try {
        clientSocket.destroy();
      } catch {}

      try {
        upSocket.destroy();
      } catch {}
    };

    clientSocket.on("error", closeBoth);
    upSocket.on("error", closeBoth);

    clientSocket.on("close", () => {
      try {
        upSocket.destroy();
      } catch {}
    });

    upSocket.on("close", () => {
      try {
        clientSocket.destroy();
      } catch {}
    });
  });

  upReq.on("response", upRes => {
    let response =
      `HTTP/1.1 ${upRes.statusCode || 502} ` +
      `${upRes.statusMessage || "Bad Gateway"}\r\n`;

    for (const [key, value] of Object.entries(upRes.headers)) {
      if (value == null) continue;

      response += `${key}: ${
        Array.isArray(value) ? value.join(", ") : value
      }\r\n`;
    }

    response += "\r\n";

    clientSocket.write(response);

    upRes.pipe(clientSocket);
  });

  upReq.setTimeout(15000, () => {
    upReq.destroy(new Error("WS upstream timeout"));
  });

  upReq.on("error", err => {
    console.error("SDR WS:", err.message);

    try {
      clientSocket.write(
        "HTTP/1.1 502 Bad Gateway\r\n" +
        "Connection: close\r\n\r\n"
      );
    } catch {}

    try {
      clientSocket.destroy();
    } catch {}
  });

  upReq.end();
}

function checkUpstream() {
  return new Promise(resolve => {
    const socket = net.createConnection({
      host: SDR_UPSTREAM_HOST,
      port: SDR_UPSTREAM_PORT
    });

    let done = false;

    const finish = ok => {
      if (done) return;

      done = true;

      try {
        socket.destroy();
      } catch {}

      resolve(ok);
    };

    socket.setTimeout(3000);

    socket.on("connect", () => {
      finish(true);
    });

    socket.on("timeout", () => {
      finish(false);
    });

    socket.on("error", () => {
      finish(false);
    });
  });
}

const server = http.createServer((req, res) => {
  let pathname = "/";

  try {
    pathname = new URL(
      req.url,
      `http://${req.headers.host || "localhost"}`
    ).pathname;
  } catch {}

  /*
   * Endpoint para comprobar que Render
   * puede alcanzar al KiwiSDR.
   */
  if (pathname === "/__health") {
    void checkUpstream().then(ok => {
      res.writeHead(ok ? 200 : 503, {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store",
        "access-control-allow-origin": "*"
      });

      res.end(
        JSON.stringify({
          ok,
          upstream: `${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`
        })
      );
    });

    return;
  }

  /*
   * Todo lo demás se envía transparentemente
   * al KiwiSDR.
   */
  proxyHttp(req, res);
});

/*
 * Los WebSocket de Kiwi:
 * /kiwi/.../SND
 * /kiwi/.../W/F
 * /kiwi/.../EXT
 * etc.
 */
server.on("upgrade", proxyUpgrade);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`SDR secure proxy listening on ${PORT}`);
  console.log(
    `Upstream: ${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`
  );
});
