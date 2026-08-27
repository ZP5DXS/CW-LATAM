const http = require("http");
const net = require("net");

const PORT = Number(process.env.PORT || 3000);
const SDR_UPSTREAM_HOST = String(
  process.env.SDR_UPSTREAM_HOST || "pardinho.websdr.com.br"
).trim();
const SDR_UPSTREAM_PORT = Number(process.env.SDR_UPSTREAM_PORT || 8073);

const PLAYER_HTML = `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
html,body{margin:0;width:100%;height:100%;overflow:hidden;background:transparent}
button{
 width:100%;height:100%;border:1px solid #173426;border-radius:9px;
 background:#0c1c13;color:#58ff92;font:900 15px/1 system-ui;
 display:flex;align-items:center;justify-content:center;padding:0;cursor:pointer;
}
button.on{
 border-color:rgba(88,255,146,.45);
 background:rgba(88,255,146,.07);
 box-shadow:inset 0 0 12px rgba(88,255,146,.06)
}
#kiwi{
 position:fixed;left:-20px;top:-20px;width:2px;height:2px;border:0;
 opacity:.01;pointer-events:none
}
</style>
</head>
<body>
<button id="p" aria-label="Play / Pause SDR">▶</button>
<iframe id="kiwi" allow="autoplay *"></iframe>
<script>
const btn=document.getElementById("p");
const kiwi=document.getElementById("kiwi");
const HOME=7033;
const RELEASE=1800;
const ONLINE_GRACE=3500;

let on=false;
let freq=HOME;
let seq=0;
let timer=null;

function url(f){
 return \`/?f=\${Number(f).toFixed(1)}cw&no_wf&vol=100&user=CW_LATAM&_=\${Date.now()}\`;
}
function send(type,extra={}){
 try{
   parent.postMessage(
     {source:"cwlatam-sdr-player",type,freq,on,...extra},
     "*"
   );
 }catch{}
}
function ui(){
 btn.textContent=on?"❚❚":"▶";
 btn.classList.toggle("on",on);
}
function openFromGesture(){
 on=true;
 seq++;
 clearTimeout(timer);
 ui();
 send("connecting");
 kiwi.src=url(freq);
 send("play");
 timer=setTimeout(()=>send("online"),ONLINE_GRACE);
}
function pause(){
 on=false;
 seq++;
 clearTimeout(timer);
 kiwi.src="about:blank";
 ui();
 send("pause");
}
function tune(f){
 freq=Number(f)||HOME;
 if(!on){
   send("armed",{freq});
   return;
 }
 const my=++seq;
 clearTimeout(timer);
 send("connecting",{freq});
 kiwi.src="about:blank";
 setTimeout(()=>{
   if(!on||my!==seq)return;
   kiwi.src=url(freq);
   timer=setTimeout(()=>send("online",{freq}),ONLINE_GRACE);
 },RELEASE);
}
btn.addEventListener("click",()=>{
 if(on)pause();
 else openFromGesture();
});
window.addEventListener("message",e=>{
 const d=e.data||{};
 if(d.source!=="cwlatam-parent")return;
 if(d.type==="tune")tune(d.freq);
 if(d.type==="pause")pause();
 if(d.type==="arm")freq=Number(d.freq)||HOME;
});
kiwi.addEventListener("load",()=>{
 if(on && kiwi.src && kiwi.src!=="about:blank"){
   clearTimeout(timer);
   send("online",{freq});
 }
});
ui();
send("ready");
</script>
</body>
</html>`;

function upstreamHeaders(headers) {
  const out = { ...headers };
  out.host = `${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`;

  // Kiwi funciona mejor viendo su upstream real.
  if (out.origin) {
    out.origin = `http://${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`;
  }

  // Render termina HTTPS; hacia Kiwi hablamos HTTP normal.
  delete out["content-length"];
  return out;
}

function proxyHttp(req, res) {
  const up = http.request({
    hostname: SDR_UPSTREAM_HOST,
    port: SDR_UPSTREAM_PORT,
    method: req.method,
    path: req.url,
    headers: upstreamHeaders(req.headers)
  }, upRes => {
    const headers = { ...upRes.headers };

    // No permitimos que el upstream bloquee el iframe de CW LATAM.
    delete headers["x-frame-options"];
    delete headers["content-security-policy"];
    delete headers["content-security-policy-report-only"];

    res.writeHead(upRes.statusCode || 200, headers);
    upRes.pipe(res);
  });

  up.setTimeout(15000, () => up.destroy(new Error("upstream timeout")));

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

    for (const [k, v] of Object.entries(upRes.headers)) {
      if (v == null) continue;
      if (Array.isArray(v)) {
        for (const item of v) response += `${k}: ${item}\r\n`;
      } else {
        response += `${k}: ${v}\r\n`;
      }
    }

    response += "\r\n";
    clientSocket.write(response);

    if (head && head.length) upSocket.write(head);
    if (upHead && upHead.length) clientSocket.write(upHead);

    clientSocket.pipe(upSocket);
    upSocket.pipe(clientSocket);

    const closeBoth = () => {
      try { clientSocket.destroy(); } catch {}
      try { upSocket.destroy(); } catch {}
    };

    clientSocket.on("error", closeBoth);
    upSocket.on("error", closeBoth);
    clientSocket.on("close", () => {
      try { upSocket.destroy(); } catch {}
    });
    upSocket.on("close", () => {
      try { clientSocket.destroy(); } catch {}
    });
  });

  upReq.on("response", upRes => {
    let response =
      `HTTP/1.1 ${upRes.statusCode || 502} ` +
      `${upRes.statusMessage || "Bad Gateway"}\r\n`;

    for (const [k, v] of Object.entries(upRes.headers)) {
      if (v == null) continue;
      response += `${k}: ${Array.isArray(v) ? v.join(", ") : v}\r\n`;
    }

    response += "\r\n";
    clientSocket.write(response);
    upRes.pipe(clientSocket);
  });

  upReq.setTimeout(15000, () => upReq.destroy(new Error("WS upstream timeout")));

  upReq.on("error", err => {
    console.error("SDR WS:", err.message);
    try {
      clientSocket.write(
        "HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n"
      );
    } catch {}
    try { clientSocket.destroy(); } catch {}
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
      try { socket.destroy(); } catch {}
      resolve(ok);
    };

    socket.setTimeout(3000);
    socket.on("connect", () => finish(true));
    socket.on("timeout", () => finish(false));
    socket.on("error", () => finish(false));
  });
}

const server = http.createServer((req, res) => {
  let pathname = "/";
  try {
    pathname = new URL(req.url, `http://${req.headers.host || "localhost"}`).pathname;
  } catch {}

  if (pathname === "/player") {
    res.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store"
    });
    res.end(PLAYER_HTML);
    return;
  }

  if (pathname === "/__health") {
    void checkUpstream().then(ok => {
      res.writeHead(ok ? 200 : 503, {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store",
        "access-control-allow-origin": "*"
      });
      res.end(JSON.stringify({
        ok,
        upstream: `${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`
      }));
    });
    return;
  }

  proxyHttp(req, res);
});

server.on("upgrade", proxyUpgrade);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`SDR secure proxy listening on ${PORT}`);
  console.log(`Upstream: ${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`);
});
