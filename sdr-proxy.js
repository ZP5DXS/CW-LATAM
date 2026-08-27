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

      delete headers["x-frame-options"];
      delete headers["content-security-policy"];
      delete headers["content-security-policy-report-only"];

      const contentType=String(upRes.headers["content-type"]||"").toLowerCase();

      // Para HTML de Kiwi necesitamos inyectar el desbloqueo Safari/iOS
      // ANTES de que Kiwi cree su AudioContext.
      if(contentType.includes("text/html")){
        const chunks=[];
        upRes.on("data",c=>chunks.push(c));
        upRes.on("end",()=>{
          let body=Buffer.concat(chunks).toString("utf8");
          body=body.replace(/<head([^>]*)>/i, `<head$1><script>
(function(){
  const ua=navigator.userAgent||"";
  const vendor=navigator.vendor||"";
  const isSafari=/Safari/i.test(ua)&&/Apple Computer/i.test(vendor)&&
    !/Chrome|CriOS|Chromium|Edg|OPR|Firefox|FxiOS/i.test(ua);
  if(!isSafari)return;

  const NativeAC=window.AudioContext||window.webkitAudioContext;
  const contexts=[];

  if(NativeAC){
    function WrappedAudioContext(){
      const ctx=new NativeAC(...arguments);
      contexts.push(ctx);
      window.__CW_AUDIO_CONTEXTS=contexts;
      return ctx;
    }
    WrappedAudioContext.prototype=NativeAC.prototype;
    try{Object.setPrototypeOf(WrappedAudioContext,NativeAC)}catch{}
    try{window.AudioContext=WrappedAudioContext}catch{}
    try{window.webkitAudioContext=WrappedAudioContext}catch{}
  }

  async function unlockAudio(){
    let ok=false;
    const list=window.__CW_AUDIO_CONTEXTS||contexts;
    for(const ctx of list){
      try{
        if(ctx.state==="suspended"||ctx.state==="interrupted")await ctx.resume();
        const b=ctx.createBuffer(1,1,22050);
        const s=ctx.createBufferSource();
        s.buffer=b;
        s.connect(ctx.destination);
        s.start(0);
        if(ctx.state==="running")ok=true;
      }catch{}
    }
    try{
      const a=document.querySelectorAll("audio,video");
      for(const el of a){
        el.muted=false;
        const p=el.play();
        if(p&&p.catch)p.catch(()=>{});
      }
    }catch{}
    const btn=document.getElementById("cwSafariAudioUnlock");
    if(btn){
      btn.textContent=ok?"AUDIO ✓":"START AUDIO";
      if(ok)setTimeout(()=>{btn.style.display="none"},450);
    }
    try{parent.postMessage({source:"cwlatam-kiwi",type:"audio-unlocked",ok},"*")}catch{}
  }

  function addButton(){
    if(document.getElementById("cwSafariAudioUnlock"))return;
    const btn=document.createElement("button");
    btn.id="cwSafariAudioUnlock";
    btn.type="button";
    btn.textContent="START AUDIO";
    btn.setAttribute("aria-label","Start audio");
    Object.assign(btn.style,{
      position:"fixed",left:"0",top:"0",zIndex:"2147483647",
      width:"118px",height:"34px",minWidth:"118px",minHeight:"34px",
      margin:"6px",padding:"0 6px",border:"1px solid #2d7d4a",
      borderRadius:"8px",background:"#0c1c13",color:"#58ff92",
      font:"900 9px system-ui",letterSpacing:".04em",
      cursor:"pointer",boxSizing:"border-box"
    });
    btn.addEventListener("click",unlockAudio,{passive:true});
    (document.body||document.documentElement).appendChild(btn);
    try{parent.postMessage({source:"cwlatam-kiwi",type:"audio-unlock-required"},"*")}catch{}
  }

  async function autoTryResume(){
    const list=window.__CW_AUDIO_CONTEXTS||contexts;
    let running=false;
    for(const ctx of list){
      try{
        if(ctx.state!=="running")await ctx.resume();
        if(ctx.state==="running")running=true;
      }catch{}
    }
    if(running){
      const b=document.getElementById("cwSafariAudioUnlock");
      if(b)b.style.display="none";
      try{parent.postMessage({source:"cwlatam-kiwi",type:"audio-unlocked",ok:true,auto:true},"*")}catch{}
      return true;
    }
    return false;
  }

  if(document.readyState==="loading"){
    document.addEventListener("DOMContentLoaded",()=>{
      setTimeout(async()=>{ if(!(await autoTryResume()))addButton(); },700);
    },{once:true});
  }else{
    setTimeout(async()=>{ if(!(await autoTryResume()))addButton(); },700);
  }

  window.addEventListener("message",e=>{
    const d=e.data||{};
    if(d.source==="cwlatam-parent"&&d.type==="show-audio-unlock"){
      addButton();
      const b=document.getElementById("cwSafariAudioUnlock");
      if(b)b.style.opacity="1";
    }
  });
})();
</script>`);
          const out=Buffer.from(body,"utf8");
          delete headers["content-length"];
          delete headers["content-encoding"];
          headers["content-length"]=String(out.length);
          headers["cache-control"]="no-store";
          res.writeHead(upRes.statusCode||200,headers);
          res.end(out);
        });
        return;
      }

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
