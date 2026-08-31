const net = require("net");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 3000);
const RBN_HOST = "telnet.reversebeacon.net";
const RBN_PORT = 7000;
const RBN_LOGIN = String(process.env.RBN_LOGIN || "ZP5DXS").trim();

/* WHAPI: credenciales exclusivamente en Environment de Render. */
const WHAPI_API_URL = String(process.env.WHAPI_API_URL || "https://gate.whapi.cloud").trim().replace(/\/+$/, "");
const WHAPI_TOKEN = String(process.env.WHAPI_TOKEN || "").trim();
const WHAPI_GROUP_ID = String(process.env.WHAPI_GROUP_ID || "120363429438454894@g.us").trim();
const WHAPI_CHANNEL_ID = String(process.env.WHAPI_CHANNEL_ID || "120363314801098585@newsletter").trim();
const WHATSAPP_ENABLED = String(process.env.WHATSAPP_ENABLED || "true").trim().toLowerCase() === "true";
const CW_LATAM_URL = String(process.env.CW_LATAM_URL || "https://zp5dxs.github.io/CW-LATAM/").trim();

/* Supabase SERVICE ROLE: sólo Render. Nunca frontend/GitHub. */
const SUPABASE_URL = String(process.env.SUPABASE_URL || "").trim().replace(/\/+$/, "");
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();

const OPERATOR_TIME_ZONE = String(process.env.OPERATOR_TIME_ZONE || "America/Asuncion").trim();
/* SDR proxy seguro: navegador HTTPS -> Render HTTPS/WSS -> Kiwi HTTP/WS */
const SDR_UPSTREAM_HOST = String(process.env.SDR_UPSTREAM_HOST || "pardinho.websdr.com.br").trim();
const SDR_UPSTREAM_PORT = Number(process.env.SDR_UPSTREAM_PORT || 8073);
const SDR_PROXY_PREFIX = "/sdr";

const HISTORY_MS = 10 * 60 * 1000;
const ANALYTICS_MEMORY_MS = 36 * 60 * 60 * 1000;
const WHATSAPP_ALERT_COOLDOWN_MS = 10 * 60 * 1000;
const WHATSAPP_AGGREGATION_MS = 5 * 1000;
const RBN_WATCHDOG_MS = 90 * 1000;
const WS_HEARTBEAT_MS = 20 * 1000;
const APP_HEARTBEAT_MS = 15 * 1000;
const SNAPSHOT_MS = 5 * 60 * 1000;
const OPERATOR_TICK_MS = 30 * 1000;

const clients = new Set();
const history = [];
const analyticsSpots = [];
const whatsappAlertedCalls = new Map();
const pendingChannelAlerts = new Map();
const sentEventKeys = new Set();

const serverStartedAt = Date.now();
let rbn = null;
let buffer = "";
let reconnectTimer = null;
let lastRbnDataAt = 0;
let rbnConnected = false;
let spotSequence = 0;
let whatsappQueue = Promise.resolve();
let whatsappLastAlertAt = 0;
let whatsappLastDigestAt = 0;
let whatsappLastError = "";
let whatsappSentAlerts = 0;
let whatsappSentDigests = 0;
let operatorLastEvent = null;
let operatorLastError = "";
let lastSnapshotAt = 0;
let historicalCache = {at:0, rows:[]};
let currentNet = null;
let lastNetSummary = null;
let netReportBaseline = null;
let spaceWeather = {kp:null,sfi:null,updatedAt:0,error:null};

const SA_PREFIXES = [
  "LU","LW","AY","AZ","LO","LP","LQ","LR","LS","LT","LV","CX","ZP",
  "PY","PP","PQ","PR","PS","PT","PU","PV","PW","PX","ZY","ZZ",
  "CE","CA","CB","CC","CD","XQ","OA","OB","CP","HC","HD","YV","YW","YY",
  "HK","HJ","FY","8R","PZ","9Y","9Z","P4","PJ2","PJ4","PJ9","VP8"
];

function cleanCall(call){return String(call||"").toUpperCase().replace(/-#$/,"").trim()}
function isSouthAmericaSpotter(call){const c=cleanCall(call);return SA_PREFIXES.some(p=>c.startsWith(p))}
function roughCountry(call){
  const c=cleanCall(call);
  const rules=[
    [/^ZP/,"PY"],[/^(LU|LW|AY|AZ|LO|LP|LQ|LR|LS|LT|LV)/,"AR"],[/^CX/,"UY"],
    [/^(PY|PP|PQ|PR|PS|PT|PU|PV|PW|PX|ZY|ZZ)/,"BR"],[/^(CE|CA|CB|CC|CD|XQ)/,"CL"],
    [/^(OA|OB)/,"PE"],[/^CP/,"BO"],[/^(HC|HD)/,"EC"],[/^(YV|YW|YY)/,"VE"],
    [/^(HK|HJ)/,"CO"],[/^FY/,"GF"],[/^8R/,"GY"],[/^PZ/,"SR"],[/^(9Y|9Z)/,"TT"],[/^P4/,"AW"]
  ];
  const f=rules.find(([re])=>re.test(c)); return f?f[1]:"DX";
}
function countryLabel(call){
  const m={PY:"🇵🇾 Paraguay",AR:"🇦🇷 Argentina",UY:"🇺🇾 Uruguay",BR:"🇧🇷 Brasil",CL:"🇨🇱 Chile",PE:"🇵🇪 Perú",BO:"🇧🇴 Bolivia",EC:"🇪🇨 Ecuador",VE:"🇻🇪 Venezuela",CO:"🇨🇴 Colombia",GF:"🇬🇫 Guayana Francesa",GY:"🇬🇾 Guyana",SR:"🇸🇷 Surinam",TT:"🇹🇹 Trinidad y Tobago",AW:"🇦🇼 Aruba",DX:"🌎 DX"};
  return m[roughCountry(call)]||"🌎 DX";
}
function median(values){const a=values.filter(Number.isFinite).sort((x,y)=>x-y);if(!a.length)return null;const m=Math.floor(a.length/2);return a.length%2?a[m]:(a[m-1]+a[m])/2}
function avg(values){const a=values.filter(Number.isFinite);return a.length?a.reduce((x,y)=>x+y,0)/a.length:null}
function pctChange(now,prev){if(prev<=0)return now>0?1:0;return (now-prev)/prev}
function clamp(v,min,max){return Math.max(min,Math.min(max,v))}

function localParts(date=new Date()){
  const parts=new Intl.DateTimeFormat("en-CA",{timeZone:OPERATOR_TIME_ZONE,year:"numeric",month:"2-digit",day:"2-digit",weekday:"short",hour:"2-digit",minute:"2-digit",second:"2-digit",hourCycle:"h23"}).formatToParts(date);
  const o={}; for(const p of parts)o[p.type]=p.value;
  const dow={Sun:0,Mon:1,Tue:2,Wed:3,Thu:4,Fri:5,Sat:6}[o.weekday];
  return {year:+o.year,month:+o.month,day:+o.day,hour:+o.hour,minute:+o.minute,second:+o.second,dow,date:`${o.year}-${o.month}-${o.day}`,hm:`${o.hour}:${o.minute}`};
}
function localDateKey(offsetDays=0){
  if(!offsetDays)return localParts().date;
  const d=new Date(Date.now()+offsetDays*86400000);return localParts(d).date;
}
function formatHm(ts){return new Intl.DateTimeFormat("es-PY",{timeZone:OPERATOR_TIME_ZONE,hour:"2-digit",minute:"2-digit",hourCycle:"h23"}).format(new Date(ts))}

function pruneHistory(){const c=Date.now()-HISTORY_MS;while(history.length&&history[0].ts<c)history.shift()}
function pruneAnalytics(){const c=Date.now()-ANALYTICS_MEMORY_MS;while(analyticsSpots.length&&analyticsSpots[0].ts<c)analyticsSpots.shift()}
function rememberSpot(spot){history.push(spot);analyticsSpots.push(spot);pruneHistory();pruneAnalytics()}
function spotsBetween(start,end=Date.now()){return analyticsSpots.filter(s=>s.ts>=start&&s.ts<end)}

function aggregateSpots(list){
  const calls=new Set(),receivers=new Set(),channel=new Set(),countries=new Set(),snrs=[],routes=new Map();
  let bestSnr=null,bestCall=null;
  for(const s of list){
    calls.add(cleanCall(s.dx)); receivers.add(cleanCall(s.spotter)); countries.add(roughCountry(s.dx));
    if(s.isChannel)channel.add(cleanCall(s.dx));
    if(Number.isFinite(Number(s.snr))){const n=Number(s.snr);snrs.push(n);if(bestSnr===null||n>bestSnr){bestSnr=n;bestCall=cleanCall(s.dx)}}
    const route=`${roughCountry(s.spotter)}→${roughCountry(s.dx)}`; routes.set(route,(routes.get(route)||0)+1);
  }
  return {spots:list.length,calls:calls.size,receivers:receivers.size,channelCalls:channel.size,callList:[...calls],receiverList:[...receivers],channelList:[...channel],countries:[...countries].filter(Boolean),medianSnr:median(snrs),avgSnr:avg(snrs),bestSnr,bestCall,routes:[...routes.entries()].sort((a,b)=>b[1]-a[1]).slice(0,8).map(([route,count])=>({route,count}))};
}
function aggregateMinutes(mins,end=Date.now()){return aggregateSpots(spotsBetween(end-mins*60000,end))}

function evaluatePropagation(){
  const now=Date.now(), a15=aggregateMinutes(15,now), p15=aggregateSpots(spotsBetween(now-30*60000,now-15*60000));
  const a30=aggregateMinutes(30,now), p30=aggregateSpots(spotsBetween(now-60*60000,now-30*60000));
  const spotGrowth=pctChange(a15.spots,p15.spots), recGrowth=pctChange(a15.receivers,p15.receivers);
  const snrDelta=(Number.isFinite(a15.medianSnr)&&Number.isFinite(p15.medianSnr))?a15.medianSnr-p15.medianSnr:0;
  const trendScore=clamp(spotGrowth*.45+recGrowth*.35+clamp(snrDelta/8,-1,1)*.20,-1,1);
  let strength=0;
  strength+=clamp(a30.spots/80,0,1)*45; strength+=clamp(a30.receivers/15,0,1)*30;
  if(Number.isFinite(a30.medianSnr))strength+=clamp((a30.medianSnr+5)/25,0,1)*25;
  if(Number.isFinite(spaceWeather.kp)&&spaceWeather.kp>=4)strength-=Math.min(12,(spaceWeather.kp-3)*4);
  let state="DÉBIL"; if(strength>=76)state="MUY BUENA";else if(strength>=58)state="ABIERTA";else if(strength>=38)state="ESTABLE";
  let trend="→ ESTABLE"; if(trendScore>=.28)trend="↗ ABRIENDO";else if(trendScore>=.10)trend="↗ MEJORANDO";else if(trendScore<=-.28)trend="↘ CAYENDO";else if(trendScore<=-.10)trend="↘ BAJANDO";
  const opening=(trendScore>=.22&&a15.spots>=5&&a15.receivers>=2)||(trendScore>=.14&&snrDelta>=3&&a15.spots>=8);
  return {state,trend,trendScore:Number(trendScore.toFixed(3)),opening,strength:Math.round(clamp(strength,0,100)),current:a30,short:a15,previous:p15,snrDelta:Number(snrDelta.toFixed(1)),spotGrowth:Number(spotGrowth.toFixed(2)),receiverGrowth:Number(recGrowth.toFixed(2)),spaceWeather:{kp:spaceWeather.kp,sfi:spaceWeather.sfi,updatedAt:spaceWeather.updatedAt}};
}

function latestNumericField(data,fields){
  if(!Array.isArray(data)||!data.length)return null;
  for(let i=data.length-1;i>=0;i--){for(const f of fields){const n=Number(data[i]?.[f]);if(Number.isFinite(n))return n}}
  return null;
}
async function loadSpaceWeather(){
  try{
    const [kpR,sfiR]=await Promise.allSettled([
      fetch("https://services.swpc.noaa.gov/json/planetary_k_index_1m.json",{cache:"no-store"}).then(r=>r.ok?r.json():Promise.reject(new Error(`Kp ${r.status}`))),
      fetch("https://services.swpc.noaa.gov/json/f107_cm_flux.json",{cache:"no-store"}).then(r=>r.ok?r.json():Promise.reject(new Error(`SFI ${r.status}`)))
    ]);
    if(kpR.status==="fulfilled")spaceWeather.kp=latestNumericField(kpR.value,["kp_index","Kp","kp"]);
    if(sfiR.status==="fulfilled")spaceWeather.sfi=latestNumericField(sfiR.value,["flux","f107","F10.7","observed_flux"]);
    spaceWeather.updatedAt=Date.now(); spaceWeather.error=null;
  }catch(e){spaceWeather.error=e.message}
}
function weatherLine(){
  const x=[]; if(Number.isFinite(spaceWeather.kp))x.push(`Kp ${Number(spaceWeather.kp).toFixed(1).replace(".0","")}`);if(Number.isFinite(spaceWeather.sfi))x.push(`SFI ${Math.round(spaceWeather.sfi)}`);return x.length?`☀️ ${x.join(" · ")}`:"";
}

function whatsappConfigured(){return Boolean(WHATSAPP_ENABLED&&WHAPI_API_URL&&WHAPI_TOKEN&&WHAPI_GROUP_ID)}
function supabaseConfigured(){return Boolean(SUPABASE_URL&&SUPABASE_SERVICE_ROLE_KEY)}
function safeDestinationLabel(){
  if(!WHAPI_GROUP_ID)return"not configured";
  return WHAPI_CHANNEL_ID?"group + channel configured":"group configured";
}
function shouldSendToChannel(eventType){
  return new Set([
    "morning_prediction",
    "midday_summary",
    "opening_prediction",
    "daily_close",
    "net_reminder",
    "net_start",
    "net_mid",
    "net_close",
    "net_sunday"
  ]).has(String(eventType||""));
}
async function whapiSend(to,message){
  if(!whatsappConfigured())return{ok:false,skipped:true,reason:"WHATSAPP_NOT_CONFIGURED"};
  if(!to)return{ok:false,skipped:true,reason:"WHAPI_DESTINATION_NOT_CONFIGURED"};
  const endpoint=`${WHAPI_API_URL}/messages/text`;
  const r=await fetch(endpoint,{method:"POST",headers:{accept:"application/json",authorization:`Bearer ${WHAPI_TOKEN}`,"content-type":"application/json; charset=utf-8"},body:JSON.stringify({to,body:message})});
  const text=await r.text();
  if(!r.ok)throw new Error(`WHAPI HTTP ${r.status}: ${text.slice(0,300)}`);
  whatsappLastError="";
  return{ok:true,response:text};
}
async function whapiSendEvent(message,eventType){
  const groupResult=await whapiSend(WHAPI_GROUP_ID,message);
  let channelResult={ok:false,skipped:true,reason:"CHANNEL_POLICY"};
  if(WHAPI_CHANNEL_ID&&shouldSendToChannel(eventType))channelResult=await whapiSend(WHAPI_CHANNEL_ID,message);
  return{ok:Boolean(groupResult.ok),group:groupResult,channel:channelResult};
}
function enqueueWhatsapp(message,kind,eventType){
  whatsappQueue=whatsappQueue.then(async()=>{const result=await whapiSendEvent(message,eventType);if(result.ok){if(kind==="alert"){whatsappSentAlerts++;whatsappLastAlertAt=Date.now()}else{whatsappSentDigests++;whatsappLastDigestAt=Date.now()}}return result}).catch(e=>{whatsappLastError=e.message;console.error("WhatsApp/Whapi:",e.message);return{ok:false,error:e.message}});return whatsappQueue;
}
async function sbRequest(path,{method="GET",body,headers={}}={}){
  if(!supabaseConfigured())return null;
  const r=await fetch(`${SUPABASE_URL}/rest/v1/${path}`,{method,headers:{apikey:SUPABASE_SERVICE_ROLE_KEY,Authorization:`Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,"content-type":"application/json",...headers},body:body===undefined?undefined:JSON.stringify(body)});
  if(!r.ok){const t=await r.text();throw new Error(`Supabase ${method} ${path} HTTP ${r.status}: ${t.slice(0,350)}`)}
  const t=await r.text();return t?JSON.parse(t):null;
}
async function rpc(name,args={}){return sbRequest(`rpc/${name}`,{method:"POST",body:args})}

async function publishOperatorEvent(eventKey,eventType,title,message,metadata={},toWhatsapp=true){
  if(sentEventKeys.has(eventKey))return false;
  // DB unique key protects against Render restarts / duplicate scheduler ticks.
  let inserted=true;
  if(supabaseConfigured()){
    try{
      const rows=await sbRequest("cw_operator_events?on_conflict=event_key",{method:"POST",body:{event_key:eventKey,event_type:eventType,title,message,metadata},headers:{Prefer:"resolution=ignore-duplicates,return=representation"}});
      inserted=Array.isArray(rows)?rows.length>0:true;
      if(Array.isArray(rows)&&rows.length===0)inserted=false;
    }catch(e){operatorLastError=e.message;console.error("Operator DB:",e.message)}
  }
  if(!inserted){sentEventKeys.add(eventKey);return false}
  sentEventKeys.add(eventKey);operatorLastEvent={eventKey,eventType,title,message,ts:Date.now(),metadata};
  broadcast({type:"operator_event",event:operatorLastEvent});
  if(toWhatsapp)await enqueueWhatsapp(message,eventType==="channel_alert"?"alert":"digest",eventType);
  return true;
}

function formatChannelAlert(alert){
  const best=Number.isFinite(Number(alert.bestSnr))?`${Number(alert.bestSnr)>=0?"+":""}${alert.bestSnr} dB`:"SNR —";
  const rec=alert.spotters.size===1?"1 receptor lo escucha":`${alert.spotters.size} receptores lo escuchan`;
  return ["🚨 *CQ EN 7.033 · CW LATAM*","",`📡 *${alert.dx}* llamando CQ`,`🌎 ${countryLabel(alert.dx)}`,`👂 *${rec}*`,`⚡ ${alert.wpm} WPM · señal máx. ${best}`,"",`🔗 ${CW_LATAM_URL}`].join("\n");
}
function flushChannelWhatsapp(call){
  const alert=pendingChannelAlerts.get(call);if(!alert)return;pendingChannelAlerts.delete(call);whatsappAlertedCalls.set(call,Date.now());console.log(`WHATSAPP 7033 -> ${call} · ${alert.spotters.size} receptores`);
  const bucket=Math.floor(Date.now()/WHATSAPP_ALERT_COOLDOWN_MS);
  void publishOperatorEvent(`7033:${call}:${bucket}`,"channel_alert","CW BOT",formatChannelAlert(alert),{callsign:call,receivers:alert.spotters.size,best_snr:alert.bestSnr,wpm:alert.wpm},true);
}
function maybeSendChannelWhatsapp(spot){
  if(!spot||!spot.isChannel)return;const call=cleanCall(spot.dx),last=Number(whatsappAlertedCalls.get(call)||0);if(Date.now()-last<WHATSAPP_ALERT_COOLDOWN_MS)return;
  let alert=pendingChannelAlerts.get(call);if(!alert){alert={dx:call,wpm:Number(spot.wpm)||0,bestSnr:Number.isFinite(Number(spot.snr))?Number(spot.snr):null,spotters:new Set(),timer:null};alert.timer=setTimeout(()=>flushChannelWhatsapp(call),WHATSAPP_AGGREGATION_MS);pendingChannelAlerts.set(call,alert)}
  alert.spotters.add(cleanCall(spot.spotter));if(Number.isFinite(Number(spot.snr))&&(alert.bestSnr===null||Number(spot.snr)>alert.bestSnr))alert.bestSnr=Number(spot.snr);if(Number(spot.wpm)>0)alert.wpm=Number(spot.wpm);
}

function rangeFromLocalHours(startHour,endHour,dateOffset=0){
  // Convertimos mediante búsqueda de los límites locales: robusto ante DST sin librerías.
  const target=localDateKey(dateOffset);const now=Date.now();
  function find(hour){for(let d=-36*60;d<=36*60;d++){const ts=now+d*60000,p=localParts(new Date(ts));if(p.date===target&&p.hour===hour&&p.minute===0)return ts}return null}
  let start=find(startHour),end=endHour===24?find(0):find(endHour);
  if(endHour===24){const next=localDateKey(dateOffset+1);for(let d=-36*60;d<=48*60;d++){const ts=now+d*60000,p=localParts(new Date(ts));if(p.date===next&&p.hour===0&&p.minute===0){end=ts;break}}}
  return {start:start||now-6*3600000,end:end||now};
}

async function getPublicReportCount(){
  try{const data=await rpc("cw_get_public_stats",{});const x=Array.isArray(data)?data[0]:data;return Number(x?.votes||x?.reports||0)}catch{return null}
}
async function saveSnapshot(){
  const bucketEnd=Math.floor(Date.now()/SNAPSHOT_MS)*SNAPSHOT_MS;
  const bucketStart=bucketEnd-SNAPSHOT_MS;
  if(serverStartedAt>bucketStart)return;
  const a=aggregateSpots(spotsBetween(bucketStart,bucketEnd));
  const p=localParts(new Date(bucketStart)),ev=evaluatePropagation();
  lastSnapshotAt=Date.now();
  if(!supabaseConfigured())return;
  try{await sbRequest("cw_operator_snapshots?on_conflict=bucket_start",{method:"POST",body:{bucket_start:new Date(bucketStart).toISOString(),observed_at:new Date(bucketEnd).toISOString(),local_date:p.date,local_hour:p.hour,window_minutes:5,spots:a.spots,calls:a.calls,receivers:a.receivers,channel_calls:a.channelCalls,call_list:a.callList,receiver_list:a.receiverList,channel_list:a.channelList,median_snr:a.medianSnr,avg_snr:a.avgSnr,countries:a.countries,routes:a.routes,band_state:ev.state,trend:ev.trend,trend_score:ev.trendScore,metadata:{strength:ev.strength,spot_growth:ev.spotGrowth,receiver_growth:ev.receiverGrowth,snr_delta:ev.snrDelta}},headers:{Prefer:"resolution=ignore-duplicates,return=minimal"}})}catch(e){operatorLastError=e.message;console.error("snapshot:",e.message)}
}
async function aggregateStoredPeriod(start,end){
  if(!supabaseConfigured())return null;
  try{
    const rows=await sbRequest(`cw_operator_snapshots?select=bucket_start,spots,call_list,receiver_list,channel_list,median_snr,avg_snr,countries,routes&bucket_start=gte.${encodeURIComponent(new Date(start).toISOString())}&bucket_start=lt.${encodeURIComponent(new Date(end).toISOString())}&order=bucket_start.asc&limit=5000`)||[];
    if(!rows.length)return null;
    const calls=new Set(),receivers=new Set(),channel=new Set(),countries=new Set(),snrs=[],routes=new Map();let spots=0;
    for(const r of rows){spots+=Number(r.spots||0);for(const x of r.call_list||[])calls.add(x);for(const x of r.receiver_list||[])receivers.add(x);for(const x of r.channel_list||[])channel.add(x);for(const x of r.countries||[])countries.add(x);if(Number.isFinite(Number(r.median_snr)))snrs.push(Number(r.median_snr));for(const x of r.routes||[])routes.set(x.route,(routes.get(x.route)||0)+Number(x.count||0))}
    return {spots,calls:calls.size,receivers:receivers.size,channelCalls:channel.size,callList:[...calls],receiverList:[...receivers],channelList:[...channel],countries:[...countries],medianSnr:median(snrs),avgSnr:avg(snrs),bestSnr:null,bestCall:null,routes:[...routes.entries()].sort((a,b)=>b[1]-a[1]).slice(0,8).map(([route,count])=>({route,count})),storedBuckets:rows.length};
  }catch(e){operatorLastError=e.message;return null}
}
async function periodAggregate(start,end){
  const stored=await aggregateStoredPeriod(start,end);
  const expected=Math.max(1,Math.floor((end-start)/SNAPSHOT_MS));
  if(stored&&stored.storedBuckets>=Math.max(2,expected*.55))return stored;
  return aggregateSpots(spotsBetween(start,end));
}

async function storedRowsBetween(start,end){
  if(!supabaseConfigured())return[];
  try{return await sbRequest(`cw_operator_snapshots?select=bucket_start,spots,calls,receivers,median_snr,band_state,trend,trend_score,routes&bucket_start=gte.${encodeURIComponent(new Date(start).toISOString())}&bucket_start=lt.${encodeURIComponent(new Date(end).toISOString())}&order=bucket_start.asc&limit=5000`)||[]}catch{return[]}
}
async function observedPeakWindow(start,end){
  const rows=await storedRowsBetween(start,end);
  if(!rows.length)return null;
  let best=null;
  for(const r of rows){const score=Number(r.spots||0)+Number(r.receivers||0)*3+(Number(r.median_snr||0)+10)*.3;if(!best||score>best.score)best={score,ts:new Date(r.bucket_start).getTime()}}
  if(!best)return null;const center=best.ts+2.5*60000;return `${formatHm(center-30*60000)}–${formatHm(center+30*60000)}`;
}
async function lastOpenTime(start,end){
  const rows=await storedRowsBetween(start,end);const open=rows.filter(r=>["ABIERTA","MUY BUENA"].includes(String(r.band_state||""))||Number(r.spots||0)>=8).pop();return open?formatHm(new Date(open.bucket_start).getTime()+5*60000):null;
}

async function historicalSnapshots(){
  if(!supabaseConfigured())return[];if(Date.now()-historicalCache.at<15*60000)return historicalCache.rows;
  const since=new Date(Date.now()-35*86400000).toISOString();
  try{const rows=await sbRequest(`cw_operator_snapshots?select=observed_at,local_hour,spots,calls,receivers,median_snr,band_state,trend,trend_score,routes&observed_at=gte.${encodeURIComponent(since)}&order=observed_at.asc&limit=10000`)||[];historicalCache={at:Date.now(),rows};return rows}catch(e){operatorLastError=e.message;return[]}
}
async function bestWindow(period="morning"){
  const rows=await historicalSnapshots();const [a,b]=period==="morning"?[6,12]:[17,24];
  const by=new Map();for(const r of rows){const h=Number(r.local_hour);if(h<a||h>=b)continue;const x=by.get(h)||{n:0,score:0};x.n++;x.score+=Number(r.spots||0)+Number(r.receivers||0)*3;by.set(h,x)}
  const ranked=[...by.entries()].filter(([,x])=>x.n>=2).map(([h,x])=>({h,score:x.score/x.n,n:x.n})).sort((x,y)=>y.score-x.score);
  if(!ranked.length)return period==="morning"?{text:"08:00–11:00",historical:false}:{text:"19:00–23:00",historical:false};
  const h=ranked[0].h;return{text:`${String(h).padStart(2,"0")}:00–${String(Math.min(24,h+2)).padStart(2,"0")}:00`,historical:true};
}
function topRoutes(a){return (a.routes||[]).slice(0,4).map(x=>x.route.replace("→"," ↔ ")).join(" · ")||"muestra aún limitada"}
function bandLine(ev){return `${ev.state}${ev.trend.includes("ESTABLE")?"":` · ${ev.trend.replace(/[↗↘→]\s*/,"")}`}`}

async function sendTwoHourDigest(hour){
  const end=Date.now(),a=await periodAggregate(end-120*60000,end),ev=evaluatePropagation();if(a.spots<=0)return;
  const msg=[`📊 *CW LATAM · ${String((hour+22)%24).padStart(2,"0")}:00–${String(hour).padStart(2,"0")}:00*`,"",`📡 ${a.calls} estaciones activas`,`📶 ${a.spots} spots válidos`,`🚨 ${a.channelCalls} llamadas en 7.033`,`👂 ${a.receivers} receptores`,`🌎 ${a.countries.length} países / regiones`,"",`${ev.trend.startsWith("↗")?"🟢":ev.trend.startsWith("↘")?"🟠":"🟡"} *40 m · ${bandLine(ev)}*`,``, `🔗 ${CW_LATAM_URL}`].join("\n");
  await publishOperatorEvent(`digest2h:${localParts().date}:${hour}`,"digest","CW BOT",msg,{aggregate:a,propagation:ev},true);
}
async function send0600(){
  const range=rangeFromLocalHours(0,6),a=await periodAggregate(range.start,range.end),ev=evaluatePropagation(),win=await bestWindow("morning");
  const liveNight=spotsBetween(range.start,range.end);const openUntil=await lastOpenTime(range.start,range.end);const nightLast=openUntil||(liveNight.length?formatHm(Math.max(...liveNight.map(s=>s.ts))):"—");
  const msg=["🌅 *CW LATAM · BUEN DÍA*","","🌙 *Así estuvo la noche*",`📡 ${a.calls} estaciones detectadas`,`📶 ${a.spots} spots válidos`,`👂 ${a.receivers} receptores`,`🚨 ${a.channelCalls} estaciones llamaron CQ en *7.033*`,"",a.spots?`40 m se mantuvo utilizable/activa hasta aproximadamente las *${nightLast}*.`:"La madrugada tuvo actividad muy baja en la muestra del radar.","","☀️ *¿Qué esperamos esta mañana?*",`🟢 Condición prevista: *${bandLine(ev)}*`,`📈 Apertura estimada: *07:00–12:00*`,`⭐ Mejor ventana: *${win.text}*${win.historical?" · histórico CW LATAM":" · estimación inicial"}`,weatherLine(),"",`🌎 Rutas con mayor actividad/probabilidad: ${topRoutes(a)}`,"","🧠 Predicción experimental basada en RBN, tendencia actual e histórico CW LATAM.","","☕ Buenos días y buenos DX. 73",`🔗 ${CW_LATAM_URL}`].join("\n");
  await publishOperatorEvent(`morning:${localParts().date}`,"morning_prediction","CW BOT",msg,{night:a,propagation:ev,best_window:win},true);
}
async function send1200(){
  const r=rangeFromLocalHours(6,12),a=await periodAggregate(r.start,r.end),ev=evaluatePropagation(),peak=await observedPeakWindow(r.start,r.end),p=localParts();
  const msg=["☀️ *CW LATAM · ASÍ ESTUVO LA MAÑANA*","","📡 *06:00–12:00*",`${a.calls} estaciones detectadas`,`📶 ${a.spots} spots válidos`,`👂 ${a.receivers} receptores`,`🚨 ${a.channelCalls} estaciones llamaron CQ en 7.033`,"",`📈 *40 m · ${bandLine(ev)}*`,peak?`⭐ Mejor período observado: *${peak}*`:"",Number.isFinite(a.medianSnr)?`⚡ SNR mediano: *${a.medianSnr.toFixed(1)} dB*`:"⚡ SNR mediano: —",`🌎 Rutas más activas: ${topRoutes(a)}`,p.dow===0?"":"",p.dow===0?"📻 *HOY 18:00 · LXCW NET CONTROL · 7.033 MHz*":"","","🧠 Esta mañana ya queda incorporada al histórico para calibrar próximas estimaciones.",`🔗 ${CW_LATAM_URL}`].filter(Boolean).join("\n");
  await publishOperatorEvent(`midday:${p.date}`,"midday_summary","CW BOT",msg,{aggregate:a,propagation:ev,peak_window:peak},true);
}
async function sendEveningOpening(forced=false){
  const ev=evaluatePropagation(),win=await bestWindow("evening"),a=aggregateMinutes(60);const p=localParts();
  if(!forced&&!ev.opening)return false;
  const lead=ev.opening?"La actividad regional está aumentando y *40 m muestra señales claras de apertura*.":"Entramos en la ventana vespertina de 40 m; la tendencia todavía es moderada y el radar continúa siguiendo la apertura.";
  const msg=["📡 *CW LATAM · 40 M ESTÁ DESPERTANDO*","",`🌆 ${lead}`,"","🔮 *PREDICCIÓN TARDE / NOCHE*",`${ev.opening?"🟢":"🟡"} Condición esperada: *${bandLine(ev)}*`,`⭐ Mejor ventana estimada: *${win.text}*${win.historical?" · histórico CW LATAM":""}`,weatherLine(),"",`📊 Última hora: ${a.calls} estaciones · ${a.receivers} receptores${Number.isFinite(a.medianSnr)?` · SNR mediano ${a.medianSnr.toFixed(1)} dB`:""}`,`🌎 Rutas activas: ${topRoutes(a)}`,"","🧠 El aviso se dispara por tendencia de actividad, receptores y señal; no sólo por la hora.",localParts().dow===0?"📻 *Hoy 18:00 · LXCW NET CONTROL en 7.033 MHz*":"","","🎧 Buen momento para empezar a escuchar 40 m.",`🔗 ${CW_LATAM_URL}`].join("\n");
  const ok=await publishOperatorEvent(`evening_open:${p.date}`,"opening_prediction","CW BOT",msg,{forced,propagation:ev,aggregate:a,best_window:win},true);return ok;
}
async function send0000(){
  const r=rangeFromLocalHours(6,24,-1),a=await periodAggregate(r.start,r.end),ev=evaluatePropagation(),peak=await observedPeakWindow(r.start,r.end);
  const msg=["🌙 *CW LATAM · CIERRE DEL DÍA*","","📡 *Actividad 06:00–00:00*",`${a.calls} estaciones detectadas`,`📶 ${a.spots} spots válidos`,`👂 ${a.receivers} receptores`,`🚨 ${a.channelCalls} estaciones llamaron CQ en 7.033`,`🌎 ${a.countries.length} países / regiones`,"",`📈 *40 m · ${bandLine(ev)}*`,peak?`⭐ Mejor período: *${peak}*`:"",Number.isFinite(a.medianSnr)?`⚡ SNR mediano: *${a.medianSnr.toFixed(1)} dB*`:"⚡ SNR mediano: —",a.bestCall?`🏆 Mejor señal observada: *${a.bestCall} · ${a.bestSnr>=0?"+":""}${a.bestSnr} dB*`:"","","🧠 Los datos del día ya forman parte del histórico CW LATAM.","","📡 *El radar no duerme.*","CW LATAM continuará monitoreando 40 m durante toda la madrugada.","🌅 A las *06:00* llega el resumen nocturno y la predicción de la mañana.","","73 👋",`🔗 ${CW_LATAM_URL}`].filter(Boolean).join("\n");
  await publishOperatorEvent(`closing:${localDateKey(-1)}`,"daily_close","CW BOT",msg,{aggregate:a,propagation:ev,peak_window:peak},true);
}
async function ensureNet(){
  const p=localParts(); if(p.dow!==0||p.hour<17||p.hour>20)return null;
  if(currentNet&&currentNet.date===p.date)return currentNet;
  currentNet={date:p.date,id:null,status:"scheduled",participants:new Map(),startedAt:null,reportsStart:null};
  if(supabaseConfigured()){
    try{
      let rows=await sbRequest(`cw_nets?select=*&net_date=eq.${p.date}&limit=1`);
      let row=rows?.[0]||null;
      if(!row){rows=await sbRequest("cw_nets?on_conflict=net_date",{method:"POST",body:{net_date:p.date,name:"LXCW NET CONTROL",frequency_khz:7033,status:"scheduled"},headers:{Prefer:"resolution=ignore-duplicates,return=representation"}});row=rows?.[0]||null}
      if(row){
        currentNet.id=row.id;currentNet.status=row.status||"scheduled";currentNet.startedAt=row.starts_at?new Date(row.starts_at).getTime():null;currentNet.reportsStart=Number.isFinite(Number(row.reports_start))?Number(row.reports_start):null;
        const participants=await sbRequest(`cw_net_participants?select=*&net_id=eq.${row.id}&order=first_seen.asc&limit=500`)||[];
        for(const q of participants){currentNet.participants.set(cleanCall(q.callsign),{call:cleanCall(q.callsign),country:q.country_code||roughCountry(q.callsign),firstSeen:new Date(q.first_seen).getTime(),lastSeen:new Date(q.last_seen).getTime(),spots:Number(q.spots||0),receivers:new Set(Array.isArray(q.receiver_calls)?q.receiver_calls:[]),maxSnr:q.max_snr===null?null:Number(q.max_snr),wpm:Number(q.wpm||0)})}
      }
    }catch(e){operatorLastError=e.message}
  }
  return currentNet;
}
async function netStart(){
  const n=await ensureNet();if(!n)return;n.status="live";if(!n.startedAt)n.startedAt=Date.now();if(n.reportsStart===null)n.reportsStart=await getPublicReportCount();netReportBaseline=n.reportsStart;
  if(n.id)try{await sbRequest(`cw_nets?id=eq.${n.id}`,{method:"PATCH",body:{status:"live",starts_at:new Date(n.startedAt).toISOString(),reports_start:n.reportsStart}})}catch{}
  const msg=["🔴 *LXCW NET CONTROL · EN EL AIRE*","","📡 *7.033 MHz · CW QRS*","🕕 18:00–20:00","","🤖 NET CONTROL automático activo.","Llamá CQ normalmente en 7.033. El radar irá registrando los check-ins detectados por RBN.","","🔗 "+CW_LATAM_URL].join("\n");
  await publishOperatorEvent(`net:start:${n.date}`,"net_start","LXCW NET CONTROL",msg,{net_date:n.date},true);
  broadcastOperatorState();
}
async function netPre(minutes){const p=localParts(),msg=minutes===30?["📻 *LXCW NET CONTROL · HOY*","","🕕 18:00–20:00","📡 *7.033 MHz · CW QRS*","","En 30 minutos comienza el NET automático semanal.","Llamá CQ normalmente y el radar registrará las estaciones detectadas.","","🔗 "+CW_LATAM_URL].join("\n"):["⏱ *LXCW NET CONTROL · 5 MINUTOS*","","📡 7.033 MHz · CW QRS","En cinco minutos abrimos el NET semanal.","","🔗 "+CW_LATAM_URL].join("\n");await publishOperatorEvent(`net:pre${minutes}:${p.date}`,"net_reminder","LXCW NET CONTROL",msg,{minutes},true)}
async function recordNetSpot(spot){
  const p=localParts();if(p.dow!==0||![18,19].includes(p.hour)||!spot.isChannel)return;const n=await ensureNet();if(!n)return;if(n.status!=="live")await netStart();
  const c=cleanCall(spot.dx);let x=n.participants.get(c);const isNew=!x;if(!x){x={call:c,country:roughCountry(c),firstSeen:Date.now(),lastSeen:Date.now(),spots:0,receivers:new Set(),maxSnr:null,wpm:Number(spot.wpm)||0};n.participants.set(c,x)}
  x.lastSeen=Date.now();x.spots++;x.receivers.add(cleanCall(spot.spotter));if(Number.isFinite(Number(spot.snr))&&(x.maxSnr===null||Number(spot.snr)>x.maxSnr))x.maxSnr=Number(spot.snr);if(Number(spot.wpm)>0)x.wpm=Number(spot.wpm);
  if(n.id&&supabaseConfigured())try{await sbRequest("cw_net_participants?on_conflict=net_id,callsign",{method:"POST",body:{net_id:n.id,callsign:c,country_code:x.country,first_seen:new Date(x.firstSeen).toISOString(),last_seen:new Date(x.lastSeen).toISOString(),spots:x.spots,receiver_count:x.receivers.size,receiver_calls:[...x.receivers],max_snr:x.maxSnr,wpm:x.wpm},headers:{Prefer:"resolution=merge-duplicates,return=minimal"}})}catch(e){operatorLastError=e.message}
  if(isNew){
    const pos=n.participants.size;
    const msg=`📡 ${c} ${countryLabel(c)} · CHECK-IN #${pos} · ${x.receivers.size} receptor${x.receivers.size===1?"":"es"}${x.maxSnr!==null?` · ${x.maxSnr>=0?"+":""}${x.maxSnr} dB`:""}`;
    void publishOperatorEvent(`net:checkin:${n.date}:${c}`,"net_checkin","LXCW NET CONTROL",msg,{callsign:c,position:pos,receivers:x.receivers.size,max_snr:x.maxSnr},false);
  }
  broadcastOperatorState();
}
function netMetrics(){
  const n=currentNet;if(!n)return{participants:0,countries:0,receivers:0,bestCall:null,bestSnr:null,widestCall:null,widestReceivers:0};const countries=new Set(),receivers=new Set();let bestCall=null,bestSnr=null,widestCall=null,widestReceivers=0;
  for(const x of n.participants.values()){countries.add(x.country);for(const r of x.receivers)receivers.add(r);if(x.maxSnr!==null&&(bestSnr===null||x.maxSnr>bestSnr)){bestSnr=x.maxSnr;bestCall=x.call}if(x.receivers.size>widestReceivers){widestReceivers=x.receivers.size;widestCall=x.call}}
  return{participants:n.participants.size,countries:countries.size,receivers:receivers.size,bestCall,bestSnr,widestCall,widestReceivers};
}
async function netMid(){const n=await ensureNet();if(!n)return;const m=netMetrics(),ev=evaluatePropagation();const msg=["📻 *LXCW NET CONTROL · 19:00*","",`✅ *${m.participants} estaciones detectadas*`,`🌎 ${m.countries} países`,`👂 ${m.receivers} receptores`,"",`📈 40 m · *${bandLine(ev)}*`,"","Todavía estás a tiempo de sumarte en *7.033 MHz*.",`🔗 ${CW_LATAM_URL}`].join("\n");await publishOperatorEvent(`net:mid:${n.date}`,"net_mid","LXCW NET CONTROL",msg,{metrics:m,propagation:ev},true)}
async function netClose(){
  const n=await ensureNet();if(!n)return;const m=netMetrics(),reportsNow=await getPublicReportCount(),reports=(reportsNow!==null&&n.reportsStart!==null)?Math.max(0,reportsNow-n.reportsStart):0;n.status="closed";const msg=["🏁 *LXCW NET CONTROL · FINALIZADO*","","📡 7.033 MHz · 18:00–20:00",`✅ *${m.participants} estaciones detectadas*`,`🌎 *${m.countries} países*`,`👂 *${m.receivers} receptores*`,reports?`📊 *${reports} reportes comunitarios durante el NET*`:"",m.bestCall?`⚡ Mejor señal: *${m.bestCall} · ${m.bestSnr>=0?"+":""}${m.bestSnr} dB*`:"",m.widestCall?`🌎 Mayor cobertura: *${m.widestCall} · ${m.widestReceivers} receptores*`:"","","🧠 El resultado queda guardado para mejorar las futuras estimaciones de CW LATAM.","","Gracias a todos. *73* 📻",`🔗 ${CW_LATAM_URL}`].filter(Boolean).join("\n");
  lastNetSummary={date:n.date,...m,reports};if(n.id)try{await sbRequest(`cw_nets?id=eq.${n.id}`,{method:"PATCH",body:{status:"closed",ends_at:new Date().toISOString(),participants:m.participants,countries:m.countries,receivers:m.receivers,reports,best_call:m.bestCall,best_snr:m.bestSnr,widest_call:m.widestCall,widest_receivers:m.widestReceivers,summary:lastNetSummary,updated_at:new Date().toISOString()}})}catch(e){operatorLastError=e.message}
  await publishOperatorEvent(`net:close:${n.date}`,"net_close","LXCW NET CONTROL",msg,{metrics:m,reports},true);broadcastOperatorState();
}
async function mondayNetSummary(){
  const yesterday=localDateKey(-1);let s=lastNetSummary;
  if(supabaseConfigured())try{const rows=await sbRequest(`cw_nets?select=*&net_date=eq.${yesterday}&limit=1`);if(rows?.length)s={date:rows[0].net_date,participants:rows[0].participants,countries:rows[0].countries,receivers:rows[0].receivers,reports:rows[0].reports,bestCall:rows[0].best_call,bestSnr:rows[0].best_snr,widestCall:rows[0].widest_call,widestReceivers:rows[0].widest_receivers}}catch{}
  if(!s)return;
  const msg=["☕ *CW LATAM · RESUMEN DEL NET*","","📻 *LXCW NET CONTROL · DOMINGO*",`✅ *${s.participants} estaciones participantes*`,`🌎 *${s.countries} países*`,`👂 *${s.receivers} receptores*`,s.reports?`📊 *${s.reports} reportes comunitarios*`:"",s.widestCall?`🏆 Mayor cobertura: *${s.widestCall} · ${s.widestReceivers} receptores*`:"",s.bestCall?`⚡ Mejor señal: *${s.bestCall} · ${s.bestSnr>=0?"+":""}${s.bestSnr} dB*`:"","","📡 Lista completa y resultados disponibles hoy en CW LATAM.","73 📻",`🔗 ${CW_LATAM_URL}`].filter(Boolean).join("\n");
  await publishOperatorEvent(`net:monday:${yesterday}`,"net_sunday","LXCW NET CONTROL",msg,{summary:s},true);
}

function broadcast(obj){const data=JSON.stringify(obj);for(const ws of clients)if(ws.readyState===1)try{ws.send(data)}catch{}}
function operatorState(){const ev=evaluatePropagation(),p=localParts(),m=netMetrics();return{type:"operator",ts:Date.now(),timeZone:OPERATOR_TIME_ZONE,local:p,propagation:ev,next:nextOperatorAction(p),net:{active:Boolean(currentNet&&currentNet.status==="live"),date:currentNet?.date||null,status:currentNet?.status||"idle",...m},lastEvent:operatorLastEvent,lastError:operatorLastError||null}}
function broadcastOperatorState(){broadcast(operatorState())}
function nextOperatorAction(p=localParts()){
  const h=p.hour,m=p.minute,d=p.dow;let actions=[];
  for(const x of [{h:6,m:0,n:"Resumen nocturno + predicción"},{h:8,m:0,n:"Digest 2 h"},{h:10,m:0,n:"Digest 2 h"},{h:12,m:0,n:"Resumen mañana"},{h:14,m:0,n:"Digest 2 h"},{h:16,m:0,n:"Digest 2 h"},{h:18,m:45,n:"Predicción vespertina"},{h:20,m:0,n:"Digest 2 h"},{h:22,m:0,n:"Digest 2 h"},{h:24,m:0,n:"Cierre diario"}]){let mins=(x.h===24?1440:x.h*60+x.m)-(h*60+m);if(mins<=0)mins+=1440;actions.push({...x,mins})}
  if(d===0)for(const x of [{h:17,m:30,n:"LXCW NET · aviso 30 min"},{h:17,m:55,n:"LXCW NET · aviso 5 min"},{h:18,m:0,n:"LXCW NET CONTROL"},{h:19,m:0,n:"LXCW NET · parcial"},{h:20,m:0,n:"LXCW NET · cierre"}]){let mins=x.h*60+x.m-(h*60+m);if(mins<=0)mins+=7*1440;actions.push({...x,mins})}
  return actions.sort((a,b)=>a.mins-b.mins)[0];
}

async function operatorTick(){
  const p=localParts();
  try{
    if(Date.now()-lastSnapshotAt>=SNAPSHOT_MS)void saveSnapshot();
    if(p.minute===0){if(p.hour===6)await send0600();else if([8,10,14,16,20,22].includes(p.hour)&&!(p.dow===0&&p.hour===20))await sendTwoHourDigest(p.hour);else if(p.hour===12)await send1200();else if(p.hour===0)await send0000()}
    // Proactividad 17:00–19:30. Primera tendencia convincente gana; fallback 18:45.
    if(!(p.dow===0&&p.hour>=18&&p.hour<20)&&(p.hour===17||p.hour===18||(p.hour===19&&p.minute<=30))&&p.minute%5===0){const key=`evening_open:${p.date}`;if(!sentEventKeys.has(key)){const ev=evaluatePropagation();if(ev.opening)await sendEveningOpening(false);else if(p.hour===18&&p.minute>=45)await sendEveningOpening(true)}}
    if(p.dow===0){if(p.hour===17&&p.minute===30)await netPre(30);if(p.hour===17&&p.minute===55)await netPre(5);if(p.hour===18&&p.minute===0)await netStart();if(p.hour===19&&p.minute===0)await netMid();if(p.hour===20&&p.minute===0)await netClose()}
    if(p.dow===1&&p.hour===9&&p.minute===0)await mondayNetSummary();
  }catch(e){operatorLastError=e.message;console.error("operatorTick:",e)}
  broadcastOperatorState();
}

function parseRbnLine(raw){
  const line=String(raw||"").replace(/\r/g,"").trim();const match=line.match(/^DX de\s+([^:]+):\s+([0-9.]+)\s+(\S+)\s+(CW)\s+(-?\d+)\s+dB\s+(\d+)\s+WPM\s+(.+?)\s+([0-2]\d[0-5]\d)Z$/i);if(!match)return null;
  const spotter=cleanCall(match[1]),actualFreq=Number(match[2]),dx=cleanCall(match[3]),mode=match[4].toUpperCase(),snr=Number(match[5]),wpm=Number(match[6]),activity=match[7].trim().toUpperCase(),rbnUtc=match[8];
  if(mode!=="CW"||activity!=="CQ"||actualFreq<7000||actualFreq>7300||!isSouthAmericaSpotter(spotter))return null;
  const isChannel=actualFreq>=7032.9&&actualFreq<=7033.1;spotSequence++;return{seq:spotSequence,source:"RBN",ts:Date.now(),spotter,dx,freq:isChannel?7033:actualFreq,actualFreq,isChannel,snr,wpm,type:"CQ",mode:"CW",rbnUtc};
}
function scheduleRbnReconnect(){clearTimeout(reconnectTimer);reconnectTimer=setTimeout(connectRbn,3000)}
function connectRbn(){
  clearTimeout(reconnectTimer);buffer="";rbnConnected=false;console.log(`Conectando ${RBN_HOST}:${RBN_PORT} como ${RBN_LOGIN}...`);rbn=net.createConnection({host:RBN_HOST,port:RBN_PORT});rbn.setNoDelay(true);rbn.setKeepAlive(true,20000);
  rbn.on("connect",()=>{rbnConnected=true;lastRbnDataAt=Date.now();console.log("RBN TCP conectado");setTimeout(()=>{if(rbn&&!rbn.destroyed)rbn.write(RBN_LOGIN+"\r\n")},700)});
  rbn.on("data",chunk=>{lastRbnDataAt=Date.now();buffer+=chunk.toString("utf8");const lines=buffer.split(/\n/);buffer=lines.pop()||"";for(const line of lines){const spot=parseRbnLine(line);if(!spot)continue;rememberSpot(spot);broadcast(spot);maybeSendChannelWhatsapp(spot);void recordNetSpot(spot);const mark=spot.isChannel?" *** 7033 ***":"";console.log(`LIVE #${spot.seq} ${spot.spotter} -> ${spot.dx} ${spot.actualFreq.toFixed(2)} ${spot.snr} dB ${spot.wpm} WPM${mark}`)}});
  rbn.on("error",e=>console.error("RBN error:",e.message));rbn.on("close",()=>{console.log("RBN desconectado");rbnConnected=false;scheduleRbnReconnect()});
}
setInterval(()=>{if(!rbn||rbn.destroyed||!rbnConnected)return;if(Date.now()-lastRbnDataAt>RBN_WATCHDOG_MS){console.warn("RBN sin datos 90 s. Reconectando...");try{rbn.destroy()}catch{}}},15000);
setInterval(()=>{const now=Date.now();for(const [call,ts] of whatsappAlertedCalls)if(now-ts>WHATSAPP_ALERT_COOLDOWN_MS*2)whatsappAlertedCalls.delete(call)},5*60000);


function isSdrProxyPath(pathname){
  return pathname===SDR_PROXY_PREFIX ||
    pathname.startsWith(SDR_PROXY_PREFIX+"/") ||
    pathname.startsWith("/kiwi/");
}
function sdrUpstreamPath(reqUrl){
  const u=new URL(reqUrl,"http://localhost");
  if(u.pathname===SDR_PROXY_PREFIX)return "/"+u.search;
  if(u.pathname.startsWith(SDR_PROXY_PREFIX+"/")){
    return (u.pathname.slice(SDR_PROXY_PREFIX.length)||"/")+u.search;
  }
  // Kiwi crea WebSockets absolutos /kiwi/<timestamp>/{SND,W/F,EXT}
  return u.pathname+u.search;
}
function cleanProxyRequestHeaders(headers){
  const out={...headers};
  delete out.host;
  delete out.connection;
  delete out.upgrade;
  delete out["content-length"];
  // Pedimos identidad para poder inyectar <base> en HTML de forma segura.
  out["accept-encoding"]="identity";
  out.host=`${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`;
  return out;
}
function rewriteProxyResponseHeaders(headers){
  const out={...headers};
  delete out["content-security-policy"];
  delete out["content-security-policy-report-only"];
  delete out["x-frame-options"];
  delete out["content-length"];
  if(out.location){
    try{
      const loc=String(out.location);
      if(loc.startsWith("/"))out.location=SDR_PROXY_PREFIX+loc;
      else if(/^https?:\/\//i.test(loc)){
        const u=new URL(loc);
        if(u.hostname===SDR_UPSTREAM_HOST)out.location=SDR_PROXY_PREFIX+(u.pathname||"/")+u.search;
      }
    }catch{}
  }
  return out;
}
function proxySdrHttp(req,res){
  const upstreamPath=sdrUpstreamPath(req.url);
  const options={
    hostname:SDR_UPSTREAM_HOST,
    port:SDR_UPSTREAM_PORT,
    method:req.method,
    path:upstreamPath,
    headers:cleanProxyRequestHeaders(req.headers)
  };
  const up=http.request(options,upRes=>{
    const chunks=[];
    upRes.on("data",c=>chunks.push(c));
    upRes.on("end",()=>{
      let body=Buffer.concat(chunks);
      const headers=rewriteProxyResponseHeaders(upRes.headers);
      const ct=String(upRes.headers["content-type"]||"").toLowerCase();

      // Mantiene todos los assets y URLs relativos dentro de /sdr/.
      if(ct.includes("text/html")){
        let text=body.toString("utf8");
        if(!/<base\b/i.test(text)){
          text=text.replace(/<head([^>]*)>/i,'<head$1><base href="/sdr/">');
        }
        body=Buffer.from(text,"utf8");
      }

      headers["content-length"]=String(body.length);
      res.writeHead(upRes.statusCode||200,headers);
      res.end(body);
    });
  });
  up.setTimeout(10000,()=>up.destroy(new Error("SDR upstream timeout")));
  up.on("error",e=>{
    if(res.headersSent){try{res.end()}catch{};return}
    res.writeHead(502,{"content-type":"text/plain; charset=utf-8","cache-control":"no-store"});
    res.end("SDR unavailable");
    console.error("SDR HTTP proxy:",e.message);
  });
  req.pipe(up);
}
function proxySdrUpgrade(req,clientSocket,head){
  const upstreamPath=sdrUpstreamPath(req.url);
  const headers={...req.headers};
  headers.host=`${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`;
  headers.origin=`http://${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`;

  const upReq=http.request({
    hostname:SDR_UPSTREAM_HOST,
    port:SDR_UPSTREAM_PORT,
    method:"GET",
    path:upstreamPath,
    headers
  });

  upReq.on("upgrade",(upRes,upSocket,upHead)=>{
    let response=`HTTP/1.1 ${upRes.statusCode||101} ${upRes.statusMessage||"Switching Protocols"}\r\n`;
    for(const [k,v] of Object.entries(upRes.headers)){
      if(v===undefined)continue;
      if(Array.isArray(v))for(const item of v)response+=`${k}: ${item}\r\n`;
      else response+=`${k}: ${v}\r\n`;
    }
    response+="\r\n";
    clientSocket.write(response);
    if(upHead?.length)clientSocket.write(upHead);
    if(head?.length)upSocket.write(head);
    upSocket.pipe(clientSocket);
    clientSocket.pipe(upSocket);

    const closeBoth=()=>{
      try{upSocket.destroy()}catch{}
      try{clientSocket.destroy()}catch{}
    };
    upSocket.on("error",closeBoth);
    clientSocket.on("error",closeBoth);
  });
  upReq.on("response",upRes=>{
    // El Kiwi puede responder HTTP cuando no hay canal disponible.
    let response=`HTTP/1.1 ${upRes.statusCode||502} ${upRes.statusMessage||"Bad Gateway"}\r\n`;
    for(const [k,v] of Object.entries(upRes.headers)){
      if(v===undefined)continue;
      response+=`${k}: ${Array.isArray(v)?v.join(", "):v}\r\n`;
    }
    response+="\r\n";
    clientSocket.write(response);
    upRes.pipe(clientSocket);
  });
  upReq.setTimeout(10000,()=>upReq.destroy(new Error("SDR WS upstream timeout")));
  upReq.on("error",e=>{
    console.error("SDR WS proxy:",e.message);
    try{clientSocket.write("HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n")}catch{}
    try{clientSocket.destroy()}catch{}
  });
  upReq.end();
}
async function sdrTcpStatus(){
  return await new Promise(resolve=>{
    const sock=net.createConnection({host:SDR_UPSTREAM_HOST,port:SDR_UPSTREAM_PORT});
    let done=false;
    const finish=ok=>{
      if(done)return;done=true;
      try{sock.destroy()}catch{}
      resolve(ok);
    };
    sock.setTimeout(2500);
    sock.on("connect",()=>finish(true));
    sock.on("timeout",()=>finish(false));
    sock.on("error",()=>finish(false));
  });
}

function corsHeaders(extra={}){return{"access-control-allow-origin":"*","cache-control":"no-store, no-cache, must-revalidate",...extra}}
const server=http.createServer((req,res)=>{
  const url=new URL(req.url,`http://${req.headers.host||"localhost"}`);
  if(url.pathname==="/sdr-status"){
    void sdrTcpStatus().then(ok=>{
      res.writeHead(ok?200:503,corsHeaders({"content-type":"application/json; charset=utf-8"}));
      res.end(JSON.stringify({ok,online:ok}));
    });
    return;
  }
  if(isSdrProxyPath(url.pathname)){
    proxySdrHttp(req,res);
    return;
  }
  if(url.pathname==="/spots"){pruneHistory();let after=Number(url.searchParams.get("after")||0);if(!Number.isFinite(after)||after<0)after=0;const result=history.filter(s=>s.seq>after);res.writeHead(200,corsHeaders({"content-type":"application/json; charset=utf-8"}));res.end(JSON.stringify({ok:true,spots:result,lastSeq:spotSequence,serverTime:Date.now()}));return}
  if(url.pathname==="/operator"){res.writeHead(200,corsHeaders({"content-type":"application/json; charset=utf-8"}));res.end(JSON.stringify({ok:true,...operatorState()}));return}
  if(url.pathname==="/net/latest"){
    const done=async()=>{let out=lastNetSummary,participants=[];if(supabaseConfigured())try{const rows=await sbRequest("cw_nets?select=*&status=eq.closed&order=net_date.desc&limit=1");if(rows?.length){out=rows[0];participants=await sbRequest(`cw_net_participants?select=callsign,country_code,first_seen,last_seen,receiver_count,max_snr,wpm&net_id=eq.${rows[0].id}&order=first_seen.asc&limit=300`)||[]}}catch{}const p=localParts();const showPublicToday=Boolean(out&&p.dow===0&&String(out.net_date||out.date||"")===localDateKey(0));res.writeHead(200,corsHeaders({"content-type":"application/json; charset=utf-8"}));res.end(JSON.stringify({ok:true,net:out||null,participants,showPublicToday}))};void done();return;
  }
  if(url.pathname==="/net/history"){
    const done=async()=>{
      try{
        if(!supabaseConfigured()){
          res.writeHead(200,corsHeaders({"content-type":"application/json; charset=utf-8"}));
          res.end(JSON.stringify({ok:true,nets:[],configured:false}));
          return;
        }
        const rows=await sbRequest("cw_nets?select=id,net_date,status,participants,countries,receivers,reports,best_call,best_snr,widest_call,widest_receivers&order=net_date.desc&limit=10")||[];
        const nets=[];
        for(const row of rows){
          const hasSummary=String(row.status||"")==="closed" || Number(row.participants||0)>0 || Number(row.reports||0)>0 || Number(row.receivers||0)>0;
          if(hasSummary){nets.push(row);continue}
          // Compatibilidad con el NET anterior: si Render no ejecutó el cierre,
          // los check-ins igualmente pueden haber quedado persistidos.
          const ps=await sbRequest(`cw_net_participants?select=callsign,country_code,receiver_count,receiver_calls,max_snr&net_id=eq.${encodeURIComponent(row.id)}&order=first_seen.asc&limit=500`)||[];
          if(!ps.length)continue;
          const countries=new Set(),receivers=new Set();
          let bestCall=null,bestSnr=null,widestCall=null,widestReceivers=0;
          for(const p of ps){
            if(p.country_code)countries.add(p.country_code);
            if(Array.isArray(p.receiver_calls))for(const rx of p.receiver_calls)if(rx)receivers.add(rx);
            const snr=p.max_snr===null?null:Number(p.max_snr);
            if(Number.isFinite(snr)&&(bestSnr===null||snr>bestSnr)){bestSnr=snr;bestCall=p.callsign}
            const rc=Number(p.receiver_count||0);
            if(rc>widestReceivers){widestReceivers=rc;widestCall=p.callsign}
          }
          nets.push({...row,participants:ps.length,countries:countries.size,receivers:receivers.size||Math.max(...ps.map(p=>Number(p.receiver_count||0)),0),best_call:bestCall,best_snr:bestSnr,widest_call:widestCall,widest_receivers:widestReceivers,recovered:true});
        }
        res.writeHead(200,corsHeaders({"content-type":"application/json; charset=utf-8"}));
        res.end(JSON.stringify({ok:true,nets,configured:true}));
      }catch(e){
        res.writeHead(500,corsHeaders({"content-type":"application/json; charset=utf-8"}));
        res.end(JSON.stringify({ok:false,error:e.message}));
      }
    };void done();return;
  }
  if(url.pathname==="/net/participants"){
    const done=async()=>{try{const id=String(url.searchParams.get("net_id")||"").trim();if(!id){res.writeHead(400,corsHeaders({"content-type":"application/json; charset=utf-8"}));res.end(JSON.stringify({ok:false,error:"net_id required"}));return}const participants=supabaseConfigured()?await sbRequest(`cw_net_participants?select=callsign,country_code,receiver_count,max_snr,wpm,first_seen&net_id=eq.${encodeURIComponent(id)}&order=first_seen.asc&limit=300`)||[]:[];res.writeHead(200,corsHeaders({"content-type":"application/json; charset=utf-8"}));res.end(JSON.stringify({ok:true,participants}))}catch(e){res.writeHead(500,corsHeaders({"content-type":"application/json; charset=utf-8"}));res.end(JSON.stringify({ok:false,error:e.message}))}};void done();return;
  }
  if(url.pathname==="/health"){pruneHistory();const st=operatorState();res.writeHead(200,corsHeaders({"content-type":"application/json; charset=utf-8"}));res.end(JSON.stringify({ok:true,live:rbnConnected,websocketClients:clients.size,historySpots:history.length,lastSeq:spotSequence,historyMinutes:10,secondsSinceRbnData:lastRbnDataAt?Math.round((Date.now()-lastRbnDataAt)/1000):null,channel:"7032.9-7033.1",whatsapp:{enabled:WHATSAPP_ENABLED,configured:whatsappConfigured(),destination:safeDestinationLabel(),provider:"WHAPI",groupConfigured:Boolean(WHAPI_GROUP_ID),channelConfigured:Boolean(WHAPI_CHANNEL_ID),channelPolicy:"important events only",alertCooldownMinutes:10,aggregationSeconds:5,digestMinutes:120,sentAlerts:whatsappSentAlerts,sentDigests:whatsappSentDigests,lastAlertAt:whatsappLastAlertAt||null,lastDigestAt:whatsappLastDigestAt||null,lastError:whatsappLastError||null},supabase:{configured:supabaseConfigured(),snapshots:lastSnapshotAt||null},operator:{timeZone:OPERATOR_TIME_ZONE,propagation:st.propagation,next:st.next,lastEvent:operatorLastEvent,lastError:operatorLastError||null},net:st.net,sdr:{proxy:true,upstream:`${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT}`,path:SDR_PROXY_PREFIX}}));return}
  res.writeHead(200,corsHeaders({"content-type":"text/plain; charset=utf-8"}));res.end("CW LATAM relay + operator LIVE\n");
});

const wss=new WebSocketServer({noServer:true,perMessageDeflate:false});

server.on("upgrade",(req,socket,head)=>{
  let pathname="/";
  try{pathname=new URL(req.url,`http://${req.headers.host||"localhost"}`).pathname}catch{}
  if(isSdrProxyPath(pathname)){
    proxySdrUpgrade(req,socket,head);
    return;
  }
  wss.handleUpgrade(req,socket,head,ws=>{
    wss.emit("connection",ws,req);
  });
});

wss.on("connection",ws=>{ws.isAlive=true;ws.on("pong",()=>{ws.isAlive=true});clients.add(ws);console.log(`Navegador conectado. Total: ${clients.size}`);ws.send(JSON.stringify({type:"status",live:rbnConnected,ts:Date.now()}));pruneHistory();ws.send(JSON.stringify({type:"history",spots:history,lastSeq:spotSequence}));ws.send(JSON.stringify(operatorState()));ws.on("close",()=>{clients.delete(ws)});ws.on("error",()=>{clients.delete(ws)})});
const websocketHeartbeat=setInterval(()=>{for(const ws of clients){if(ws.isAlive===false){clients.delete(ws);try{ws.terminate()}catch{};continue}ws.isAlive=false;try{ws.ping()}catch{clients.delete(ws);try{ws.terminate()}catch{}}}},WS_HEARTBEAT_MS);
const applicationHeartbeat=setInterval(()=>broadcast({type:"heartbeat",ts:Date.now(),live:rbnConnected,lastSeq:spotSequence,operator:{propagation:evaluatePropagation(),net:operatorState().net}}),APP_HEARTBEAT_MS);
wss.on("close",()=>{clearInterval(websocketHeartbeat);clearInterval(applicationHeartbeat)});

server.listen(PORT,()=>{console.log(`CW LATAM relay activo en puerto ${PORT}`);console.log("WhatsApp:",whatsappConfigured()?`ACTIVO -> ${safeDestinationLabel()}`:"DESACTIVADO / incompleto");console.log("Supabase operator:",supabaseConfigured()?"ACTIVO":"DESACTIVADO / incompleto");console.log("Operator timezone:",OPERATOR_TIME_ZONE);console.log("SDR proxy:",`${SDR_UPSTREAM_HOST}:${SDR_UPSTREAM_PORT} -> ${SDR_PROXY_PREFIX}`);connectRbn();void loadSpaceWeather();setInterval(()=>void loadSpaceWeather(),10*60*1000);void saveSnapshot();void operatorTick();setInterval(()=>void operatorTick(),OPERATOR_TICK_MS)});
