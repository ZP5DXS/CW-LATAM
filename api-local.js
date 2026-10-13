"use strict";

const db = require("./db-local.js");

/*
 * CW LATAM — API PostgreSQL local
 *
 * Reemplaza exclusivamente las operaciones Supabase utilizadas
 * por el frontend. NO acepta SQL arbitrario.
 */

const RPC_SIGNATURES = Object.freeze({
  cw_add_usage: [
    "p_visitor_id","p_seconds"
  ],

  cw_focus_scores_v2: [
    "p_token","p_candidates"
  ],

  cw_get_collaboration_leaderboard: [
    "p_token","p_limit"
  ],

  cw_get_country_stats: [],

  cw_get_map_focus: [
    "p_token"
  ],

  cw_get_me: [
    "p_token"
  ],

  cw_get_model_stats: [],

  cw_get_public_station_locations: [],

  cw_get_public_stats: [],

  cw_get_season_leaderboard: [
    "p_limit"
  ],

  cw_get_season_status: [
    "p_token"
  ],

  cw_identify_profile: [
    "p_callsign",
    "p_locator",
    "p_lat",
    "p_lon",
    "p_visitor_id",
    "p_country_code",
    "p_country_name"
  ],

  cw_log_focus_predictions: [
    "p_token",
    "p_predictions"
  ],

  cw_rebuild_my_affinities: [
    "p_token"
  ],

  cw_recent_votes: [
    "p_token"
  ],

  cw_record_visit: [
    "p_visitor_id",
    "p_country_code",
    "p_country_name"
  ],

  cw_send_chat: [
    "p_token",
    "p_message"
  ],

  cw_set_map_focus: [
    "p_token",
    "p_lat",
    "p_lon",
    "p_zoom",
    "p_enabled"
  ],

  cw_system_info: [],

  cw_update_profile: [
    "p_token",
    "p_callsign",
    "p_locator",
    "p_lat",
    "p_lon",
    "p_country_code",
    "p_country_name"
  ],

  cw_vote: [
    "p_token",
    "p_dx_call",
    "p_frequency",
    "p_rating",
    "p_snr",
    "p_spotter_count",
    "p_spot_seq"
  ],

  cw_watchlist_add: [
    "p_token",
    "p_callsign"
  ],

  cw_watchlist_get: [
    "p_token"
  ],

  cw_watchlist_remove: [
    "p_token",
    "p_callsign"
  ]
});


function sendJson(res,status,payload){
  const body=JSON.stringify(payload);

  res.statusCode=status;
  res.setHeader("Content-Type","application/json; charset=utf-8");
  res.setHeader("Cache-Control","no-store");
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Headers","Content-Type");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  res.setHeader("Content-Length",Buffer.byteLength(body));

  res.end(body);
}


function readJson(req,maxBytes=1024*1024){
  return new Promise((resolve,reject)=>{
    let size=0;
    const chunks=[];

    req.on("data",chunk=>{
      size+=chunk.length;

      if(size>maxBytes){
        reject(new Error("request_too_large"));
        req.destroy();
        return;
      }

      chunks.push(chunk);
    });

    req.on("end",()=>{
      if(!chunks.length){
        resolve({});
        return;
      }

      try{
        resolve(
          JSON.parse(
            Buffer.concat(chunks).toString("utf8")
          )
        );
      }catch{
        reject(new Error("invalid_json"));
      }
    });

    req.on("error",reject);
  });
}


function quoteIdentifier(value){
  return `"${String(value).replace(/"/g,'""')}"`;
}


async function executeRpc(name,args={}){
  const signature=RPC_SIGNATURES[name];

  if(!signature){
    const err=new Error("rpc_not_allowed");
    err.code="RPC_NOT_ALLOWED";
    throw err;
  }

  /*
   * Usamos argumentos nombrados para mantener el mismo contrato
   * que Supabase RPC y evitar depender del orden del objeto JSON.
   */
  const values=[];
  const assignments=[];

  for(const param of signature){
    values.push(
      Object.prototype.hasOwnProperty.call(args,param)
        ? args[param]
        : null
    );

    assignments.push(
      `${quoteIdentifier(param)} => $${values.length}`
    );
  }

  const call=signature.length
    ? `public.${quoteIdentifier(name)}(${assignments.join(",")})`
    : `public.${quoteIdentifier(name)}()`;

  const sql=`SELECT ${call} AS data`;

  const result=await db.pool.query(sql,values);

  return result.rows[0]?.data ?? null;
}


async function getChatHistory(){
  const result=await db.pool.query(`
    SELECT *
    FROM public.cw_chat_messages
    WHERE created_at >= now() - interval '24 hours'
    ORDER BY created_at DESC
    LIMIT 80
  `);

  return result.rows;
}


async function getOperatorEvents(){
  const result=await db.pool.query(`
    SELECT *
    FROM public.cw_operator_events
    ORDER BY created_at DESC
    LIMIT 60
  `);

  return result.rows;
}


/*
 * Retorna true si la petición pertenecía a nuestra API.
 * Así server-vps.js puede conservar intactas sus rutas actuales.
 */
async function handleApi(req,res,url){
  if(!url.pathname.startsWith("/api/")){
    return false;
  }

  if(req.method==="OPTIONS"){
    res.statusCode=204;
    res.setHeader("Access-Control-Allow-Origin","*");
    res.setHeader("Access-Control-Allow-Headers","Content-Type");
    res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
    res.end();
    return true;
  }

  try{

    if(
      req.method==="GET" &&
      url.pathname==="/api/health"
    ){
      const health=await db.dbHealth();

      sendJson(res,200,{
        ok:true,
        database:health
      });

      return true;
    }


    if(
      req.method==="GET" &&
      url.pathname==="/api/chat/history"
    ){
      sendJson(res,200,{
        data:await getChatHistory(),
        error:null
      });

      return true;
    }


    if(
      req.method==="GET" &&
      url.pathname==="/api/operator-events"
    ){
      sendJson(res,200,{
        data:await getOperatorEvents(),
        error:null
      });

      return true;
    }


    /*
     * Compatibilidad temporal:
     * esta función no existe en el dump local y el frontend
     * explícitamente la considera enriquecimiento secundario.
     */
    if(
      req.method==="POST" &&
      url.pathname==="/api/rpc/cw_attach_network_snapshot"
    ){
      sendJson(res,200,{
        data:{
          skipped:true,
          reason:"compatibility_noop"
        },
        error:null
      });

      return true;
    }


    const match=url.pathname.match(
      /^\/api\/rpc\/([a-z0-9_]+)$/
    );

    if(match && req.method==="POST"){
      const name=match[1];

      if(!RPC_SIGNATURES[name]){
        sendJson(res,404,{
          data:null,
          error:{
            message:"RPC no permitida",
            code:"RPC_NOT_ALLOWED"
          }
        });

        return true;
      }

      const args=await readJson(req);
      const data=await executeRpc(name,args);

      sendJson(res,200,{
        data,
        error:null
      });

      return true;
    }





    sendJson(res,404,{
      data:null,
      error:{
        message:"Endpoint no encontrado",
        code:"NOT_FOUND"
      }
    });

    return true;

  }catch(err){

    console.error(
      "[api-local]",
      req.method,
      url.pathname,
      err
    );

    sendJson(res,500,{
      data:null,
      error:{
        message:err?.message || "internal_error",
        code:err?.code || "INTERNAL_ERROR"
      }
    });

    return true;
  }
}


module.exports={
  handleApi,
  executeRpc,
  RPC_SIGNATURES
};
