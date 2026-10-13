const { Pool } = require("pg");

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL no configurada");
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000
});

async function dbHealth() {
  const r = await pool.query(
    "SELECT current_database() AS db, current_user AS usr, now() AS ts"
  );
  return r.rows[0];
}

async function rpcLocal(name, args = {}) {
  const allowed = new Set([
    "cw_get_public_stats"
  ]);

  if (!allowed.has(name)) {
    throw new Error(`RPC local no permitida: ${name}`);
  }

  if (name === "cw_get_public_stats") {
    const r = await pool.query(
      "SELECT public.cw_get_public_stats() AS result"
    );
    return r.rows[0]?.result ?? null;
  }
}

async function insertOperatorEvent(row) {
  const r = await pool.query(`
    INSERT INTO public.cw_operator_events
      (event_key,event_type,title,message,metadata)
    VALUES ($1,$2,$3,$4,$5::jsonb)
    ON CONFLICT (event_key) DO NOTHING
    RETURNING *
  `, [
    row.event_key,
    row.event_type,
    row.title,
    row.message,
    JSON.stringify(row.metadata ?? {})
  ]);

  return r.rows;
}

async function upsertSnapshot(row) {
  await pool.query(`
    INSERT INTO public.cw_operator_snapshots (
      bucket_start,observed_at,local_date,local_hour,window_minutes,
      spots,calls,receivers,channel_calls,
      call_list,receiver_list,channel_list,
      median_snr,avg_snr,countries,routes,
      band_state,trend,trend_score,metadata
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,
      $10::jsonb,$11::jsonb,$12::jsonb,
      $13,$14,$15::jsonb,$16::jsonb,
      $17,$18,$19,$20::jsonb
    )
    ON CONFLICT (bucket_start) DO NOTHING
  `, [
    row.bucket_start,row.observed_at,row.local_date,row.local_hour,
    row.window_minutes,row.spots,row.calls,row.receivers,row.channel_calls,
    JSON.stringify(row.call_list ?? []),
    JSON.stringify(row.receiver_list ?? []),
    JSON.stringify(row.channel_list ?? []),
    row.median_snr,row.avg_snr,
    JSON.stringify(row.countries ?? []),
    JSON.stringify(row.routes ?? []),
    row.band_state,row.trend,row.trend_score,
    JSON.stringify(row.metadata ?? {})
  ]);
}

async function snapshotsBetween(start,end,fields="*") {
  const allowed = new Set([
    "bucket_start,spots,call_list,receiver_list,channel_list,median_snr,avg_snr,countries,routes",
    "bucket_start,spots,calls,receivers,median_snr,band_state,trend,trend_score,routes",
    "observed_at,local_hour,spots,calls,receivers,median_snr,band_state,trend,trend_score,routes"
  ]);

  if (!allowed.has(fields))
    throw new Error("Selección snapshot no permitida");

  const timeColumn = fields.startsWith("observed_at")
    ? "observed_at"
    : "bucket_start";

  const r = await pool.query(`
    SELECT ${fields}
    FROM public.cw_operator_snapshots
    WHERE ${timeColumn} >= $1
      AND ${timeColumn} < $2
    ORDER BY ${timeColumn} ASC
    LIMIT 10000
  `,[start,end]);

  return r.rows;
}

async function getNet(netDate) {
  const r = await pool.query(`
    SELECT *
    FROM public.cw_nets
    WHERE net_date=$1
    LIMIT 1
  `,[netDate]);

  return r.rows[0] ?? null;
}

async function createNet(row) {
  const r = await pool.query(`
    INSERT INTO public.cw_nets
      (net_date,name,frequency_khz,status)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (net_date) DO NOTHING
    RETURNING *
  `,[row.net_date,row.name,row.frequency_khz,row.status]);

  if (r.rows[0]) return r.rows[0];
  return getNet(row.net_date);
}

async function updateNet(id, patch) {
  const allowed = new Set([
    "status","starts_at","ends_at","participants","countries",
    "receivers","reports","reports_start","best_call","best_snr",
    "widest_call","widest_receivers","summary","updated_at"
  ]);

  const entries = Object.entries(patch)
    .filter(([k]) => allowed.has(k));

  if (!entries.length) return;

  const sets = entries.map(
    ([k],i) => `${k}=$${i+2}${k==="summary" ? "::jsonb" : ""}`
  );

  const values = entries.map(([k,v]) =>
    k==="summary" ? JSON.stringify(v ?? {}) : v
  );

  await pool.query(
    `UPDATE public.cw_nets SET ${sets.join(",")} WHERE id=$1`,
    [id,...values]
  );
}

async function getNetParticipants(netId) {
  const r = await pool.query(`
    SELECT *
    FROM public.cw_net_participants
    WHERE net_id=$1
    ORDER BY first_seen ASC
    LIMIT 500
  `,[netId]);

  return r.rows;
}

async function upsertNetParticipant(row) {
  await pool.query(`
    INSERT INTO public.cw_net_participants (
      net_id,callsign,country_code,first_seen,last_seen,
      spots,receiver_count,receiver_calls,max_snr,wpm
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10)
    ON CONFLICT (net_id,callsign)
    DO UPDATE SET
      country_code=EXCLUDED.country_code,
      first_seen=EXCLUDED.first_seen,
      last_seen=EXCLUDED.last_seen,
      spots=EXCLUDED.spots,
      receiver_count=EXCLUDED.receiver_count,
      receiver_calls=EXCLUDED.receiver_calls,
      max_snr=EXCLUDED.max_snr,
      wpm=EXCLUDED.wpm
  `,[
    row.net_id,row.callsign,row.country_code,row.first_seen,row.last_seen,
    row.spots,row.receiver_count,
    JSON.stringify(row.receiver_calls ?? []),
    row.max_snr,row.wpm
  ]);
}


module.exports = {
  pool,
  dbHealth,
  rpcLocal,
  insertOperatorEvent,
  upsertSnapshot,
  snapshotsBetween,
  getNet,
  createNet,
  updateNet,
  getNetParticipants,
  upsertNetParticipant
};

async function getLatestClosedNet() {
  const r = await pool.query(`
    SELECT *
    FROM public.cw_nets
    WHERE status = 'closed'
    ORDER BY net_date DESC
    LIMIT 1
  `);
  return r.rows[0] ?? null;
}

async function getRecentNets(limit = 10) {
  const n = Math.min(Math.max(Number(limit) || 10, 1), 50);
  const r = await pool.query(`
    SELECT
      id, net_date, status, participants, countries, receivers,
      reports, best_call, best_snr, widest_call, widest_receivers
    FROM public.cw_nets
    ORDER BY net_date DESC
    LIMIT $1
  `, [n]);
  return r.rows;
}

async function getNetParticipantsPublic(netId, limit = 300) {
  const n = Math.min(Math.max(Number(limit) || 300, 1), 500);
  const r = await pool.query(`
    SELECT
      callsign, country_code, first_seen, last_seen,
      receiver_count, receiver_calls, max_snr, wpm
    FROM public.cw_net_participants
    WHERE net_id = $1
    ORDER BY first_seen ASC
    LIMIT $2
  `, [netId, n]);
  return r.rows;
}

module.exports.getLatestClosedNet = getLatestClosedNet;
module.exports.getRecentNets = getRecentNets;
module.exports.getNetParticipantsPublic = getNetParticipantsPublic;
