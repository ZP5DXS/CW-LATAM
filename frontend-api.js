"use strict";

/*
 * CW LATAM — Supabase compatibility shim
 * Backend: latam-api.zp5dxs.org
 *
 * Mantiene la interfaz usada por index.html:
 *   sb.rpc(...)
 *   sb.from(...).select(...).gte(...).order(...).limit(...)
 *   sb.channel(...).on(...).subscribe()
 *
 * Realtime temporal:
 * polling ligero contra la API local.
 */

(function () {

  const API_BASE = "https://latam-api.zp5dxs.org";

  async function request(path, options={}) {
    try {
      const response = await fetch(API_BASE + path, {
        cache: "no-store",
        ...options,
        headers: {
          "Content-Type": "application/json",
          ...(options.headers || {})
        }
      });

      let body=null;

      try {
        body=await response.json();
      } catch (_) {
        return {
          data:null,
          error:{
            message:"Respuesta no JSON",
            status:response.status
          }
        };
      }

      if (!response.ok || body?.error) {
        return {
          data:body?.data ?? null,
          error:body?.error || {
            message:`HTTP ${response.status}`
          }
        };
      }

      return {
        data:body?.data ?? body,
        error:null
      };

    } catch (err) {
      return {
        data:null,
        error:{
          message:err?.message || String(err)
        }
      };
    }
  }

  async function rpc(name,args={}) {
    return request(
      "/api/rpc/" + encodeURIComponent(name),
      {
        method:"POST",
        body:JSON.stringify(args || {})
      }
    );
  }

  class Query {
    constructor(table) {
      this.table=table;
      this.filters=[];
      this.orderBy=null;
      this.limitN=null;
    }

    select() {
      return this;
    }

    gte(column,value) {
      this.filters.push({
        op:"gte",
        column,
        value
      });
      return this;
    }

    order(column,opts={}) {
      this.orderBy={
        column,
        ascending:opts.ascending !== false
      };
      return this;
    }

    limit(n) {
      this.limitN=Number(n);
      return this.execute();
    }

    then(resolve,reject) {
      return this.execute().then(resolve,reject);
    }

    async execute() {
      let endpoint;

      if (this.table === "cw_chat_messages") {
        endpoint="/api/chat/history";
      } else if (this.table === "cw_operator_events") {
        endpoint="/api/operator-events";
      } else {
        return {
          data:null,
          error:{
            message:`Tabla no soportada por shim: ${this.table}`
          }
        };
      }

      const result=await request(endpoint);

      if (result.error || !Array.isArray(result.data)) {
        return result;
      }

      let rows=[...result.data];

      for (const f of this.filters) {
        if (f.op === "gte") {
          rows=rows.filter(row =>
            String(row?.[f.column] ?? "") >= String(f.value)
          );
        }
      }

      if (this.orderBy) {
        const {column,ascending}=this.orderBy;

        rows.sort((a,b)=>{
          const av=a?.[column];
          const bv=b?.[column];

          if (av===bv) return 0;
          if (av==null) return 1;
          if (bv==null) return -1;

          const cmp=String(av).localeCompare(String(bv));
          return ascending ? cmp : -cmp;
        });
      }

      if (Number.isFinite(this.limitN)) {
        rows=rows.slice(0,this.limitN);
      }

      return {
        data:rows,
        error:null
      };
    }
  }

  const channels=new Set();

  class Channel {
    constructor(name) {
      this.name=name;
      this.handlers=[];
      this.timer=null;
      this.seen=new Set();
      this.initialized=false;
    }

    on(kind,filter,callback) {
      if (
        kind === "postgres_changes" &&
        filter?.event === "INSERT"
      ) {
        this.handlers.push({
          table:filter.table,
          callback
        });
      }

      return this;
    }

    subscribe() {
      channels.add(this);
      this.poll();
      this.timer=setInterval(()=>this.poll(),4000);
      return this;
    }

    async poll() {
      for (const h of this.handlers) {

        let endpoint=null;

        if (h.table === "cw_chat_messages") {
          endpoint="/api/chat/history";
        }

        if (h.table === "cw_operator_events") {
          endpoint="/api/operator-events";
        }

        if (!endpoint) continue;

        const result=await request(endpoint);

        if (result.error || !Array.isArray(result.data)) {
          continue;
        }

        const rows=[...result.data].reverse();

        if (!this.initialized) {
          for (const row of rows) {
            if (row.id != null) {
              this.seen.add(String(row.id));
            }
          }
          continue;
        }

        for (const row of rows) {
          const id=String(
            row.id ??
            row.event_key ??
            row.created_at ??
            JSON.stringify(row)
          );

          if (this.seen.has(id)) continue;

          this.seen.add(id);

          try {
            h.callback({new:row});
          } catch (err) {
            console.warn(
              "CW LATAM shim realtime callback",
              err
            );
          }
        }
      }

      this.initialized=true;

      if (this.seen.size > 500) {
        this.seen=new Set(
          Array.from(this.seen).slice(-250)
        );
      }
    }

    unsubscribe() {
      if (this.timer) {
        clearInterval(this.timer);
        this.timer=null;
      }

      channels.delete(this);

      return Promise.resolve({
        status:"ok"
      });
    }
  }

  const sb={
    rpc,

    from(table) {
      return new Query(table);
    },

    channel(name) {
      return new Channel(name);
    },

    removeChannel(channel) {
      if (channel?.unsubscribe) {
        return channel.unsubscribe();
      }

      return Promise.resolve({
        status:"ok"
      });
    }
  };

  window.CW_LATAM_API_BASE=API_BASE;
  window.cwLatamLocalApi=sb;

})();
