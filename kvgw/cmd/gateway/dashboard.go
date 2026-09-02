package main

// dashboardHTML is a self-contained control room served at GET /dashboard.
// No build step, no dependencies — polls the gateway's own JSON endpoints.
const dashboardHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>KVStore — control room</title>
<style>
  :root{
    --bg:#0b0d10; --panel:#12151b; --line:#1c2330;
    --phosphor:#9ae6b4; --amber:#e6b35a; --steel:#8b9bb4;
    --ink:#d5ddd8; --dim:#6d7b88; --bad:#e07a7a;
  }
  *{box-sizing:border-box}
  html,body{margin:0;background:var(--bg);color:var(--ink);
    font-family:"IBM Plex Sans","Segoe UI",system-ui,sans-serif;}
  body{
    min-height:100vh;
    background-image:
      linear-gradient(rgba(139,155,180,.045) 1px, transparent 1px),
      linear-gradient(90deg, rgba(139,155,180,.045) 1px, transparent 1px);
    background-size:28px 28px;
  }
  .wrap{max-width:1180px;margin:0 auto;padding:28px 20px 64px}
  header.brand{display:flex;flex-wrap:wrap;gap:12px;align-items:baseline;margin-bottom:8px}
  h1{margin:0;font-size:22px;letter-spacing:.18em;text-transform:uppercase;color:var(--phosphor);
    text-shadow:0 0 18px rgba(154,230,180,.25);font-weight:650}
  .tag{color:var(--steel);font-size:12px;letter-spacing:.08em;text-transform:uppercase}
  .top{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin:6px 0 20px;color:var(--steel);font-size:13px}
  .pill{padding:3px 10px;border-radius:999px;font-size:11px;letter-spacing:.08em;text-transform:uppercase;border:1px solid var(--line)}
  .pill.ok{color:var(--phosphor);border-color:rgba(154,230,180,.35);background:rgba(154,230,180,.08)}
  .pill.bad{color:var(--bad);border-color:rgba(224,122,122,.4);background:rgba(224,122,122,.08)}
  h2{font-size:11px;color:var(--steel);font-weight:600;margin:0 0 10px;text-transform:uppercase;letter-spacing:1.4px}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px;margin-bottom:18px}
  .node{border:1px solid var(--line);border-radius:4px;padding:12px;background:var(--panel);position:relative}
  .node.down{opacity:.55}
  .node::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--phosphor)}
  .node.down::before{background:var(--amber)}
  .nh{display:flex;align-items:center;gap:8px;font-weight:600;font-size:13px;font-variant-numeric:tabular-nums}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--phosphor);box-shadow:0 0 8px var(--phosphor)}
  .node.down .dot{background:var(--amber);box-shadow:0 0 8px var(--amber)}
  .nm{margin-top:6px;font-size:12px;color:var(--steel);font-variant-numeric:tabular-nums;line-height:1.45}
  .bar{height:7px;background:#0b0d10;border:1px solid var(--line);border-radius:2px;margin-top:8px;overflow:hidden}
  .bar>i{display:block;height:100%;background:linear-gradient(90deg,var(--phosphor),var(--amber));width:0}
  .hint{color:var(--amber)}
  .panel{border:1px solid var(--line);border-radius:4px;padding:14px;background:var(--panel);margin-bottom:16px}
  .row{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
  input,select{background:#0b0d10;color:var(--ink);border:1px solid var(--line);border-radius:3px;
    padding:8px 10px;font:inherit;font-size:13px}
  input:focus{outline:1px solid var(--phosphor);border-color:var(--phosphor)}
  button{background:transparent;color:var(--phosphor);border:1px solid rgba(154,230,180,.45);border-radius:3px;
    padding:8px 13px;font:inherit;font-size:12px;letter-spacing:.06em;text-transform:uppercase;cursor:pointer}
  button:hover{background:rgba(154,230,180,.1)}
  button.ghost{color:var(--steel);border-color:var(--line)}
  button.amber{color:var(--amber);border-color:rgba(230,179,90,.45)}
  button.on{background:rgba(154,230,180,.12);border-color:var(--phosphor)}
  #msg{margin-top:10px;font-size:13px;color:var(--steel);font-variant-numeric:tabular-nums;min-height:18px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}
  th{color:var(--steel);font-weight:500;letter-spacing:.04em;text-transform:uppercase;font-size:11px}
  tbody tr{cursor:pointer}
  tbody tr:hover{background:rgba(154,230,180,.05)}
  tbody tr.sel{background:rgba(154,230,180,.1);outline:1px solid rgba(154,230,180,.28)}
  .muted{color:var(--dim)}
  .metrics{display:grid;grid-template-columns:repeat(auto-fill,minmax(118px,1fr));gap:8px;margin-bottom:10px}
  .metric{border:1px solid var(--line);border-radius:4px;padding:8px 10px;background:var(--panel)}
  .metric .k{font-size:10px;color:var(--steel);letter-spacing:.08em;text-transform:uppercase}
  .metric .v{font-size:18px;font-weight:600;color:var(--phosphor);font-variant-numeric:tabular-nums;margin-top:2px}
  .sparkwrap{display:flex;align-items:center;gap:10px;margin-bottom:18px;color:var(--steel);font-size:12px}
  #spark{display:block;background:#0b0d10;border:1px solid var(--line);border-radius:3px}
  input#ttl,input#naddr,input#cas{width:140px}
  input#filter{min-width:180px;flex:1}
  .split{display:grid;grid-template-columns:1.4fr .9fr;gap:16px}
  .inspect dl{margin:0;display:grid;grid-template-columns:88px 1fr;gap:6px 10px;font-size:13px}
  .inspect dt{color:var(--steel);text-transform:uppercase;font-size:10px;letter-spacing:.08em;padding-top:4px}
  .inspect dd{margin:0;word-break:break-all;font-variant-numeric:tabular-nums}
  #inspect.flash{animation:flash .85s ease}
  @keyframes flash{from{box-shadow:inset 0 0 0 1px var(--amber);background:rgba(230,179,90,.16)}to{box-shadow:none}}
  #oplog{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;
    background:#0b0d10;border:1px solid var(--line);border-radius:3px;padding:8px 10px;
    max-height:210px;overflow:auto;line-height:1.55;color:var(--steel);white-space:pre}
  #oplog .ok{color:var(--phosphor)}
  #oplog .bad{color:var(--bad)}
  #oplog .warn{color:var(--amber)}
  .valhex{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px}
  .rpills{display:flex;flex-wrap:wrap;gap:6px}
  .inspect pre{background:#0b0d10;border:1px solid var(--line);border-radius:3px;padding:8px 10px;
    overflow:auto;font-size:12px;color:var(--phosphor);margin:6px 0 0;white-space:pre;max-height:220px}
  .node button{padding:4px 9px;font-size:10px;margin-top:8px}
  @media(max-width:800px){
    .split{grid-template-columns:1fr}
    .row{flex-direction:column;align-items:stretch}
    input,button,select{width:100%;min-height:44px;font-size:16px}
    button{padding:12px 16px}
    .wrap{padding:16px 12px 48px}
  }
</style>
</head>
<body>
<div class="wrap">
  <header class="brand">
    <h1>KVStore</h1>
    <span class="tag">control room · C engine · Rust nodes · Go coordinator</span>
  </header>
  <div class="top">
    <span id="cfg">loading…</span>
    <span class="pill" id="healthy">…</span>
    <span style="margin-left:auto" class="muted">auto-refresh 1.5s</span>
  </div>

  <h2>nodes / heartbeat</h2>
  <div class="grid" id="nodes"></div>

  <h2>metrics</h2>
  <div class="metrics" id="metrics"></div>
  <div class="sparkwrap">
    <svg id="spark" width="240" height="36" viewBox="0 0 240 36" aria-label="ops per poll"></svg>
    <span id="sparkcap">ops / 1.5s</span>
  </div>

  <div class="panel">
    <h2>key operations</h2>
    <div class="row">
      <input id="k" placeholder="key" value="hello" size="14" />
      <input id="v" placeholder="value" value="world" size="16" />
      <input id="ttl" placeholder="ttl seconds" type="number" min="0" />
      <input id="cas" placeholder="If-Match version" />
      <button id="put">put</button>
      <button id="get" class="ghost">get</button>
      <button id="del" class="ghost">delete</button>
      <button id="route" class="ghost">route</button>
    </div>
    <div class="row" style="margin-top:8px">
      <input id="naddr" placeholder="host:port" />
      <button id="addn" class="ghost">add node</button>
      <button id="rmn" class="amber">remove node</button>
      <button id="compact" class="amber">compact</button>
      <button id="export" class="ghost">export JSON</button>
      <button id="importBtn" class="ghost">import JSON</button>
      <input id="importFile" type="file" accept="application/json,.json" hidden />
    </div>
    <div id="msg"></div>
  </div>

  <div class="panel">
    <h2>ops log</h2>
    <div id="oplog" class="muted">waiting for actions…</div>
  </div>

  <div class="split">
    <div class="panel">
      <h2>CRDT counter</h2>
      <div class="row">
        <input id="ck" placeholder="counter key" value="votes" size="12" />
        <button id="cinc">+1</button>
        <button id="cdec" class="ghost">-1</button>
        <button id="cread" class="ghost">read</button>
      </div>
      <div id="cmsg" class="muted" style="margin-top:8px;font-variant-numeric:tabular-nums">—</div>
    </div>
    <div class="panel">
      <h2>CRDT set</h2>
      <div class="row">
        <input id="sk" placeholder="set key" value="team" size="10" />
        <input id="sm" placeholder="member" value="alice" size="10" />
        <button id="sadd">add</button>
        <button id="srm" class="ghost">remove</button>
        <button id="sread" class="ghost">read</button>
      </div>
      <div id="smsg" class="muted" style="margin-top:8px">—</div>
    </div>
  </div>

  <div class="split">
    <div class="panel">
      <h2>key browser</h2>
      <div class="row" style="margin-bottom:10px">
        <input id="filter" placeholder="prefix filter (GET /scan?prefix=)" />
        <button id="hexbtn" class="ghost">values: utf-8</button>
        <button id="rmprefix" class="amber">delete prefix</button>
      </div>
      <table><thead><tr><th>key</th><th>value</th><th>version</th><th>replicas</th><th>expires</th></tr></thead>
      <tbody id="keys"></tbody></table>
    </div>
    <div class="panel inspect" id="inspect">
      <h2>inspect</h2>
      <div id="inspectBody" class="muted">click a key row</div>
      <div class="row" style="margin-top:12px">
        <button id="iget" class="ghost">get</button>
        <button id="idel" class="ghost">delete</button>
        <button id="iroute" class="ghost">route</button>
      </div>
    </div>
  </div>
</div>
<script>
(function(){
  function el(id){return document.getElementById(id);}
  function esc(t){var d=document.createElement('div');d.textContent=(t==null?'':String(t));return d.innerHTML;}
  function jget(u){return fetch(u).then(function(r){return r.json();});}
  function msg(t){el('msg').textContent=t;}
  function hdr(r,n){return r.headers.get('X-KV-'+n)||r.headers.get('X-Triglot-'+n)||'';}
  function clock(){
    var d=new Date();
    function p(n){return (n<10?'0':'')+n;}
    return p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds());
  }
  var hist=[];
  var lastTotal=null;
  var allRows=[];
  var hexMode=false;
  var selected=null;
  var watchVer=null;
  var ops=[];
  var filterTimer=null;

  function looksLikePrefix(q){
    q=(q||'').trim();
    if(!q) return false;
    if(/\s/.test(q)) return false;
    return true;
  }
  function scanURL(){
    var q=(el('filter').value||'').trim();
    if(q.slice(-1)==='*') q=q.slice(0,-1);
    if(looksLikePrefix(q)) return '/scan?prefix='+encodeURIComponent(q);
    return '/scan';
  }
  function toHex(s){
    try{
      var bytes=new TextEncoder().encode(String(s==null?'':s));
      var hex='';
      for(var i=0;i<bytes.length;i++) hex+=('0'+bytes[i].toString(16)).slice(-2);
      return hex||'—';
    }catch(e){return String(s);}
  }
  function showVal(v){ return hexMode ? toHex(v) : String(v==null?'':v); }
  function prettyJSON(v){
    if(v==null) return null;
    var s=String(v).trim();
    if(!s) return null;
    var c=s.charAt(0);
    if(c!=='{' && c!=='[') return null;
    try{ return JSON.stringify(JSON.parse(s), null, 2); }catch(e){ return null; }
  }
  function currentPrefix(){
    var q=(el('filter').value||'').trim();
    if(q.slice(-1)==='*') q=q.slice(0,-1);
    return q;
  }
  function fetchRoute(key){
    if(!key) return;
    jget('/route/'+encodeURIComponent(key)).then(function(o){
      if(selected && selected.key===key){
        selected.route=o.replicas||[];
        renderInspect();
      }
    }).catch(function(){});
  }
  function expLabel(e){
    if(!e || !e.expires_at){return '—';}
    var sec=e.expires_at-Math.floor(Date.now()/1000);
    if(sec<=0){return 'expired';}
    if(sec<60){return sec+'s';}
    if(sec<3600){return Math.floor(sec/60)+'m '+ (sec%60)+'s';}
    return Math.floor(sec/3600)+'h';
  }
  function logOp(kind, status, detail){
    var cls='ok';
    if(status>=400) cls='bad';
    else if(status>=300) cls='warn';
    ops.unshift({t:clock(), kind:kind, status:status, detail:detail||'', cls:cls});
    if(ops.length>24) ops.pop();
    el('oplog').innerHTML=ops.map(function(o){
      return '<span class="'+o.cls+'">'+esc(o.t)+'  '+esc(o.kind.toUpperCase())+'  '+esc(o.status)+'  '+esc(o.detail)+'</span>';
    }).join('\n') || '<span class="muted">waiting for actions…</span>';
  }
  function drawSpark(){
    var svg=el('spark'), w=240, h=36;
    if(hist.length<2){
      svg.innerHTML='<text x="8" y="22" fill="#6d7b88" font-size="11">collecting…</text>';
      return;
    }
    var max=1;
    for(var i=0;i<hist.length;i++){if(hist[i]>max)max=hist[i];}
    var pts=[];
    for(var j=0;j<hist.length;j++){
      var x=(j/(hist.length-1))*(w-4)+2;
      var y=h-3-(hist[j]/max)*(h-8);
      pts.push(x.toFixed(1)+','+y.toFixed(1));
    }
    var last=hist[hist.length-1];
    svg.innerHTML='<polyline fill="none" stroke="#9ae6b4" stroke-width="1.6" points="'+pts.join(' ')+'"/>';
    el('sparkcap').textContent=last+' ops / 1.5s  (last '+hist.length+' samples)';
  }
  function renderInspect(){
    var body=el('inspectBody');
    if(!selected){
      body.innerHTML='<span class="muted">click a key row</span>';
      return;
    }
    var exp=selected.expires_at?String(selected.expires_at)+' ('+expLabel(selected)+')':'never';
    var json=(!hexMode)?prettyJSON(selected.value):null;
    var valHtml;
    if(json){
      valHtml='<pre>'+esc(json)+'</pre>';
    }else{
      valHtml='<span class="'+(hexMode?'valhex':'')+'">'+esc(showVal(selected.value))+'</span>';
    }
    var pills=(selected.route||[]).map(function(a){
      return '<span class="pill ok">'+esc(a)+'</span>';
    }).join('');
    if(!pills) pills='<span class="muted">—</span>';
    body.innerHTML='<dl>'+
      '<dt>key</dt><dd>'+esc(selected.key)+'</dd>'+
      '<dt>value</dt><dd>'+valHtml+'</dd>'+
      '<dt>version</dt><dd>v'+esc(selected.version)+'</dd>'+
      '<dt>replicas</dt><dd>'+esc(selected.replicas)+'</dd>'+
      '<dt>route</dt><dd class="rpills">'+pills+'</dd>'+
      '<dt>expiry</dt><dd>'+esc(exp)+'</dd>'+
      '<dt>watch</dt><dd class="muted">re-get each refresh</dd>'+
      '</dl>';
  }
  function flashInspect(){
    var p=el('inspect');
    p.classList.remove('flash');
    void p.offsetWidth;
    p.classList.add('flash');
  }
  function loadFields(row){
    if(!row) return;
    el('k').value=row.key||'';
    el('v').value=row.value==null?'':String(row.value);
    if(row.version!=null) el('cas').value=String(row.version);
  }
  function selectRow(row, fromClick){
    selected=row?{key:row.key,value:row.value,version:row.version,replicas:row.replicas,expires_at:row.expires_at,route:row.route||[]}:null;
    watchVer=selected?String(selected.version):null;
    if(fromClick) loadFields(selected);
    renderKeys();
    renderInspect();
    if(selected) fetchRoute(selected.key);
  }
  function renderKeys(){
    var q=(el('filter').value||'').trim().toLowerCase();
    var prefix=looksLikePrefix((el('filter').value||'').trim().replace(/\*$/,''));
    var shown=allRows.filter(function(e){
      if(!q) return true;
      if(prefix) return true;
      return String(e.key).toLowerCase().indexOf(q)!==-1;
    });
    var rows=shown.map(function(e){
      var sel=selected && e.key===selected.key ? ' class="sel"' : '';
      var vcls=hexMode?' class="valhex"':'';
      return '<tr data-key="'+esc(e.key)+'"'+sel+'><td>'+esc(e.key)+'</td><td'+vcls+'>'+esc(showVal(e.value))+'</td><td>v'+e.version+'</td><td>'+e.replicas+'</td><td>'+esc(expLabel(e))+'</td></tr>';
    }).join('');
    el('keys').innerHTML=rows||'<tr><td colspan="5" class="muted">'+(allRows.length?'no keys match filter':'no keys yet')+'</td></tr>';
  }
  function fetchKeys(){
    return jget(scanURL()).then(function(s){
      allRows=s.entries||[];
      if(selected){
        var hit=null;
        for(var i=0;i<allRows.length;i++){
          if(allRows[i].key===selected.key){hit=allRows[i];break;}
        }
        if(hit){
          selected.value=hit.value;
          selected.replicas=hit.replicas;
          selected.expires_at=hit.expires_at;
          if(String(hit.version)!==String(selected.version)){
            selected.version=hit.version;
          }
        }
      }
      renderKeys();
      renderInspect();
    }).catch(function(){});
  }
  function watchSelected(){
    if(!selected || !selected.key) return;
    var k=selected.key;
    fetch('/kv/'+encodeURIComponent(k)).then(function(r){
      return r.text().then(function(t){
        var ver=hdr(r,'Version');
        if(r.status===200){
          selected.value=t;
          if(ver){
            if(watchVer!=null && ver!==watchVer){
              selected.version=ver;
              flashInspect();
              msg('watch '+k+' · version '+watchVer+' → '+ver);
            }
            selected.version=ver;
            watchVer=ver;
          }
        }else if(watchVer!=null && watchVer!=='missing'){
          watchVer='missing';
          selected.value='';
          flashInspect();
          msg('watch '+k+' · gone ('+r.status+')');
        }
        renderInspect();
        renderKeys();
      });
    }).catch(function(){});
  }
  function refresh(){
    jget('/health').then(function(h){
      el('cfg').textContent='RF '+h.rf+' · R '+h.read+' · W '+h.write+(h.consistent?' · consistent':' · NOT consistent');
      var hb=el('healthy');hb.textContent=h.healthy?'all nodes up':'degraded';hb.className='pill '+(h.healthy?'ok':'bad');
      el('nodes').innerHTML=(h.nodes||[]).map(function(n){
        var down=n.down || !n.up;
        var last=n.last_ok?('last ok '+String(n.last_ok).replace('T',' ').replace(/\.\d+/,'').replace('Z','')):'no heartbeat yet';
        var cap=n.capacity||0;
        var load=n.load||0;
        var pct=cap?Math.max(0,Math.min(100,Math.round(load*100))):0;
        var hints=n.hints_pending||0;
        var tombs=n.tombstones||0;
        var bar=cap?('<div class="bar" title="load '+pct+'% of '+cap+'"><i style="width:'+pct+'%"></i></div>'+
          '<div class="nm">load '+pct+'% · cap '+cap+'</div>'):'';
        var hintLine=hints?('<div class="nm hint">pending hints '+hints+'</div>'):('<div class="nm">pending hints 0</div>');
        var forced=!!n.forced;
        var faultLabel=forced?'heal':'fault';
        var faultDown=forced?'0':'1';
        var forcedLine=forced?'<div class="nm hint">forced down</div>':'';
        return '<div class="node '+(down?'down':'up')+'"><div class="nh"><span class="dot"></span>'+esc(n.addr)+'</div>'+
          '<div class="nm">'+(n.up?('keys '+n.count+' · tombs '+tombs+' · max v'+n.max_version):('offline'))+'</div>'+
          '<div class="nm">'+esc(last)+'</div>'+hintLine+forcedLine+bar+
          '<button class="'+(forced?'amber':'ghost')+' fault" data-addr="'+esc(n.addr)+'" data-down="'+faultDown+'">'+faultLabel+'</button></div>';
      }).join('');
      var m=h.metrics||{};
      function fmtMs(v){
        var n=Number(v||0);
        if(!isFinite(n)) n=0;
        return n>=10?n.toFixed(1):n.toFixed(2);
      }
      var cells=[['reads',m.gets],['writes',m.puts],['deletes',m.dels],['crdt ops',m.mutations],
        ['quorum fails',m.quorum_failures],['read-repairs',m.read_repairs],['sweep-repairs',m.sweep_repairs],
        ['hints stored',m.hints_stored],['hints replayed',m.hints_replayed],['compacts',m.compacts],
        ['prefix dels',m.prefix_deletes],['get ms',fmtMs(m.latency_get_ms)],['put ms',fmtMs(m.latency_put_ms)]];
      el('metrics').innerHTML=cells.map(function(c){
        return '<div class="metric"><div class="k">'+c[0]+'</div><div class="v">'+(c[1]==null?0:c[1])+'</div></div>';
      }).join('');
      var total=(m.gets||0)+(m.puts||0)+(m.dels||0)+(m.mutations||0)+(m.compacts||0);
      if(lastTotal!==null){
        hist.push(Math.max(0, total-lastTotal));
        if(hist.length>40) hist.shift();
        drawSpark();
      }
      lastTotal=total;
    }).catch(function(){el('healthy').textContent='gateway unreachable';el('healthy').className='pill bad';});
    fetchKeys();
    watchSelected();
    if(selected && selected.key) fetchRoute(selected.key);
  }
  function doPut(){var k=el('k').value,v=el('v').value;if(!k){return;}
    var hdrs={};
    var ttl=el('ttl').value;
    if(ttl!=='' && Number(ttl)>0){hdrs['X-KV-TTL']=String(Math.floor(Number(ttl)));}
    var cas=(el('cas').value||'').trim();
    if(cas!==''){hdrs['If-Match']=cas;}
    fetch('/kv/'+encodeURIComponent(k),{method:'PUT',body:v,headers:hdrs}).then(function(r){
      var detail='v'+(hdr(r,'Version')||'?')+' · acks '+(hdr(r,'Acks')||'?')+' · ['+(hdr(r,'Replicas')||'')+']';
      if(r.status===409) detail='conflict current v'+(hdr(r,'Version')||'?');
      msg('put '+k+' → '+r.status+' · '+detail);
      logOp('put', r.status, k+' '+detail);
      refresh();
    });}
  function doGet(){var k=el('k').value;if(!k){return;}
    fetch('/kv/'+encodeURIComponent(k)).then(function(r){return r.text().then(function(t){
      if(r.status===200){
        el('v').value=t;
        var ver=hdr(r,'Version');
        if(ver) el('cas').value=ver;
        var rep=hdr(r,'Repaired');var exp=hdr(r,'Expires-At');
        var detail='"'+t+'" · v'+(ver||'?')+' · responders ['+(hdr(r,'Responders')||'')+']'+(rep?' · repaired '+rep:'')+(exp?' · expires '+exp:'');
        msg('get '+k+' → '+detail);
        logOp('get', r.status, k+' v'+(ver||'?')+' '+t);
        if(selected && selected.key===k){
          if(watchVer!=null && ver && ver!==watchVer) flashInspect();
          selected.value=t; selected.version=ver||selected.version; watchVer=ver||watchVer;
          renderInspect();
        }
      }else{
        msg('get '+k+' → '+r.status+' '+t.trim());
        logOp('get', r.status, k+' '+t.trim());
      }
      refresh();
    });});}
  function doDel(){var k=el('k').value;if(!k){return;}
    fetch('/kv/'+encodeURIComponent(k),{method:'DELETE'}).then(function(r){
      msg('delete '+k+' → '+r.status+' · v'+(hdr(r,'Version')||'?'));
      logOp('del', r.status, k+' v'+(hdr(r,'Version')||'?'));
      if(selected && selected.key===k){selected=null;watchVer=null;renderInspect();}
      refresh();
    });}
  function doRoute(){var k=el('k').value;if(!k){return;}
    jget('/route/'+encodeURIComponent(k)).then(function(o){
      var d=(o.replicas||[]).join(', ');
      msg('route '+k+' → '+d);
      logOp('route', 200, k+' → '+d);
    });}
  function doRmPrefix(){
    var q=currentPrefix();
    var label=q?('prefix "'+q+'"'):'ALL keys';
    if(!confirm('Delete all keys matching '+label+'?')) return;
    fetch('/scan?prefix='+encodeURIComponent(q),{method:'DELETE'}).then(function(r){
      return r.json().then(function(j){
        var d='deleted '+(j.deleted||0)+(j.error?(' · '+j.error):'');
        msg('prefix delete → '+r.status+' · '+d);
        logOp('rm-prefix', r.status, d);
        selected=null; watchVer=null;
        refresh();
      }).catch(function(){msg('prefix delete → '+r.status);logOp('rm-prefix', r.status, '');});
    });
  }
  function doFault(addr, down){
    if(!addr) return;
    fetch('/admin/fault?addr='+encodeURIComponent(addr)+'&down='+down,{method:'POST'}).then(function(r){
      return r.json().then(function(j){
        var d=(j.addr||addr)+' forced='+(j.forced||j.down);
        msg('fault → '+r.status+' · '+d);
        logOp('fault', r.status, d);
        refresh();
      }).catch(function(){msg('fault → '+r.status);logOp('fault', r.status, addr);});
    });
  }
  function doCompact(){
    fetch('/admin/compact',{method:'POST'}).then(function(r){return r.json().then(function(j){
      var d='compacted '+(j.compacted||0)+(j.errors&&j.errors.length?(' · errs '+j.errors.join('; ')): '');
      msg('compact → '+r.status+' · '+d);
      logOp('compact', r.status, d);
      refresh();
    }).catch(function(){msg('compact → '+r.status);logOp('compact', r.status, '');});});}
  function doExport(){
    fetch('/export').then(function(r){return r.blob();}).then(function(b){
      var a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='kvstore-export.json';a.click();
      msg('exported JSON');logOp('export', 200, 'json');});}
  function doImport(file){
    file.text().then(function(t){
      return fetch('/import',{method:'POST',headers:{'Content-Type':'application/json'},body:t});
    }).then(function(r){return r.json().then(function(j){
      msg('import → '+r.status+' · imported '+(j.imported||0)+' · skipped '+(j.skipped||0));
      logOp('import', r.status, 'imported '+(j.imported||0));
      refresh();});});}
  function doAdd(){var a=el('naddr').value;if(!a){return;}
    fetch('/admin/nodes?addr='+encodeURIComponent(a),{method:'POST'}).then(function(r){return r.text().then(function(t){
      msg('add node '+a+' → '+r.status+' '+t.trim());
      logOp('add node', r.status, a);
      refresh();});});}
  function doRm(){var a=el('naddr').value;if(!a){return;}
    fetch('/admin/nodes/'+encodeURIComponent(a),{method:'DELETE'}).then(function(r){return r.text().then(function(t){
      msg('remove node '+a+' → '+r.status+' '+t.trim());
      logOp('rm node', r.status, a);
      refresh();});});}
  function cdo(delta){
    var k=el('ck').value;if(!k){return;}
    var u='/crdt/counter/'+encodeURIComponent(k)+(delta!=null?('?delta='+delta):'');
    fetch(u,{method:delta==null?'GET':'POST'}).then(function(r){return r.json();}).then(function(j){
      el('cmsg').textContent=k+' = '+(j.value!=null?j.value:'?');refresh();});
  }
  function sdo(kind){
    var k=el('sk').value,m=el('sm').value;if(!k){return;}
    var u='/crdt/set/'+encodeURIComponent(k);
    var method='GET';
    if(kind==='add'){method='POST';u+='?add='+encodeURIComponent(m);}
    if(kind==='rm'){method='POST';u+='?remove='+encodeURIComponent(m);}
    fetch(u,{method:method}).then(function(r){return r.json();}).then(function(j){
      el('smsg').textContent=k+' = ['+((j.members||[]).join(', '))+']';refresh();});
  }
  el('put').addEventListener('click',doPut);
  el('get').addEventListener('click',doGet);
  el('del').addEventListener('click',doDel);
  el('route').addEventListener('click',doRoute);
  el('compact').addEventListener('click',doCompact);
  el('rmprefix').addEventListener('click',doRmPrefix);
  el('nodes').addEventListener('click',function(ev){
    var btn=ev.target.closest('button.fault');
    if(!btn) return;
    doFault(btn.getAttribute('data-addr'), btn.getAttribute('data-down'));
  });
  el('export').addEventListener('click',doExport);
  el('importBtn').addEventListener('click',function(){el('importFile').click();});
  el('importFile').addEventListener('change',function(){if(this.files&&this.files[0]){doImport(this.files[0]);this.value='';}});
  el('filter').addEventListener('input',function(){
    renderKeys();
    if(filterTimer) clearTimeout(filterTimer);
    filterTimer=setTimeout(fetchKeys, 220);
  });
  el('hexbtn').addEventListener('click',function(){
    hexMode=!hexMode;
    el('hexbtn').textContent=hexMode?'values: hex':'values: utf-8';
    el('hexbtn').className=hexMode?'on':'ghost';
    renderKeys();renderInspect();
  });
  el('keys').addEventListener('click',function(ev){
    var tr=ev.target.closest('tr');
    if(!tr) return;
    var key=tr.getAttribute('data-key');
    if(!key) return;
    var row=null;
    for(var i=0;i<allRows.length;i++){if(allRows[i].key===key){row=allRows[i];break;}}
    if(row) selectRow(row, true);
  });
  el('iget').addEventListener('click',function(){if(selected){el('k').value=selected.key;doGet();}});
  el('idel').addEventListener('click',function(){if(selected){el('k').value=selected.key;doDel();}});
  el('iroute').addEventListener('click',function(){if(selected){el('k').value=selected.key;doRoute();}});
  el('addn').addEventListener('click',doAdd);
  el('rmn').addEventListener('click',doRm);
  el('cinc').addEventListener('click',function(){cdo(1);});
  el('cdec').addEventListener('click',function(){cdo(-1);});
  el('cread').addEventListener('click',function(){cdo(null);});
  el('sadd').addEventListener('click',function(){sdo('add');});
  el('srm').addEventListener('click',function(){sdo('rm');});
  el('sread').addEventListener('click',function(){sdo('read');});
  refresh();setInterval(refresh,1500);
})();
</script>
</body>
</html>`
