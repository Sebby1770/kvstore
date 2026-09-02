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
  .wrap{max-width:1080px;margin:0 auto;padding:28px 20px 64px}
  header.brand{display:flex;flex-wrap:wrap;gap:12px;align-items:baseline;margin-bottom:8px}
  h1{margin:0;font-size:22px;letter-spacing:.18em;text-transform:uppercase;color:var(--phosphor);
    text-shadow:0 0 18px rgba(154,230,180,.25);font-weight:650}
  .tag{color:var(--steel);font-size:12px;letter-spacing:.08em;text-transform:uppercase}
  .top{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin:6px 0 20px;color:var(--steel);font-size:13px}
  .pill{padding:3px 10px;border-radius:999px;font-size:11px;letter-spacing:.08em;text-transform:uppercase;border:1px solid var(--line)}
  .pill.ok{color:var(--phosphor);border-color:rgba(154,230,180,.35);background:rgba(154,230,180,.08)}
  .pill.bad{color:var(--bad);border-color:rgba(224,122,122,.4);background:rgba(224,122,122,.08)}
  h2{font-size:11px;color:var(--steel);font-weight:600;margin:0 0 10px;text-transform:uppercase;letter-spacing:1.4px}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px;margin-bottom:18px}
  .node{border:1px solid var(--line);border-radius:4px;padding:12px;background:var(--panel);position:relative}
  .node.down{opacity:.55}
  .node::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--phosphor)}
  .node.down::before{background:var(--amber)}
  .nh{display:flex;align-items:center;gap:8px;font-weight:600;font-size:13px;font-variant-numeric:tabular-nums}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--phosphor);box-shadow:0 0 8px var(--phosphor)}
  .node.down .dot{background:var(--amber);box-shadow:0 0 8px var(--amber)}
  .nm{margin-top:6px;font-size:12px;color:var(--steel);font-variant-numeric:tabular-nums;line-height:1.45}
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
  #msg{margin-top:10px;font-size:13px;color:var(--steel);font-variant-numeric:tabular-nums;min-height:18px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}
  th{color:var(--steel);font-weight:500;letter-spacing:.04em;text-transform:uppercase;font-size:11px}
  .muted{color:var(--dim)}
  .metrics{display:grid;grid-template-columns:repeat(auto-fill,minmax(118px,1fr));gap:8px;margin-bottom:10px}
  .metric{border:1px solid var(--line);border-radius:4px;padding:8px 10px;background:var(--panel)}
  .metric .k{font-size:10px;color:var(--steel);letter-spacing:.08em;text-transform:uppercase}
  .metric .v{font-size:18px;font-weight:600;color:var(--phosphor);font-variant-numeric:tabular-nums;margin-top:2px}
  .sparkwrap{display:flex;align-items:center;gap:10px;margin-bottom:18px;color:var(--steel);font-size:12px}
  #spark{display:block;background:#0b0d10;border:1px solid var(--line);border-radius:3px}
  input#ttl,input#naddr{width:130px}
  input#filter{min-width:180px}
  .split{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  @media(max-width:800px){.split{grid-template-columns:1fr}}
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
      <button id="put">put</button>
      <button id="get" class="ghost">get</button>
      <button id="del" class="ghost">delete</button>
      <button id="route" class="ghost">route</button>
    </div>
    <div class="row" style="margin-top:8px">
      <input id="naddr" placeholder="host:port" />
      <button id="addn" class="ghost">add node</button>
      <button id="rmn" class="amber">remove node</button>
      <button id="export" class="ghost">export JSON</button>
      <button id="importBtn" class="ghost">import JSON</button>
      <input id="importFile" type="file" accept="application/json,.json" hidden />
    </div>
    <div id="msg"></div>
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

  <div class="panel">
    <h2>key browser</h2>
    <div class="row" style="margin-bottom:10px">
      <input id="filter" placeholder="search / filter keys…" />
    </div>
    <table><thead><tr><th>key</th><th>value</th><th>version</th><th>replicas</th><th>expires</th></tr></thead>
    <tbody id="keys"></tbody></table>
  </div>
</div>
<script>
(function(){
  function el(id){return document.getElementById(id);}
  function esc(t){var d=document.createElement('div');d.textContent=(t==null?'':String(t));return d.innerHTML;}
  function jget(u){return fetch(u).then(function(r){return r.json();});}
  function msg(t){el('msg').textContent=t;}
  var hist=[];
  var lastTotal=null;
  var allRows=[];
  function expLabel(e){
    if(!e || !e.expires_at){return '—';}
    var sec=e.expires_at-Math.floor(Date.now()/1000);
    if(sec<=0){return 'expired';}
    if(sec<60){return sec+'s';}
    if(sec<3600){return Math.floor(sec/60)+'m '+ (sec%60)+'s';}
    return Math.floor(sec/3600)+'h';
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
  function renderKeys(){
    var q=(el('filter').value||'').toLowerCase();
    var shown=allRows.filter(function(e){return !q || String(e.key).toLowerCase().indexOf(q)!==-1;});
    var rows=shown.map(function(e){
      return '<tr><td>'+esc(e.key)+'</td><td>'+esc(e.value)+'</td><td>v'+e.version+'</td><td>'+e.replicas+'</td><td>'+esc(expLabel(e))+'</td></tr>';
    }).join('');
    el('keys').innerHTML=rows||'<tr><td colspan="5" class="muted">'+(allRows.length?'no keys match filter':'no keys yet')+'</td></tr>';
  }
  function refresh(){
    jget('/health').then(function(h){
      el('cfg').textContent='RF '+h.rf+' · R '+h.read+' · W '+h.write+(h.consistent?' · consistent':' · NOT consistent');
      var hb=el('healthy');hb.textContent=h.healthy?'all nodes up':'degraded';hb.className='pill '+(h.healthy?'ok':'bad');
      el('nodes').innerHTML=(h.nodes||[]).map(function(n){
        var down=n.down || !n.up;
        var last=n.last_ok?(' · last ok '+String(n.last_ok).replace('T',' ').replace('Z','')):'';
        return '<div class="node '+(down?'down':'up')+'"><div class="nh"><span class="dot"></span>'+esc(n.addr)+'</div>'+
          '<div class="nm">'+(n.up?('keys '+n.count+' · max v'+n.max_version+(n.tombstones?(' · tombs '+n.tombstones):'')+last):('offline'+last))+'</div></div>';
      }).join('');
      var m=h.metrics||{};
      var cells=[['reads',m.gets],['writes',m.puts],['deletes',m.dels],['crdt ops',m.mutations],
        ['quorum fails',m.quorum_failures],['read-repairs',m.read_repairs],['sweep-repairs',m.sweep_repairs],
        ['hints stored',m.hints_stored],['hints replayed',m.hints_replayed]];
      el('metrics').innerHTML=cells.map(function(c){
        return '<div class="metric"><div class="k">'+c[0]+'</div><div class="v">'+(c[1]||0)+'</div></div>';
      }).join('');
      var total=(m.gets||0)+(m.puts||0)+(m.dels||0)+(m.mutations||0);
      if(lastTotal!==null){
        hist.push(Math.max(0, total-lastTotal));
        if(hist.length>40) hist.shift();
        drawSpark();
      }
      lastTotal=total;
    }).catch(function(){el('healthy').textContent='gateway unreachable';el('healthy').className='pill bad';});
    jget('/scan').then(function(s){
      allRows=s.entries||[];
      renderKeys();
    }).catch(function(){});
  }
  function doPut(){var k=el('k').value,v=el('v').value;if(!k){return;}
    var hdr={};
    var ttl=el('ttl').value;
    if(ttl!=='' && Number(ttl)>0){hdr['X-KV-TTL']=String(Math.floor(Number(ttl)));}
    fetch('/kv/'+encodeURIComponent(k),{method:'PUT',body:v,headers:hdr}).then(function(r){
      msg('put '+k+' → '+r.status+' · v'+(r.headers.get('X-KV-Version')||r.headers.get('X-Triglot-Version')||'?')+' · acks '+(r.headers.get('X-KV-Acks')||'?')+' · ['+(r.headers.get('X-KV-Replicas')||'')+']');refresh();});}
  function doGet(){var k=el('k').value;if(!k){return;}
    fetch('/kv/'+encodeURIComponent(k)).then(function(r){return r.text().then(function(t){
      if(r.status===200){var rep=r.headers.get('X-KV-Repaired')||r.headers.get('X-Triglot-Repaired');var exp=r.headers.get('X-KV-Expires-At');
        msg('get '+k+' → "'+t+'" · v'+(r.headers.get('X-KV-Version')||'?')+' · responders ['+(r.headers.get('X-KV-Responders')||'')+']'+(rep?' · read-repaired '+rep:'')+(exp?' · expires '+exp:''));}
      else{msg('get '+k+' → '+r.status+' '+t.trim());}refresh();});});}
  function doDel(){var k=el('k').value;if(!k){return;}
    fetch('/kv/'+encodeURIComponent(k),{method:'DELETE'}).then(function(r){msg('delete '+k+' → '+r.status+' · v'+(r.headers.get('X-KV-Version')||'?'));refresh();});}
  function doRoute(){var k=el('k').value;if(!k){return;}
    jget('/route/'+encodeURIComponent(k)).then(function(o){msg('route '+k+' → '+(o.replicas||[]).join(', '));});}
  function doExport(){
    fetch('/export').then(function(r){return r.blob();}).then(function(b){
      var a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='kvstore-export.json';a.click();
      msg('exported JSON');});}
  function doImport(file){
    file.text().then(function(t){
      return fetch('/import',{method:'POST',headers:{'Content-Type':'application/json'},body:t});
    }).then(function(r){return r.json().then(function(j){
      msg('import → '+r.status+' · imported '+(j.imported||0)+' · skipped '+(j.skipped||0));
      refresh();});});}
  function doAdd(){var a=el('naddr').value;if(!a){return;}
    fetch('/admin/nodes?addr='+encodeURIComponent(a),{method:'POST'}).then(function(r){return r.text().then(function(t){msg('add node '+a+' → '+r.status+' '+t.trim());refresh();});});}
  function doRm(){var a=el('naddr').value;if(!a){return;}
    fetch('/admin/nodes/'+encodeURIComponent(a),{method:'DELETE'}).then(function(r){return r.text().then(function(t){msg('remove node '+a+' → '+r.status+' '+t.trim());refresh();});});}
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
  el('export').addEventListener('click',doExport);
  el('importBtn').addEventListener('click',function(){el('importFile').click();});
  el('importFile').addEventListener('change',function(){if(this.files&&this.files[0]){doImport(this.files[0]);this.value='';}});
  el('filter').addEventListener('input',renderKeys);
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
