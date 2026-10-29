package node

// meshboardHTML is the mesh control room served at GET /mesh. Like the KV
// dashboard it is one self-contained page (no build step, no dependencies) that
// polls the node's own JSON: /mesh/status for membership, consensus, transport,
// actors and the event log, plus the lease / job / actor endpoints for the
// controls. Any node serves it; every node shows the cluster from its own seat.
const meshboardHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>KVStore Mesh</title>
<style>
  :root{
    --bg:#0b0d10; --panel:#12151b; --line:#1c2330;
    --phosphor:#9ae6b4; --amber:#e6b35a; --steel:#8b9bb4;
    --ink:#d5ddd8; --dim:#6d7b88; --bad:#e07a7a; --cyan:#7cc7e8;
  }
  *{box-sizing:border-box}
  html,body{margin:0;background:var(--bg);color:var(--ink);
    font-family:"IBM Plex Sans","Segoe UI",system-ui,sans-serif;}
  body{min-height:100vh;
    background-image:linear-gradient(rgba(139,155,180,.045) 1px,transparent 1px),
      linear-gradient(90deg,rgba(139,155,180,.045) 1px,transparent 1px);
    background-size:28px 28px;}
  .wrap{max-width:1240px;margin:0 auto;padding:28px 20px 64px}
  header.brand{display:flex;flex-wrap:wrap;gap:12px;align-items:baseline;margin-bottom:6px}
  h1{margin:0;font-size:22px;letter-spacing:.18em;text-transform:uppercase;color:var(--phosphor);
    text-shadow:0 0 18px rgba(154,230,180,.25);font-weight:650}
  .tag{color:var(--steel);font-size:12px;letter-spacing:.08em;text-transform:uppercase}
  a{color:var(--cyan)}
  .top{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:8px 0 18px;color:var(--steel);font-size:13px}
  .pill{padding:3px 10px;border-radius:999px;font-size:11px;letter-spacing:.08em;text-transform:uppercase;
    border:1px solid var(--line);white-space:nowrap}
  .pill.ok{color:var(--phosphor);border-color:rgba(154,230,180,.35);background:rgba(154,230,180,.08)}
  .pill.warn{color:var(--amber);border-color:rgba(230,179,90,.4);background:rgba(230,179,90,.08)}
  .pill.bad{color:var(--bad);border-color:rgba(224,122,122,.4);background:rgba(224,122,122,.08)}
  .pill.info{color:var(--cyan);border-color:rgba(124,199,232,.35);background:rgba(124,199,232,.07)}
  h2{font-size:11px;color:var(--steel);font-weight:600;margin:0 0 10px;text-transform:uppercase;letter-spacing:1.4px}
  .panel{border:1px solid var(--line);border-radius:4px;padding:14px;background:var(--panel);margin-bottom:16px;min-width:0}
  .cols{display:grid;grid-template-columns:1.05fr 1fr;gap:16px}
  .cols3{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
  #topo{display:block;width:100%;height:auto;background:#0b0d10;border:1px solid var(--line);border-radius:3px}
  table{width:100%;border-collapse:collapse;font-size:12.5px}
  th,td{text-align:left;padding:6px 7px;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums;
    overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:220px}
  th{color:var(--steel);font-weight:500;letter-spacing:.04em;text-transform:uppercase;font-size:10.5px}
  .muted{color:var(--dim)}
  .st-alive{color:var(--phosphor)} .st-suspect{color:var(--amber)} .st-dead,.st-left{color:var(--bad)}
  .bar{height:6px;background:#0b0d10;border:1px solid var(--line);border-radius:2px;overflow:hidden;min-width:60px}
  .bar>i{display:block;height:100%;background:linear-gradient(90deg,var(--phosphor),var(--cyan))}
  .kv{display:grid;grid-template-columns:repeat(auto-fill,minmax(112px,1fr));gap:8px;margin-bottom:12px}
  .metric{border:1px solid var(--line);border-radius:4px;padding:8px 10px;background:#0f1217}
  .metric .k{font-size:10px;color:var(--steel);letter-spacing:.08em;text-transform:uppercase}
  .metric .v{font-size:17px;font-weight:600;color:var(--phosphor);font-variant-numeric:tabular-nums;margin-top:2px;
    overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-top:8px}
  input{background:#0b0d10;color:var(--ink);border:1px solid var(--line);border-radius:3px;padding:7px 9px;
    font:inherit;font-size:13px;min-width:0}
  input:focus{outline:1px solid var(--phosphor);border-color:var(--phosphor)}
  button{background:transparent;color:var(--phosphor);border:1px solid rgba(154,230,180,.45);border-radius:3px;
    padding:7px 12px;font:inherit;font-size:11.5px;letter-spacing:.06em;text-transform:uppercase;cursor:pointer}
  button:hover{background:rgba(154,230,180,.1)}
  button.ghost{color:var(--steel);border-color:var(--line)}
  button.amber{color:var(--amber);border-color:rgba(230,179,90,.45)}
  .out{margin-top:8px;font-size:12.5px;color:var(--steel);font-variant-numeric:tabular-nums;min-height:17px;word-break:break-all}
  #events{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;background:#0b0d10;
    border:1px solid var(--line);border-radius:3px;padding:8px 10px;max-height:260px;overflow:auto;line-height:1.55;
    color:var(--steel);white-space:pre}
  #events .k-swim{color:var(--cyan)} #events .k-raft{color:var(--phosphor)} #events .k-ring{color:var(--amber)}
  #events .k-actor{color:#c5a3ef} #events .k-bad{color:var(--bad)}
  .scroll{overflow-x:auto}
  @media(max-width:900px){
    .cols,.cols3{grid-template-columns:1fr}
    input,button{min-height:42px;font-size:15px}
    .wrap{padding:16px 12px 48px}
  }
</style>
</head>
<body>
<div class="wrap">
  <header class="brand">
    <h1>KVStore Mesh</h1>
    <span class="tag">distributed runtime · KTP transport · SWIM · Raft · virtual actors</span>
    <span style="margin-left:auto" class="tag"><a href="/dashboard">kv control room →</a></span>
  </header>
  <div class="top" id="top"><span class="muted">connecting…</span></div>

  <div class="cols">
    <div class="panel">
      <h2>topology · gossip view from this node</h2>
      <svg id="topo" viewBox="0 0 520 330" aria-label="cluster topology"></svg>
      <div class="out muted" id="topocap">edges are live KTP connections, labelled with smoothed RTT</div>
    </div>
    <div class="panel">
      <h2>consensus · control plane</h2>
      <div class="kv" id="raftkv"></div>
      <div class="scroll"><table><thead><tr><th>peer</th><th>role</th><th>match</th><th>next</th><th>state</th><th>replication</th></tr></thead>
        <tbody id="progress"></tbody></table></div>
    </div>
  </div>

  <div class="panel">
    <h2>members · SWIM</h2>
    <div class="scroll"><table><thead><tr><th>name</th><th>address</th><th>state</th><th>incarnation</th><th>raft</th><th>http</th><th>since</th></tr></thead>
      <tbody id="members"></tbody></table></div>
  </div>

  <div class="panel">
    <h2>transport · KTP connections from this node</h2>
    <div class="scroll"><table><thead><tr><th>peer</th><th>srtt</th><th>min rtt</th><th>cwnd</th><th>in flight</th><th>sent</th><th>lost</th><th>retransmits</th><th>streams</th><th>cc</th></tr></thead>
      <tbody id="conns"></tbody></table></div>
  </div>

  <div class="cols3">
    <div class="panel">
      <h2>leases · fencing tokens</h2>
      <div class="row" style="margin-top:0">
        <input id="lname" placeholder="lease" value="leader-election" size="12" />
        <input id="lholder" placeholder="holder" value="worker-a" size="8" />
        <input id="lttl" placeholder="ttl" value="10s" size="4" />
      </div>
      <div class="row">
        <button id="lacq">acquire</button>
        <button id="lren" class="ghost">renew</button>
        <button id="lrel" class="amber">release</button>
      </div>
      <div class="out" id="lout"></div>
      <div class="scroll"><table><thead><tr><th>name</th><th>holder</th><th>token</th><th>expires</th></tr></thead>
        <tbody id="leases"></tbody></table></div>
    </div>
    <div class="panel">
      <h2>job queues · at-least-once</h2>
      <div class="row" style="margin-top:0">
        <input id="jq" placeholder="queue" value="emails" size="8" />
        <input id="jp" placeholder="payload" value="hello" size="10" />
      </div>
      <div class="row">
        <button id="jenq">enqueue</button>
        <button id="jclaim" class="ghost">claim</button>
        <button id="jdone" class="ghost">complete</button>
        <button id="jfail" class="amber">fail</button>
      </div>
      <div class="out" id="jout"></div>
      <div class="scroll"><table><thead><tr><th>queue</th><th>ready</th><th>in flight</th><th>done</th><th>dead</th></tr></thead>
        <tbody id="queues"></tbody></table></div>
    </div>
    <div class="panel">
      <h2>virtual actors · on this node</h2>
      <div class="row" style="margin-top:0">
        <input id="atype" placeholder="type" value="counter" size="7" />
        <input id="akey" placeholder="key" value="visits" size="8" />
        <input id="amsg" placeholder="message" value="inc 1" size="7" />
        <button id="aask">ask</button>
      </div>
      <div class="out" id="aout"></div>
      <div class="scroll"><table><thead><tr><th>actor</th><th>messages</th><th>age</th></tr></thead>
        <tbody id="actors"></tbody></table></div>
    </div>
  </div>

  <div class="panel">
    <h2>event log · hybrid-logical-clock ordered</h2>
    <div id="events" class="muted">waiting for events…</div>
  </div>
</div>
<script>
(function(){
  function el(id){return document.getElementById(id);}
  function esc(t){var d=document.createElement('div');d.textContent=(t==null?'':String(t));return d.innerHTML;}
  function num(v){var n=Number(v);return isFinite(n)?n:0;}
  function ms(v){var n=num(v);return n>=100?n.toFixed(0)+' ms':n>=10?n.toFixed(1)+' ms':n.toFixed(2)+' ms';}
  function bytes(v){var n=num(v);if(n>=1<<20)return (n/(1<<20)).toFixed(1)+' MiB';if(n>=1024)return (n/1024).toFixed(1)+' KiB';return n+' B';}
  function ago(iso){if(!iso)return '—';var t=Date.parse(iso);if(!t)return '—';var s=Math.max(0,Math.round((Date.now()-t)/1000));
    if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m';return Math.floor(s/3600)+'h';}
  function untilNs(unixNanos){if(!unixNanos)return '—';var s=Math.round((num(unixNanos)/1e6-Date.now())/1000);return s<=0?'expired':s+'s';}
  function post(u,body){return fetch(u,{method:'POST',body:body||''}).then(function(r){
    return r.text().then(function(t){var j=null;try{j=JSON.parse(t);}catch(e){}return {status:r.status,json:j,text:t};});});}
  var lastJob=null;
  var status=null;

  function renderTop(s){
    var n=s.node||{}, r=s.raft||{}, ring=(s.control||{}).ring||{};
    var role=(r.role||'—').toLowerCase();
    var cls=role==='leader'?'ok':role==='follower'?'info':role==='candidate'||role==='precandidate'?'warn':'';
    var alive=(s.members||[]).filter(function(m){return m.state==='alive';}).length;
    el('top').innerHTML=[
      '<span class="pill ok">'+esc(n.name||'?')+'</span>',
      '<span class="muted">'+esc(n.addr||'')+'</span>',
      '<span class="pill '+cls+'">raft '+esc(role)+'</span>',
      '<span class="pill">term '+esc(r.term||0)+'</span>',
      '<span class="pill info">leader '+esc(r.leader||'none')+'</span>',
      '<span class="pill '+(alive===(s.members||[]).length?'ok':'warn')+'">'+alive+'/'+(s.members||[]).length+' alive</span>',
      '<span class="pill">ring epoch '+esc(ring.epoch||0)+'</span>',
      '<span class="muted" style="margin-left:auto">hlc '+esc(n.hlc||'')+'</span>'
    ].join('');
  }

  function renderTopo(s){
    var svg=el('topo'), W=520, H=330, cx=W/2, cy=H/2+4, R=118;
    var members=(s.members||[]).slice().sort(function(a,b){return a.name<b.name?-1:1;});
    var me=(s.node||{}).name, leader=(s.raft||{}).leader;
    var pos={};
    members.forEach(function(m,i){
      var a=-Math.PI/2+i*2*Math.PI/Math.max(1,members.length);
      pos[m.name]={x:cx+R*Math.cos(a),y:cy+R*Math.sin(a)};
    });
    var parts=[];
    (s.transport||[]).forEach(function(c){
      var p=pos[c.peer], q=pos[me];
      if(!p||!q) return;
      var mx=(p.x+q.x)/2, my=(p.y+q.y)/2;
      parts.push('<line x1="'+q.x.toFixed(1)+'" y1="'+q.y.toFixed(1)+'" x2="'+p.x.toFixed(1)+'" y2="'+p.y.toFixed(1)+
        '" stroke="#7cc7e8" stroke-opacity=".55" stroke-width="1.4" stroke-dasharray="4 4">'+
        '<animate attributeName="stroke-dashoffset" from="16" to="0" dur="1.2s" repeatCount="indefinite"/></line>');
      parts.push('<text x="'+mx.toFixed(1)+'" y="'+(my-4).toFixed(1)+'" fill="#8b9bb4" font-size="10" text-anchor="middle">'+esc(ms(c.srtt_ms))+'</text>');
    });
    members.forEach(function(m){
      var p=pos[m.name];
      var col=m.state==='alive'?'#9ae6b4':m.state==='suspect'?'#e6b35a':'#e07a7a';
      if(m.name===leader){
        parts.push('<circle cx="'+p.x.toFixed(1)+'" cy="'+p.y.toFixed(1)+'" r="21" fill="none" stroke="#9ae6b4" stroke-opacity=".5" stroke-width="1.2">'+
          '<animate attributeName="r" values="18;25;18" dur="2.4s" repeatCount="indefinite"/></circle>');
      }
      parts.push('<circle cx="'+p.x.toFixed(1)+'" cy="'+p.y.toFixed(1)+'" r="13" fill="#12151b" stroke="'+col+'" stroke-width="'+(m.name===me?3:1.6)+'"/>');
      parts.push('<text x="'+p.x.toFixed(1)+'" y="'+(p.y+4).toFixed(1)+'" fill="'+col+'" font-size="10.5" text-anchor="middle" font-weight="600">'+esc(m.name.slice(0,4))+'</text>');
      var ly=p.y>cy?p.y+30:p.y-21;
      parts.push('<text x="'+p.x.toFixed(1)+'" y="'+ly.toFixed(1)+'" fill="#6d7b88" font-size="9.5" text-anchor="middle">'+esc(m.state+(m.name===leader?' · leader':''))+'</text>');
    });
    if(!members.length) parts.push('<text x="20" y="30" fill="#6d7b88" font-size="12">no members yet</text>');
    svg.innerHTML=parts.join('');
  }

  function renderRaft(s){
    var r=s.raft||{}, ctl=s.control||{}, ring=ctl.ring||{};
    var cells=[['term',r.term],['commit',r.commit],['applied',r.applied],['voters',(r.voters||[]).length],
      ['learners',(r.learners||[]).length],['ring nodes',(ring.nodes||[]).length],['rf/r/w',(ring.rf||0)+'/'+(ring.r||0)+'/'+(ring.w||0)],
      ['leases',ctl.leases||0]];
    el('raftkv').innerHTML=cells.map(function(c){return '<div class="metric"><div class="k">'+c[0]+'</div><div class="v">'+esc(c[1]==null?0:c[1])+'</div></div>';}).join('');
    var prog=r.progress||{}, names=Object.keys(prog).sort(), last=num(r.last_index||r.commit);
    var voters={};(r.voters||[]).forEach(function(v){voters[v]=1;});
    el('progress').innerHTML=names.map(function(id){
      var p=prog[id]||{}, pct=last?Math.max(0,Math.min(100,Math.round(num(p.match)/last*100))):0;
      return '<tr><td>'+esc(id)+'</td><td>'+(voters[id]?'voter':'learner')+'</td><td>'+esc(p.match)+'</td><td>'+esc(p.next)+'</td><td>'+esc(p.state||'')+
        '</td><td><div class="bar"><i style="width:'+pct+'%"></i></div></td></tr>';
    }).join('')||'<tr><td colspan="6" class="muted">'+(r.role==='leader'?'no peers':'replication progress is tracked by the leader ('+esc(r.leader||'none')+')')+'</td></tr>';
  }

  function renderMembers(s){
    var r=s.raft||{}, voters={}, learners={};
    (r.voters||[]).forEach(function(v){voters[v]=1;});(r.learners||[]).forEach(function(v){learners[v]=1;});
    el('members').innerHTML=(s.members||[]).map(function(m){
      var meta=m.meta||{};
      var http=meta.http?'<a href="http://'+esc(meta.http)+'/mesh">'+esc(meta.http)+'</a>':'—';
      return '<tr><td>'+esc(m.name)+'</td><td>'+esc(m.addr)+'</td><td class="st-'+esc(m.state)+'">'+esc(m.state)+'</td><td>'+esc(m.incarnation)+
        '</td><td>'+(voters[m.name]?'voter':learners[m.name]?'learner':'—')+'</td><td>'+http+'</td><td>'+esc(ago(m.since))+'</td></tr>';
    }).join('')||'<tr><td colspan="7" class="muted">no members</td></tr>';
  }

  function renderConns(s){
    el('conns').innerHTML=(s.transport||[]).map(function(c){
      return '<tr><td>'+esc(c.peer||c.addr)+'</td><td>'+ms(c.srtt_ms)+'</td><td>'+ms(c.min_rtt_ms)+'</td><td>'+bytes(c.cwnd)+'</td><td>'+bytes(c.bytes_in_flight)+
        '</td><td>'+esc(c.packets_sent)+'</td><td>'+esc(c.packets_lost)+'</td><td>'+esc(c.retransmits)+'</td><td>'+esc(c.streams)+'</td><td>'+esc(c.congestion||'')+'</td></tr>';
    }).join('')||'<tr><td colspan="10" class="muted">no connections yet</td></tr>';
  }

  function renderControl(s){
    var ctl=s.control||{};
    el('leases').innerHTML=(ctl.lease_list||[]).map(function(l){
      return '<tr><td>'+esc(l.name)+'</td><td>'+esc(l.holder)+'</td><td>'+esc(l.token)+'</td><td>'+esc(untilNs(l.expires_at))+'</td></tr>';
    }).join('')||'<tr><td colspan="4" class="muted">no leases</td></tr>';
    var qs=ctl.queues||{}, names=Object.keys(qs).sort();
    el('queues').innerHTML=names.map(function(q){var x=qs[q]||{};
      return '<tr><td>'+esc(q)+'</td><td>'+esc(x.ready||0)+'</td><td>'+esc(x.in_flight||0)+'</td><td>'+esc(x.completed||0)+'</td><td>'+esc(x.dead||0)+'</td></tr>';
    }).join('')||'<tr><td colspan="5" class="muted">no queues</td></tr>';
  }

  function renderActors(s){
    var a=s.actors||{};
    el('actors').innerHTML=(a.activations||[]).map(function(x){
      return '<tr><td>'+esc(x.type+'/'+x.key)+'</td><td>'+esc(x.messages||0)+'</td><td>'+esc(ago(x.activated))+'</td></tr>';
    }).join('')||'<tr><td colspan="3" class="muted">no activations here</td></tr>';
  }

  function renderEvents(s){
    var ev=(s.events||[]).slice().reverse();
    el('events').innerHTML=ev.map(function(e){
      var k=String(e.kind||''), cls='k-'+k.split('.')[0];
      if(/dead|fail|lost|stale/.test(k)) cls='k-bad';
      return '<span class="'+cls+'">'+esc(e.hlc||'')+'  '+esc(k.padEnd(18))+'  '+esc(e.detail||'')+'</span>';
    }).join('\n')||'<span class="muted">no events yet</span>';
  }

  function refresh(){
    fetch('/mesh/status').then(function(r){return r.json();}).then(function(s){
      status=s;
      renderTop(s);renderTopo(s);renderRaft(s);renderMembers(s);renderConns(s);renderControl(s);renderActors(s);renderEvents(s);
    }).catch(function(){el('top').innerHTML='<span class="pill bad">node unreachable</span>';});
  }

  function lease(kind){
    var n=el('lname').value, h=el('lholder').value, ttl=el('lttl').value;
    if(!n) return;
    var u='/lease/'+encodeURIComponent(n)+'?holder='+encodeURIComponent(h)+'&ttl='+encodeURIComponent(ttl);
    var tok=el('lout').getAttribute('data-token');
    if(tok) u+='&token='+encodeURIComponent(tok);
    var p=kind==='release'?fetch(u,{method:'DELETE'}).then(function(r){return r.text().then(function(t){var j=null;try{j=JSON.parse(t);}catch(e){}return {status:r.status,json:j,text:t};});})
      :post(u+(kind==='renew'?'&renew=1':''));
    p.then(function(res){
      var j=res.json||{};
      if(j.token) el('lout').setAttribute('data-token', j.token);
      el('lout').textContent=kind+' '+n+' → '+res.status+(j.token?' · fencing token '+j.token:'')+(j.error?' · '+j.error:'')+(j.holder?' · holder '+j.holder:'');
      refresh();
    });
  }
  function job(kind){
    var q=el('jq').value; if(!q) return;
    var base='/jobs/'+encodeURIComponent(q);
    var p;
    if(kind==='enqueue') p=post(base, el('jp').value);
    else if(kind==='claim') p=post(base+'/claim?worker=dashboard&visibility=30s');
    else {
      if(!lastJob){el('jout').textContent='claim a job first';return;}
      p=post(base+'/'+encodeURIComponent(lastJob.id)+'/'+kind+'?token='+encodeURIComponent(lastJob.token)+(kind==='fail'?'&error=manual':''));
    }
    p.then(function(res){
      var j=res.json||{};
      if(kind==='claim'&&j.id){lastJob={id:j.id,token:j.token};}
      if(kind==='complete'||kind==='fail'){lastJob=null;}
      el('jout').textContent=kind+' → '+res.status+(j.id?' · job '+j.id:'')+(j.token?' · token '+j.token:'')+(j.attempt?' · attempt '+j.attempt:'')+
        (j.payload!=null?' · "'+j.payload+'"':'')+(j.error?' · '+j.error:'');
      refresh();
    });
  }
  function ask(){
    var t=el('atype').value,k=el('akey').value; if(!t||!k) return;
    post('/actor/'+encodeURIComponent(t)+'/'+encodeURIComponent(k), el('amsg').value).then(function(res){
      el('aout').textContent=t+'/'+k+' → '+res.status+' · '+(res.json&&res.json.reply!=null?res.json.reply:res.text.trim())+
        (res.json&&res.json.node?' · served by '+res.json.node:'');
      refresh();
    });
  }
  el('lacq').addEventListener('click',function(){lease('acquire');});
  el('lren').addEventListener('click',function(){lease('renew');});
  el('lrel').addEventListener('click',function(){lease('release');});
  el('jenq').addEventListener('click',function(){job('enqueue');});
  el('jclaim').addEventListener('click',function(){job('claim');});
  el('jdone').addEventListener('click',function(){job('complete');});
  el('jfail').addEventListener('click',function(){job('fail');});
  el('aask').addEventListener('click',ask);
  refresh();setInterval(refresh,1200);
})();
</script>
</body>
</html>`
