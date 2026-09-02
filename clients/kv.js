// Tiny KVStore HTTP client. No dependencies.
// Browser: KVStore.createClient(base). Node: require('./kv.js').

function buildUrl(base, path, query) {
  var root = String(base == null ? '' : base).replace(/\/+$/, '');
  var p = path == null ? '' : String(path);
  if (p && p.charAt(0) !== '/') p = '/' + p;
  var u = root + p;
  if (query && typeof query === 'object') {
    var parts = [];
    for (var k in query) {
      if (!Object.prototype.hasOwnProperty.call(query, k)) continue;
      var v = query[k];
      if (v == null || v === '') continue;
      parts.push(encodeURIComponent(k) + '=' + encodeURIComponent(String(v)));
    }
    if (parts.length) u += (u.indexOf('?') >= 0 ? '&' : '?') + parts.join('&');
  }
  return u;
}

function spec(op, opts) {
  opts = opts || {};
  var base = opts.base || '';
  var key = opts.key;
  switch (op) {
    case 'get':
      return { method: 'GET', url: buildUrl(base, '/kv/' + encodeURIComponent(key)) };
    case 'put':
      return { method: 'PUT', url: buildUrl(base, '/kv/' + encodeURIComponent(key)) };
    case 'del':
      return { method: 'DELETE', url: buildUrl(base, '/kv/' + encodeURIComponent(key)) };
    case 'scan':
      return { method: 'GET', url: buildUrl(base, '/scan', opts.prefix ? { prefix: opts.prefix } : undefined) };
    case 'health':
      return { method: 'GET', url: buildUrl(base, '/health') };
    default:
      throw new Error('unknown op: ' + op);
  }
}

function createClient(base, fetchFn) {
  var f = fetchFn || (typeof fetch === 'function' ? fetch : null);
  function send(s, init) {
    if (!f) return Promise.reject(new Error('fetch is not available'));
    var opts = init || {};
    opts.method = s.method;
    return f(s.url, opts);
  }
  return {
    get: function (key) {
      return send(spec('get', { base: base, key: key }));
    },
    put: function (key, val, headers) {
      return send(spec('put', { base: base, key: key }), { body: val, headers: headers || {} });
    },
    del: function (key) {
      return send(spec('del', { base: base, key: key }));
    },
    scan: function (prefix) {
      return send(spec('scan', { base: base, prefix: prefix })).then(function (r) { return r.json(); });
    },
    health: function () {
      return send(spec('health', { base: base })).then(function (r) { return r.json(); });
    }
  };
}

var api = { buildUrl: buildUrl, spec: spec, createClient: createClient };
if (typeof module !== 'undefined' && module.exports) {
  module.exports = api;
}
if (typeof globalThis !== 'undefined') {
  globalThis.KVStore = api;
}
