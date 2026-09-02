'use strict';

var assert = require('assert');
var kv = require('./kv.js');

var base = 'http://127.0.0.1:8090';

assert.strictEqual(kv.buildUrl(base, '/health'), 'http://127.0.0.1:8090/health');
assert.strictEqual(kv.buildUrl(base + '/', '/kv/hello'), 'http://127.0.0.1:8090/kv/hello');
assert.strictEqual(kv.buildUrl(base, '/scan', { prefix: 'a' }), 'http://127.0.0.1:8090/scan?prefix=a');
assert.strictEqual(kv.buildUrl(base, '/scan', { prefix: 'a b' }), 'http://127.0.0.1:8090/scan?prefix=a%20b');
assert.strictEqual(kv.buildUrl(base, '/scan'), 'http://127.0.0.1:8090/scan');
assert.strictEqual(kv.buildUrl(base, '/scan', { prefix: '' }), 'http://127.0.0.1:8090/scan');

var get = kv.spec('get', { base: base, key: 'hello' });
assert.strictEqual(get.method, 'GET');
assert.strictEqual(get.url, 'http://127.0.0.1:8090/kv/hello');

var put = kv.spec('put', { base: base, key: 'hello' });
assert.strictEqual(put.method, 'PUT');
assert.strictEqual(put.url, 'http://127.0.0.1:8090/kv/hello');

var del = kv.spec('del', { base: base, key: 'hello' });
assert.strictEqual(del.method, 'DELETE');
assert.strictEqual(del.url, 'http://127.0.0.1:8090/kv/hello');

var scan = kv.spec('scan', { base: base, prefix: 'aa' });
assert.strictEqual(scan.method, 'GET');
assert.strictEqual(scan.url, 'http://127.0.0.1:8090/scan?prefix=aa');

var scanAll = kv.spec('scan', { base: base });
assert.strictEqual(scanAll.method, 'GET');
assert.strictEqual(scanAll.url, 'http://127.0.0.1:8090/scan');

var health = kv.spec('health', { base: base });
assert.strictEqual(health.method, 'GET');
assert.strictEqual(health.url, 'http://127.0.0.1:8090/health');

var calls = [];
var client = kv.createClient(base, function (url, opts) {
  calls.push({ url: url, method: opts && opts.method, body: opts && opts.body });
  return Promise.resolve({ json: function () { return Promise.resolve({ ok: true, url: url }); } });
});
assert.strictEqual(typeof client.get, 'function');
assert.strictEqual(typeof client.put, 'function');
assert.strictEqual(typeof client.del, 'function');
assert.strictEqual(typeof client.scan, 'function');
assert.strictEqual(typeof client.health, 'function');

Promise.resolve()
  .then(function () { return client.get('k'); })
  .then(function () { return client.put('k', 'v'); })
  .then(function () { return client.del('k'); })
  .then(function () { return client.scan('pre'); })
  .then(function () { return client.health(); })
  .then(function () {
    assert.deepStrictEqual(calls.map(function (c) { return c.method + ' ' + c.url; }), [
      'GET http://127.0.0.1:8090/kv/k',
      'PUT http://127.0.0.1:8090/kv/k',
      'DELETE http://127.0.0.1:8090/kv/k',
      'GET http://127.0.0.1:8090/scan?prefix=pre',
      'GET http://127.0.0.1:8090/health'
    ]);
    console.log('clients/kv.js: ok');
  })
  .catch(function (err) {
    console.error(err);
    process.exit(1);
  });
