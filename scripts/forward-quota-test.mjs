import assert from 'node:assert/strict'
import http from 'node:http'

// Only the upstream is a fixture; the adapter, fold and both HTTP APIs are real.
// Losing RATE_LIMIT in the fold or mapping it to a generic 502 breaks these checks.
const model = 'stepfun/step-5-preview-free'
const secret = 'fixture-private-upstream-detail'
const quota = {
  error: {
    message: 'Provider returned error', code: 429,
    metadata: { raw: JSON.stringify({ error: { type: 'rate_limited', message: `request limited TPM reached; ${secret} https://private.invalid/token` } }) },
  },
}
let reply = { status: 429, payload: quota, retryAfter: '2.5' }
let requests = 0
const upstream = http.createServer(async (req, res) => {
  for await (const _ of req) {}
  requests++
  assert.equal(req.headers.authorization, undefined, 'the Kilo free lane stays keyless')
  res.writeHead(reply.status, {
    'content-type': reply.sse ? 'text/event-stream' : 'application/json',
    ...(reply.retryAfter === undefined ? {} : { 'retry-after': reply.retryAfter }),
  })
  res.end(reply.sse ? `data: ${JSON.stringify(reply.payload)}\n\ndata: [DONE]\n\n` : JSON.stringify(reply.payload))
})
await new Promise(resolve => upstream.listen(0, '127.0.0.1', resolve))
process.env.OUR_FREE_MODEL_KILO_BASE = `http://127.0.0.1:${upstream.address().port}`
process.env.OUR_FREE_MODEL_BASE = 'http://127.0.0.1:1'
const { FreeModelAdapter, ROUTE_MAIN } = await import('../src/adapter.js')
const { createForwardCompletion } = await import('../src/core/completion.js')
const { startForwardServer } = await import('../src/forward.js')
const state = () => ({
  catalog: [{ id: model, name: 'Step fixture', channel: 'kilo', maxOutput: 8192, contextWindow: 128000 }],
  membership: { [ROUTE_MAIN]: [model] }, settings: { enabled: true, defaultMaxTokens: 4096 },
})
const adapter = new FreeModelAdapter({ state, recordUsage() {} })
const complete = createForwardCompletion({ adapter, state })
const local = await startForwardServer({
  config: () => ({ host: '127.0.0.1', port: 0, enabled: true, key: 'fixture-key' }),
  complete, modelRows: () => [],
})
const post = (endpoint, stream = false) => fetch(`http://127.0.0.1:${local.port}/v1/${endpoint}`, {
  method: 'POST', headers: { authorization: 'Bearer fixture-key', 'content-type': 'application/json' },
  body: JSON.stringify({ model, stream, messages: [{ role: 'user', content: 'Reply OK.' }], input: 'Reply OK.', max_tokens: 32 }),
})
let failures = 0
let checks = 0
async function check(name, test) {
  checks++
  try { await test(); console.log(`ok   ${name}`) }
  catch (error) { failures++; console.error(`FAIL ${name}: ${error.message}`) }
}
function safeQuota(error) {
  assert.equal(error.code, 'RATE_LIMIT')
  assert.equal(error.type, 'rate_limit_error')
  assert.match(error.message, /upstream.*tokens.per.minute|upstream.*TPM/i)
  assert.doesNotMatch(JSON.stringify(error), /fixture-private|private.invalid|metadata|token=/)
}
try {
  for (const endpoint of ['chat/completions', 'responses']) {
    await check(`${endpoint}: upstream quota is HTTP429 with safe detail and rounded Retry-After`, async () => {
      reply = { status: 429, payload: quota, retryAfter: '2.5' }
      const before = requests
      const response = await post(endpoint)
      assert.equal(response.status, 429)
      assert.equal(response.headers.get('retry-after'), '3')
      safeQuota((await response.json()).error)
      assert.equal(requests, before + 1, 'quota must not trigger a retry or fallback')
    })
    await check(`${endpoint}: streaming quota terminates with a classified error, never success`, async () => {
      const response = await post(endpoint, true)
      assert.equal(response.status, 200, 'stream headers were already committed')
      assert.match(response.headers.get('content-type'), /text\/event-stream/)
      const wire = await response.text()
      const frames = wire.split('\n').filter(line => line.startsWith('data: ') && line !== 'data: [DONE]').map(line => JSON.parse(line.slice(6)))
      const failed = frames.find(frame => frame.error || frame.type === 'response.failed')
      safeQuota(failed?.error ?? failed?.response?.error)
      assert.equal(frames.some(frame => frame.type === 'response.completed' || frame.choices?.some(choice => choice.finish_reason)), false)
      if (endpoint === 'chat/completions') assert.ok(wire.endsWith('data: [DONE]\n\n'))
      else assert.equal(frames.at(-1).response.status, 'failed')
    })
  }
  await check('quota inside an upstream SSE stream uses the same safe classification', async () => {
    reply = { status: 200, payload: quota, sse: true }
    const response = await post('chat/completions')
    assert.equal(response.status, 429)
    safeQuota((await response.json()).error)
    assert.equal(response.headers.get('retry-after'), null)
  })
  for (const retryAfter of [undefined, 'not-a-delay', '-1', 'Infinity', '1e300']) {
    await check(`invalid or absent Retry-After is omitted (${retryAfter ?? 'absent'})`, async () => {
      reply = { status: 429, payload: quota, retryAfter }
      const response = await post('chat/completions')
      assert.equal(response.status, 429)
      assert.equal(response.headers.get('retry-after'), null)
      await response.arrayBuffer()
    })
  }
  await check('unclassified server failures remain HTTP502 without quota detail or delay', async () => {
    reply = { status: 503, payload: { error: { message: 'Provider returned error', metadata: quota.error.metadata } }, retryAfter: '10' }
    const response = await post('chat/completions')
    assert.equal(response.status, 502)
    assert.equal(response.headers.get('retry-after'), null)
    const error = (await response.json()).error
    assert.equal(error.type, 'server_error')
    assert.doesNotMatch(error.message, /TPM|tokens.per.minute|fixture-private/)
  })
  await check('unrecognized nested quota metadata is never reflected', async () => {
    reply = { status: 429, payload: { error: { message: 'Provider returned error', code: 429, metadata: { raw: `${secret} https://private.invalid/token` } } } }
    const response = await post('chat/completions')
    assert.equal(response.status, 429)
    const error = (await response.json()).error
    assert.equal(error.code, 'RATE_LIMIT')
    assert.doesNotMatch(JSON.stringify(error), /fixture-private|private.invalid|TPM/)
  })
} finally {
  await local.close()
  upstream.closeAllConnections?.()
  await new Promise(resolve => upstream.close(resolve))
}
console.log(`\nforward-quota: ${checks - failures}/${checks} checks passed`)
process.exitCode = failures ? 1 : 0
