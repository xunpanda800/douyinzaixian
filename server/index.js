const express = require('express')
const http = require('http')
const https = require('https')
const path = require('path')
const fs = require('fs')
const crypto = require('crypto')
const { WebSocketServer } = require('ws')
const db = require('./db')

const app = express()
const server = http.createServer(app)
const wss = new WebSocketServer({ server })

app.use(express.json())
app.use((req, res, next) => {
  if (req.path === '/' || req.path.endsWith('.html')) {
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
  }
  next()
})
app.use(express.static(path.join(__dirname, '../public')))

const API_HOST = 'dyapi.phpnbw.com'
const POLL_MS = 30000
const HISTORY_MAX = 500 // in-memory limit; DB stores all

let rooms = new Map()

async function loadFromDb() {
  await db.init()
  const rows = db.loadRooms()
  for (const r of rows) {
    const room = { ...r, error: false, updated: r.updated_at, history: db.loadHistory(r.id) || [] }
    rooms.set(r.id, room)
  }
  if (rows.length) console.log(`Loaded ${rows.length} rooms from database`)
}

function apiGet(pathname, query) {
  return new Promise((resolve, reject) => {
    const qs = Object.entries(query || {}).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&')
    const url = `${pathname}${qs ? '?' + qs : ''}`
    const req = https.get({ host: API_HOST, path: url, rejectUnauthorized: false, timeout: 8000, headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' } }, (res) => {
      let body = ''
      res.on('data', c => body += c)
      res.on('end', () => {
        try {
          if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`))
          resolve(JSON.parse(body))
        } catch (e) { reject(e) }
      })
    })
    req.on('error', reject)
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')) })
  })
}

async function fetchViewerCount(webcastId, retries = 2) {
  for (let i = 0; i <= retries; i++) {
    try {
      const data = await apiGet('/get_live_room_num', {
        webcast_id: webcastId, version: '187', platform: 'server'
      })
      const count = data?.data?.data?.data?.[0]?.room_view_stats?.display_value
      if (count !== null && count !== undefined) return count
    } catch (e) {
      if (i < retries) {
        await new Promise(r => setTimeout(r, 1000 * (i + 1)))
      } else {
        throw e
      }
    }
  }
  return null
}

const md5 = s => crypto.createHash('md5').update(s).digest('hex')

function signParams() {
  const t = Date.now().toString()
  const k = md5(t)
  return { sign_qm: md5(md5(k)), sign_key: k, sign_t: t, no_live: '0' }
}

async function fetchRoomInfo(webcastId, useFallback) {
  for (const source of useFallback ? ['dyapi', 'wtf'] : ['dyapi']) {
    try {
      const data = source === 'dyapi'
        ? await apiGet('/api/douyin/web/fetch_user_live_videos', { webcast_id: webcastId, ...signParams() })
        : await apiGetWtf('/api/douyin/web/fetch_user_live_videos', { webcast_id: webcastId })
      const d = data?.data?.data
      if (!d) continue
      const stream = (d.data || [])[0] || {}
      const owner = stream.owner || {}
      const user = d.user || {}
      return {
        room_status: d.room_status ?? 0,
        nickname: user.nickname || owner.nickname || '',
        avatar: user.avatar_thumb?.url_list?.[0] || owner.avatar_thumb?.url_list?.[0] || '',
        title: stream.title || '',
        like_count: stream.like_count ?? 0,
        room_id: stream.id_str || '',
        sec_uid: user.sec_uid || owner.sec_uid || '',
        follower_count: null,
        viewer_count: stream.room_view_stats?.display_value ?? null,
        webcast_id: webcastId,
      }
    } catch (e) {
      if (useFallback) console.log(`fetchRoomInfo (${source}) error:`, e.message)
    }
  }
  return null
}

async function poll() {
  const entries = [...rooms.entries()]
  const CONCURRENCY = 10
  for (let i = 0; i < entries.length; i += CONCURRENCY) {
    const batch = entries.slice(i, i + CONCURRENCY)
    await Promise.allSettled(batch.map(async ([id, room]) => {
      try {
        const count = await fetchViewerCount(id)
        room.viewer_count = count
        room.error = false
        room.updated = Date.now()
        if (room.offline_count === undefined) room.offline_count = 0
        if (count !== null && parseInt(count) > 0) {
          room.offline_count = 0
          room.is_live = 1
        } else {
          room.offline_count++
          if (room.offline_count >= 10) room.is_live = 0
        }
        if (!room.history) room.history = []
        if (count !== null) {
          const now = Date.now()
          room.history.push({ time: now, count, like_count: room.like_count ?? 0, title: room.title || '' })
          if (room.history.length > HISTORY_MAX) room.history.shift()
          db.addHistory(id, now, parseInt(count) || 0, room.like_count ?? 0, room.title || '')
        }
      } catch {
        room.error = true
        room.updated = Date.now()
        if (room.offline_count === undefined) room.offline_count = 0
        room.offline_count++
        if (room.offline_count >= 10) room.is_live = 0
      }
    }))
  }
  // Fetch room info for all rooms (with fallback only if viewer count fetch failed)
  for (const [id, room] of rooms) {
    const needsFallback = room.viewer_count === null || room.viewer_count === undefined
    fetchRoomInfo(id, needsFallback).then(info => {
      if (!info) return
      if (info.like_count !== undefined) room.like_count = info.like_count
      if (info.title) room.title = info.title
      if (info.nickname) room.nickname = info.nickname
      if (info.avatar) room.avatar = info.avatar
      if (info.room_id) room.room_id = info.room_id
      if (info.sec_uid) room.sec_uid = info.sec_uid
      if (info.viewer_count !== null && needsFallback) room.viewer_count = info.viewer_count
      db.updateRoom(id, room)
      db.updateLatestHistory(id, info.like_count ?? 0, info.title || '')
    })
  }
  broadcast()
}

function broadcast() {
  const list = [...rooms.values()].map(r => ({
    id: r.id, viewer_count: r.viewer_count, nickname: r.nickname,
    avatar: r.avatar || '', title: r.title || '',
    room_status: r.room_status ?? 0, room_id: r.room_id || '',
    like_count: r.like_count ?? 0, follower_count: r.follower_count ?? null,
    sec_uid: r.sec_uid || '', error: r.error, updated: r.updated,
    history: (r.history || []).slice(-60), pinned: r.pinned || 0,
    is_live: r.is_live ?? 0,
  }))
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(JSON.stringify({ rooms: list })) })
}

function broadcastMessage(type, data) {
  const msg = JSON.stringify({ type, data })
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg) })
}

async function addRoom(input) {
  input = input.trim()
  let m = input.match(/(?:live\.douyin\.com|douyin\.com\/(?:live\/)?)([a-zA-Z0-9_.]{3,20})(?:\?|$)/)
  let id = m ? m[1] : (/^[a-zA-Z0-9_.]{3,20}$/.test(input) ? input : null)
  if (!id || rooms.has(id)) return null

  const now = Date.now()
  const room = { id, nickname: id, avatar: '', title: '', room_status: 0, room_id: '', like_count: 0, follower_count: null, sec_uid: '', viewer_count: null, error: false, updated: now, history: [], pinned: 0, is_live: 0 }

  rooms.set(id, room)
  db.insertRoom({ ...room, created_at: now, updated_at: now })

  const info = await fetchRoomInfo(id, true)
  if (info) {
    if (info.nickname) room.nickname = info.nickname
    if (info.avatar) room.avatar = info.avatar
    if (info.title) room.title = info.title
    if (info.room_status !== undefined) room.room_status = info.room_status
    if (info.room_id) room.room_id = info.room_id
    if (info.like_count !== undefined) room.like_count = info.like_count
    if (info.follower_count !== undefined) room.follower_count = info.follower_count
    if (info.sec_uid) room.sec_uid = info.sec_uid
    db.updateRoom(id, room)
  }

  return id
}

function apiGetWtf(pathname, query) {
  return new Promise((resolve, reject) => {
    const qs = Object.entries(query || {}).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&')
    const url = `${pathname}${qs ? '?' + qs : ''}`
    const req = https.get({ host: 'douyin.wtf', path: url, rejectUnauthorized: false, timeout: 8000, headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' } }, (res) => {
      let body = ''
      res.on('data', c => body += c)
      res.on('end', () => {
        try {
          if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`))
          resolve(JSON.parse(body))
        } catch (e) { reject(e) }
      })
    })
    req.on('error', reject)
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')) })
  })
}

app.get('/api/ranking/:roomId', async (req, res) => {
  try {
    const room = rooms.get(req.params.roomId)
    const roomId = room?.room_id || req.params.roomId
    let data = await apiGetWtf('/api/douyin/web/fetch_live_gift_ranking', { room_id: roomId })
    const ranks = data?.data?.data?.ranks || []
    if (!ranks.length) console.log('rank empty for', req.params.roomId, 'room_id:', roomId)
    res.json(ranks.map(u => ({
      rank: u.rank,
      score: u.score,
      score_description: u.score_description,
      nickname: u.user?.nickname || '',
      gender: u.user?.gender || 0,
      level: u.user?.pay_grade?.level || u.user?.level || 0,
      sec_uid: u.user?.sec_uid || '',
      avatar_url: u.user?.avatar_thumb?.url_list?.[0] || '',
      user_id: u.user?.id?.toString() || '',
      honor_level: u.user?.pay_grade?.level || 0,
      fans_club: u.user?.fans_club?.data || null,
      user: u.user,
    })))
  } catch (e) { console.log('rank error', req.params.roomId, e.message); res.json([]) }
})

app.get('/api/analytics/:roomId', (req, res) => {
  const history = db.loadHistory(req.params.roomId)
  if (!history || history.length < 2) return res.json({ error: '数据不足' })

  const counts = history.map(h => h.count).filter(c => c !== null && c > 0)
  if (counts.length < 2) return res.json({ error: '数据不足' })

  const n = counts.length, now = Date.now()
  const current = counts[counts.length - 1]
  const sum = counts.reduce((a, b) => a + b, 0)
  const avg = Math.round(sum / n)
  const max = Math.max(...counts)
  const min = Math.min(...counts)
  const maxTime = history[counts.indexOf(max)].time
  const minTime = history[counts.indexOf(min)].time
  const variance = counts.reduce((s, v) => s + (v - avg) ** 2, 0) / n
  const stddev = Math.round(Math.sqrt(variance))
  const cv = avg > 0 ? Math.round(stddev / avg * 100) : 0

  const trendPts = Math.min(30, counts.length)
  const recent = counts.slice(-trendPts)
  const xM = (trendPts - 1) / 2, yM = recent.reduce((a, b) => a + b, 0) / trendPts
  let num = 0, den = 0
  for (let i = 0; i < trendPts; i++) { num += (i - xM) * (recent[i] - yM); den += (i - xM) ** 2 }
  const slope = den ? Math.round(num / den * 100) / 100 : 0

  const findVal = t => { const i = history.findLastIndex(h => h.time <= t); return i >= 0 ? counts[i] : null }
  const windows = { '5min': 5, '15min': 15, '30min': 30 }
  const deltas = {}
  for (const [k, m] of Object.entries(windows)) deltas[k] = current - findVal(now - m * 60000)

  const liveCount = counts.filter(c => c > 0).length
  const liveRatio = Math.round(liveCount / counts.length * 100)

  const buckets = {}
  history.forEach(h => { if (h.count > 0) { const hh = new Date(h.time).getHours(); if (!buckets[hh]) buckets[hh] = { s: 0, c: 0 }; buckets[hh].s += h.count; buckets[hh].c++ } })
  const peakHours = Object.entries(buckets).map(([h, d]) => ({ h: parseInt(h), avg: Math.round(d.s / d.c) })).sort((a, b) => b.avg - a.avg).slice(0, 5)

  const recent30 = history.filter(h => h.time >= now - 30 * 60000).map(h => h.count).filter(c => c !== null)

  res.json({
    current, avg, max, maxTime, min, minTime, stddev, cv,
    trend: slope > 0.5 ? 'up' : slope < -0.5 ? 'down' : 'stable',
    slope, deltas, liveRatio,
    totalPoints: n,
    totalMinutes: Math.round((now - history[0].time) / 60000),
    peakHours,
    recentPeak: recent30.length ? Math.max(...recent30) : null,
  })
})

app.get('/api/history/:roomId', (req, res) => {
  const history = db.loadHistory(req.params.roomId)
  res.json(history)
})

app.post('/api/room', async (req, res) => {
  const id = await addRoom(req.body.input)
  if (!id) return res.status(400).json({ error: '无效或已存在' })
  broadcast(); res.json({ id })
})

app.post('/api/room/nickname', (req, res) => {
  const room = rooms.get(req.body.id)
  if (!room) return res.status(404).json({ error: 'not found' })
  room.nickname = req.body.nickname || room.id
  db.updateRoom(req.body.id, { nickname: room.nickname })
  broadcast(); res.json({ ok: true })
})

app.post('/api/room/batch', async (req, res) => {
  const ids = req.body.ids || []
  const added = []
  for (const input of ids) {
    const id = await addRoom(input.trim())
    if (id) added.push(id)
  }
  broadcast()
  res.json({ added })
})

app.post('/api/room/pin', (req, res) => {
  const room = rooms.get(req.body.id)
  if (!room) return res.status(404).json({ error: 'not found' })
  room.pinned = req.body.pinned ? 1 : 0
  db.updateRoom(req.body.id, { pinned: room.pinned })
  broadcast(); res.json({ ok: true })
})

app.post('/api/room/reorder', (req, res) => {
  const ids = req.body.ids || []
  const now = Date.now()
  const reordered = new Map()
  ids.forEach((id, i) => {
    const room = rooms.get(id)
    if (room) {
      db.updateRoom(id, { ...room, created_at: now + i })
      reordered.set(id, room)
    }
  })
  // Keep any rooms not in the new order at the end
  for (const [id, room] of rooms) {
    if (!reordered.has(id)) reordered.set(id, room)
  }
  rooms = reordered
  res.json({ ok: true })
})

app.post('/api/room/refresh/:id', async (req, res) => {
  const room = rooms.get(req.params.id)
  if (!room) return res.status(404).json({ error: 'not found' })
  const info = await fetchRoomInfo(req.params.id, true)
  if (info) {
    if (info.nickname) room.nickname = info.nickname
    if (info.avatar) room.avatar = info.avatar
    if (info.title) room.title = info.title
    if (info.room_status !== undefined) room.room_status = info.room_status
    if (info.room_id) room.room_id = info.room_id
    if (info.like_count !== undefined) room.like_count = info.like_count
    if (info.follower_count !== undefined) room.follower_count = info.follower_count
    if (info.sec_uid) room.sec_uid = info.sec_uid
    db.updateRoom(req.params.id, room)
  }
  broadcast()
  res.json({ ok: true, updated: !!info })
})

app.delete('/api/room/:id', (req, res) => {
  rooms.delete(req.params.id)
  db.removeRoom(req.params.id)
  broadcast(); res.json({ ok: true })
})

app.get('/api/system/version', (req, res) => {
  try {
    const v = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'version.json'), 'utf8'))
    res.json(v)
  } catch { res.json({ version: 'unknown' }) }
})

// Docker API client - tries all possible connection methods
const DOCKER_SOCKETS = [
  '/var/run/docker.sock', '/run/docker.sock',
  '/var/run/docker-ce.sock', '/run/docker-ce.sock',
  '/var/run/docker.sys', '/docker.sock',
]

function findDockerSocket() {
  for (const s of DOCKER_SOCKETS) {
    try { if (fs.existsSync(s) && fs.statSync(s).isSocket()) return s } catch {}
  }
  return null
}

function dockerHttp(opts) {
  return new Promise((resolve, reject) => {
    const req = http.request(opts, res => {
      let b = ''
      res.on('data', c => b += c)
      res.on('end', () => {
        try { resolve(b ? JSON.parse(b) : null) } catch { resolve(b) }
      })
    })
    req.on('error', reject)
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')) })
    if (opts.body) req.write(JSON.stringify(opts.body))
    req.end()
  })
}

function dockerRequest(method, apiPath, body, timeout = 10000) {
  const sock = findDockerSocket()
  if (sock) {
    return dockerHttp({ socketPath: sock, method, path: apiPath, headers: { 'Host': 'localhost', 'Content-Type': body ? 'application/json' : undefined }, body, timeout, setTimeout: timeout })
  }
  // Try TCP as fallback (common Docker ports)
  const hosts = (process.env.DOCKER_HOST || 'tcp://127.0.0.1:2375').replace('tcp://', '').split(':')
  return dockerHttp({ host: hosts[0] || '127.0.0.1', port: parseInt(hosts[1]) || 2375, method, path: apiPath, headers: { 'Host': 'localhost', 'Content-Type': body ? 'application/json' : undefined }, body, timeout, setTimeout: timeout })
}

async function dockerPull(image) {
  const m = image.match(/(.+?)\/(.+?):(.+)/)
  if (!m) throw new Error('invalid image: ' + image)
  const fromImage = encodeURIComponent(m[1] + '/' + m[2])
  const tag = encodeURIComponent(m[3])
  const apiPath = `/v1.41/images/create?fromImage=${fromImage}&tag=${tag}`
  let lastStatus = ''
  const sock = findDockerSocket()
  if (!sock) throw new Error('无法找到 Docker 套接字，请确认已在 docker-compose.yml 中挂载 /var/run/docker.sock')

  return new Promise((resolve, reject) => {
    const req = http.request({ socketPath: sock, path: apiPath, method: 'POST', headers: { 'Host': 'localhost' }, timeout: 180000 }, res => {
      res.on('data', c => {
        const lines = c.toString().split('\n').filter(Boolean)
        for (const l of lines) {
          try {
            const j = JSON.parse(l)
            if (j.status) lastStatus = j.status
            if (j.error) reject(new Error(j.error))
          } catch {}
        }
      })
      res.on('end', () => {
        if (lastStatus.includes('Downloaded') || lastStatus.includes('Already exists') || lastStatus.includes('Pulled') || lastStatus.includes('up to date')) resolve(lastStatus)
        else reject(new Error('pull incomplete: ' + lastStatus))
      })
    })
    req.on('error', reject)
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')) })
    req.setTimeout(185000)
    req.end()
  })
}

// Latest GitHub release version check
app.get('/api/system/check-update', (req, res) => {
  let current = 'dev'
  try { current = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'version.json'), 'utf8')).version || current } catch {}
  const ghReq = https.get('https://api.github.com/repos/xunpanda800/douyinzaixian/releases/latest', { headers: { 'User-Agent': 'dy-live-viewer', 'Accept': 'application/vnd.github.v3+json' }, timeout: 8000 }, ghRes => {
    let b = ''
    ghRes.on('data', c => b += c)
    ghRes.on('end', () => {
      try {
        const data = JSON.parse(b)
        const latest = (data.tag_name || data.name || '').replace(/^v/, '')
        const hasUpdate = latest !== current
        res.json({ current, latest, hasUpdate, url: data.html_url || '' })
      } catch { res.json({ current, latest: '', hasUpdate: false, error: 'parse failed' }) }
    })
  })
  ghReq.on('error', () => res.json({ current, latest: '', hasUpdate: false, error: 'network' }))
  ghReq.setTimeout(8000, () => { ghReq.destroy(); res.json({ current, latest: '', hasUpdate: false, error: 'timeout' }) })
})

// Diagnose Docker connectivity
app.get('/api/system/docker-diag', (req, res) => {
  const results = {}
  for (const s of DOCKER_SOCKETS) {
    try {
      const exists = fs.existsSync(s)
      let isSock = false
      if (exists) { try { isSock = fs.statSync(s).isSocket() } catch {} }
      results[s] = exists ? (isSock ? '✅ socket' : '⚠️ 文件存在但不是 socket') : '❌ 不存在'
    } catch (e) { results[s] = '❌ ' + e.message }
  }
  // Try connecting to Docker daemon
  const sock = findDockerSocket()
  if (sock) {
    dockerHttp({ socketPath: sock, path: '/v1.41/info', headers: { 'Host': 'localhost' }, timeout: 3000 })
      .then(info => res.json({ sockets: results, connected: true, version: info.ServerVersion || '?' }))
      .catch(e => res.json({ sockets: results, connected: false, error: e.message }))
  } else {
    res.json({ sockets: results, connected: false, note: '未找到 Docker 套接字。请确认 docker-compose.yml 中已添加: volumes: [ "/var/run/docker.sock:/var/run/docker.sock" ]' })
  }
})

app.post('/api/system/update', async (req, res) => {
  res.json({ ok: true, message: '开始更新...' })
  try {
    const status = await dockerPull('ghcr.io/xunpanda800/douyinzaixian:latest')
    console.log('update pull:', status)
    broadcastMessage('update', { ok: true, message: '镜像拉取完成，正在重启...' })
    setTimeout(async () => {
      try {
        const containers = await dockerRequest('GET', '/v1.41/containers/json?all=true&filters={"name":["dy-live-viewer"]}')
        if (containers && containers[0]) {
          await dockerRequest('POST', `/v1.41/containers/${containers[0].Id}/restart`)
        }
      } catch (e) {
        console.log('update restart error:', e.message)
      }
    }, 500)
  } catch (e) {
    console.log('update pull error:', e.message)
    broadcastMessage('update', { ok: false, message: '更新失败: ' + e.message })
  }
})

wss.on('connection', (ws) => {
  broadcast()
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString())
      if (msg.type === 'add') { addRoom(msg.input).then(id => { if (id) broadcast() }) }
      else if (msg.type === 'remove') { rooms.delete(msg.id); db.removeRoom(msg.id); broadcast() }
      else if (msg.type === 'rename') {
        const room = rooms.get(msg.id)
        if (room) { room.nickname = msg.nickname || room.id; db.updateRoom(msg.id, { nickname: room.nickname }); broadcast() }
      }
    } catch {}
  })
})

setInterval(poll, POLL_MS)

const PORT = process.env.PORT || 3000
loadFromDb().then(() => {
  server.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`)
  })
})
