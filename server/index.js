const express = require('express')
const http = require('http')
const https = require('https')
const path = require('path')
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

async function fetchViewerCount(webcastId) {
  const data = await apiGet('/get_live_room_num', {
    webcast_id: webcastId, version: '187', platform: 'server'
  })
  return data?.data?.data?.data?.[0]?.room_view_stats?.display_value ?? null
}

const md5 = s => crypto.createHash('md5').update(s).digest('hex')

function signParams() {
  const t = Date.now().toString()
  const k = md5(t)
  return { sign_qm: md5(md5(k)), sign_key: k, sign_t: t, no_live: '0' }
}

async function fetchRoomInfo(webcastId) {
  try {
    const data = await apiGet('/api/douyin/web/fetch_user_live_videos', {
      webcast_id: webcastId, ...signParams()
    })
    const d = data?.data?.data
    if (!d) return null
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
    console.log('fetchRoomInfo error:', e.message)
    return null
  }
}

async function poll() {
  for (const [id, room] of rooms) {
    try {
      const count = await fetchViewerCount(id)
      room.viewer_count = count
      room.error = false
      room.updated = Date.now()
      room.is_live = count !== null && parseInt(count) > 0 ? 1 : 0
      if (!room.history) room.history = []
      if (count !== null) {
        const now = Date.now()
        room.history.push({ time: now, count, like_count: room.like_count ?? 0, title: room.title || '' })
        if (room.history.length > HISTORY_MAX) room.history.shift()
        db.addHistory(id, now, parseInt(count) || 0, room.like_count ?? 0, room.title || '')
      }
      // Fetch like_count, title, avatar asynchronously (don't block)
      fetchRoomInfo(id).then(info => {
        if (!info) return
        if (info.like_count !== undefined) room.like_count = info.like_count
        if (info.title) room.title = info.title
        if (info.nickname) room.nickname = info.nickname
        if (info.avatar) room.avatar = info.avatar
        if (info.room_id) room.room_id = info.room_id
        if (info.sec_uid) room.sec_uid = info.sec_uid
        db.updateRoom(id, room)
        db.updateLatestHistory(id, info.like_count ?? 0, info.title || '')
      })
    } catch {
      room.error = true
      room.updated = Date.now()
    }
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

async function addRoom(input) {
  input = input.trim()
  let m = input.match(/(?:live\.douyin\.com|douyin\.com\/(?:live\/)?)([a-zA-Z0-9_.]{3,20})(?:\?|$)/)
  let id = m ? m[1] : (/^[a-zA-Z0-9_.]{3,20}$/.test(input) ? input : null)
  if (!id || rooms.has(id)) return null

  const now = Date.now()
  const room = { id, nickname: id, avatar: '', title: '', room_status: 0, room_id: '', like_count: 0, follower_count: null, sec_uid: '', viewer_count: null, error: false, updated: now, history: [], pinned: 0, is_live: 0 }

  rooms.set(id, room)
  db.insertRoom({ ...room, created_at: now, updated_at: now })

  const info = await fetchRoomInfo(id)
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

app.delete('/api/room/:id', (req, res) => {
  rooms.delete(req.params.id)
  db.removeRoom(req.params.id)
  broadcast(); res.json({ ok: true })
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
