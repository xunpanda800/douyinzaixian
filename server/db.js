const fs = require('fs')
const path = require('path')
const initSqlJs = require('sql.js')

const DB_PATH = path.join(__dirname, '..', 'data', 'dy.db')
const BACKUP_DIR = path.join(__dirname, '..', 'data', 'backups')

function backup() {
  if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true })
  const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
  fs.copyFileSync(DB_PATH, path.join(BACKUP_DIR, `dy_${ts}.db`))
  // Keep only last 20 backups
  const files = fs.readdirSync(BACKUP_DIR).filter(f => f.startsWith('dy_')).sort().reverse()
  files.slice(20).forEach(f => fs.unlinkSync(path.join(BACKUP_DIR, f)))
}

let db

async function init() {
  const SQL = await initSqlJs()
  const dir = path.dirname(DB_PATH)
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true })
  if (fs.existsSync(DB_PATH)) {
    db = new SQL.Database(fs.readFileSync(DB_PATH))
  } else {
    db = new SQL.Database()
  }
  db.run(`CREATE TABLE IF NOT EXISTS rooms (
    id TEXT PRIMARY KEY,
    nickname TEXT NOT NULL DEFAULT '',
    avatar TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    room_status INTEGER NOT NULL DEFAULT 0,
    room_id TEXT NOT NULL DEFAULT '',
    like_count INTEGER NOT NULL DEFAULT 0,
    follower_count INTEGER,
    sec_uid TEXT NOT NULL DEFAULT '',
    viewer_count INTEGER,
    pinned INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    total_viewers INTEGER DEFAULT 0
  )`)
  try { db.run("ALTER TABLE rooms ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0") } catch (e) {}
  try { db.run("ALTER TABLE rooms ADD COLUMN total_viewers INTEGER DEFAULT 0") } catch (e) {}
  db.run(`CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    end_time INTEGER,
    peak_viewers INTEGER DEFAULT 0,
    total_viewers INTEGER DEFAULT 0,
    duration_minutes INTEGER DEFAULT 0
  )`)
  db.run(`CREATE INDEX IF NOT EXISTS idx_sessions_room ON sessions(room_id, start_time)`)
  try { db.run("ALTER TABLE sessions ADD COLUMN total_viewers INTEGER DEFAULT 0") } catch (e) {}
  db.run(`CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id TEXT NOT NULL,
    time INTEGER NOT NULL,
    count INTEGER,
    like_count INTEGER DEFAULT 0,
    title TEXT DEFAULT '',
    FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
  )`)
  db.run(`CREATE INDEX IF NOT EXISTS idx_history_room ON history(room_id, time)`)
  try { db.run("ALTER TABLE history ADD COLUMN like_count INTEGER DEFAULT 0") } catch (e) {}
  try { db.run("ALTER TABLE history ADD COLUMN title TEXT DEFAULT ''") } catch (e) {}
  save()
  backup()
}

function save() {
  fs.writeFileSync(DB_PATH, Buffer.from(db.export()))
  backup()
}

function loadRooms() {
  const rows = db.exec("SELECT * FROM rooms ORDER BY pinned DESC, created_at ASC")
  if (!rows.length) return []
  const cols = rows[0].columns
  return rows[0].values.map(v => {
    const r = {}
    cols.forEach((c, i) => { r[c] = v[i] })
    return r
  })
}

function loadHistory(roomId) {
  const rows = db.exec("SELECT time, count, like_count, title FROM history WHERE room_id = ? ORDER BY time ASC", [roomId])
  if (!rows.length) return []
  const cols = rows[0].columns
  return rows[0].values.map(v => {
    const r = {}
    cols.forEach((c, i) => { r[c] = v[i] })
    return r
  })
}

function insertRoom(room) {
  db.run("INSERT OR REPLACE INTO rooms (id, nickname, avatar, title, room_status, room_id, like_count, follower_count, sec_uid, viewer_count, pinned, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [room.id, room.nickname, room.avatar, room.title, room.room_status ?? 0, room.room_id ?? '', room.like_count ?? 0, room.follower_count ?? null, room.sec_uid ?? '', room.viewer_count ?? null, room.pinned ?? 0, room.created_at || Date.now(), room.updated_at || Date.now()])
  save()
}

function updateRoom(id, data) {
  const sets = []
  const vals = []
  for (const k of ['nickname','avatar','title','room_status','room_id','like_count','follower_count','sec_uid','viewer_count','created_at','pinned']) {
    if (data[k] !== undefined) { sets.push(`${k}=?`); vals.push(data[k]) }
  }
  if (sets.length === 0) return
  sets.push('updated_at=?')
  vals.push(Date.now())
  vals.push(id)
  db.run(`UPDATE rooms SET ${sets.join(',')} WHERE id=?`, vals)
  save()
}

function removeRoom(id) {
  db.run("DELETE FROM history WHERE room_id=?", [id])
  db.run("DELETE FROM rooms WHERE id=?", [id])
  save()
}

function addHistory(roomId, time, count, likeCount, title) {
  db.run("INSERT INTO history (room_id, time, count, like_count, title) VALUES (?, ?, ?, ?, ?)", [roomId, time, count, likeCount ?? 0, title ?? ''])
  save()
}

function updateLatestHistory(roomId, likeCount, title) {
  try {
    db.run("UPDATE history SET like_count=?, title=? WHERE id=(SELECT MAX(id) FROM history WHERE room_id=?)", [likeCount ?? 0, title ?? '', roomId])
    save()
  } catch (e) {}
}

function saveSession(roomId, startTime, endTime, peakViewers, totalViewers) {
  const dur = Math.round((endTime - startTime) / 60000)
  db.run("INSERT INTO sessions (room_id, start_time, end_time, peak_viewers, total_viewers, duration_minutes) VALUES (?, ?, ?, ?, ?, ?)",
    [roomId, startTime, endTime, peakViewers || 0, totalViewers || 0, dur])
  save()
}

function loadSessions(roomId) {
  const rows = db.exec("SELECT * FROM sessions WHERE room_id=? ORDER BY start_time DESC LIMIT 30", [roomId])
  if (!rows.length) return []
  const cols = rows[0].columns
  return rows[0].values.map(v => {
    const r = {}
    cols.forEach((c, i) => { r[c] = v[i] })
    return r
  })
}

module.exports = { init, loadRooms, loadHistory, insertRoom, updateRoom, removeRoom, addHistory, updateLatestHistory, saveSession, loadSessions }
