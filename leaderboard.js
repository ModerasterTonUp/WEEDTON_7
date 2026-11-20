// leaderboard.js — общая таблица лидеров (30-дневный период, очки хранятся на сервере)
// Зависимость: npm i better-sqlite3
//
// Подключение (server.js, после создания io):
//   const lb = require("./leaderboard")({
//     io,
//     getUser: s => ({ id: s.data.userId, name: s.data.name }),  // ID/имя после проверки initData
//   });
//
// ВАЖНО: очки за пополнение начисляйте только с сервера, в обработчике подтверждённого депозита:
//   lb.addPoints(userId, name, "dep", amountTon);   // ×10 очков за каждый TON
// Очки за кейсы/PvP/апгрейды/крафт/продажу приходят от клиента (событие lb_points) и ограничены
// лимитами, но клиенту можно соврать. Когда будут призы, переносите и их на сервер:
//   lb.addPoints(userId, name, "case", priceTon) в вашем обработчике открытия кейса и т.д.
// Для этого уберите нужный тип из trustClientKinds.

const Database = require("better-sqlite3");

module.exports = function setupLeaderboard({
  io,
  getUser,
  dbPath,
  periodMs = 30 * 86400000,
  top = 50,
  maxAmount = 500, // потолок суммы за одно событие от клиента (TON)
  trustClientKinds = ["case", "pvp", "up", "craft", "sell"],
}) {
  const db = new Database(dbPath || process.env.LB_DB || "leaderboard.db");
  db.exec(`
    CREATE TABLE IF NOT EXISTS lb_meta(k TEXT PRIMARY KEY, v TEXT);
    CREATE TABLE IF NOT EXISTS lb_scores(
      period INTEGER, user_id TEXT, name TEXT, score REAL DEFAULT 0, PRIMARY KEY(period, user_id));
    CREATE INDEX IF NOT EXISTS lb_rank ON lb_scores(period, score DESC);
    CREATE TABLE IF NOT EXISTS lb_daily(user_id TEXT, day TEXT, PRIMARY KEY(user_id, day));`);

  const MULT = { dep: 10, case: 1, pvp: 2, up: 1, craft: 1, sell: 1 }; // как на клиенте

  // Начало текущего периода. По истечении 30 дней стартует новый, старые строки остаются в базе
  function period() {
    const r = db.prepare("SELECT v FROM lb_meta WHERE k='start'").get();
    let s = r ? +r.v : 0;
    if (!s || Date.now() - s >= periodMs) {
      s = Date.now();
      db.prepare("INSERT OR REPLACE INTO lb_meta VALUES('start', ?)").run(String(s));
    }
    return s;
  }

  function addPoints(userId, name, kind, amount) {
    const pts = +((+amount || 0) * (MULT[kind] || 1) + 1).toFixed(2);
    if (!userId || !(pts > 0)) return 0;
    const clean = String(name || "Игрок").replace(/[\u0000-\u001f<>]/g, "").slice(0, 32) || "Игрок";
    db.prepare(`INSERT INTO lb_scores(period, user_id, name, score) VALUES(?,?,?,?)
      ON CONFLICT(period, user_id) DO UPDATE SET score = score + excluded.score, name = excluded.name`)
      .run(period(), String(userId), clean, pts);
    return pts;
  }

  // Ответ для клиента: топ + строка самого игрока (me:true) + его реальное место
  function snapshot(userId) {
    const p = period(), uid = userId ? String(userId) : null;
    const out = db.prepare("SELECT user_id id, name, score FROM lb_scores WHERE period=? AND score>0 ORDER BY score DESC LIMIT ?")
      .all(p, top).map(r => ({ id: r.id, name: r.name, score: r.score, me: r.id === uid }));
    let rank = null;
    const mine = uid && db.prepare("SELECT name, score FROM lb_scores WHERE period=? AND user_id=?").get(p, uid);
    if (mine && mine.score > 0) {
      rank = db.prepare("SELECT COUNT(*)+1 n FROM lb_scores WHERE period=? AND score>?").get(p, mine.score).n;
      if (!out.some(r => r.me)) out.push({ id: uid, name: mine.name, score: mine.score, me: true });
    }
    // Клиент хранит строки по имени, поэтому одинаковые имена делаем уникальными
    const seen = new Set();
    for (const r of out) {
      if (!r.me) {
        let n = r.name;
        if (seen.has(n)) n += " " + String(r.id).slice(-3);
        seen.add(n);
        r.name = n;
      }
      delete r.id;
    }
    return { rows: out, start: p, rank };
  }

  // Победители периода (для выдачи призов): winners(startTimestamp, 10)
  const winners = (start, n = 10) =>
    db.prepare("SELECT user_id, name, score FROM lb_scores WHERE period=? AND score>0 ORDER BY score DESC LIMIT ?").all(start, n);

  // Антиспам: не больше 60 событий в минуту на игрока
  const hits = new Map();
  const allowed = id => {
    const t = Date.now(), a = (hits.get(id) || []).filter(x => t - x < 60000);
    a.push(t);
    hits.set(id, a);
    return a.length <= 60;
  };

  // Обработка очков от клиента (общая для сокета и HTTP-сервиса)
  function points(u, d = {}) {
    if (!u || !u.id || !allowed(String(u.id))) return;
    const kind = String(d.kind);
    if (kind === "login") { // ежедневный бонус: один раз за сутки (UTC)
      const day = new Date().toISOString().slice(0, 10);
      if (db.prepare("INSERT OR IGNORE INTO lb_daily VALUES(?,?)").run(String(u.id), day).changes) addPoints(u.id, u.name, "login", 5);
      return;
    }
    if (!trustClientKinds.includes(kind)) return; // "dep" и всё лишнее клиенту не доверяем
    const amount = Math.min(Math.max(+d.amount || 0, 0), maxAmount);
    if (amount > 0) addPoints(u.id, u.name, kind, amount);
  }

  if (io) io.on("connection", socket => {
    socket.on("get_lb", (a, b) => {
      const cb = typeof a === "function" ? a : b;
      if (typeof cb !== "function") return;
      const u = getUser(socket) || {};
      cb(snapshot(u.id));
    });
    socket.on("lb_points", d => points(getUser(socket), d));
  });

  return { addPoints, snapshot, winners, period, points };
};
