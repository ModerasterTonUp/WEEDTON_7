// crypto-deposit.js — пополнение прямо на ваши кошельки: USDT (TRC20) и TON/GRAM
// Заменяет usdt-deposit.js (Crypto Pay), старый файл можно удалить.
// Зависимость: npm i better-sqlite3 (Node 18+ для fetch)
//
// Подключение (server.js):
//   require("./crypto-deposit")({
//     io,
//     getUser: s => ({ id: s.data.userId }),
//     credit: async (userId, amountGram, meta) => {
//       // ВАША функция: прибавить баланс, отправить io.to(...).emit("balance", { reason: "deposit", amount })
//       // и очки рейтинга: lb.addPoints(userId, name, "dep", amountGram)
//     },
//   });
// Адреса берутся из переменных TRC20_ADDRESS и TON_ADDRESS (ниже стоят ваши по умолчанию).
// Необязательно: TRONGRID_KEY, TONCENTER_KEY (ключи API, чтобы не упираться в лимиты),
// GRAM_USD (запасной курс, если сервис курсов недоступен).
//
// ВНИМАНИЕ: код работает с реальными деньгами и не проверялся на живой сети.
// Сначала проверьте суммами 1-2 USDT и 0.1-0.5 TON.

const Database = require("better-sqlite3");

const USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"; // USDT в сети TRON

module.exports = function setupCryptoDeposit({
  io, getUser, credit,
  trc20 = process.env.TRC20_ADDRESS || "TTyG6UPzTykjtZ5mZKPTH4Apj4PbmP6YNT",
  ton = process.env.TON_ADDRESS || "UQCUNPYifoSd23RF4PlAKnjvmLAvYBWGuzg6luiWA98Czo7n",
  minUsdt = 1, maxUsdt = 5000, minTon = 0.1, ttlMin = 30, dbPath,
}) {
  const db = new Database(dbPath || process.env.CD_DB || "crypto_deposits.db");
  db.exec(`
    CREATE TABLE IF NOT EXISTS cd_usdt(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, amount REAL,
      status TEXT, tx TEXT, created_at INTEGER);
    CREATE TABLE IF NOT EXISTS cd_seen(tx TEXT PRIMARY KEY);`);
  const seen = tx => db.prepare("INSERT OR IGNORE INTO cd_seen VALUES(?)").run(tx).changes === 1; // true = новая

  // Курс: сколько GRAM (бывш. TON) за 1 USDT
  let cached = null;
  async function gramPerUsdt() {
    if (cached && Date.now() - cached.t < 60000) return cached.v;
    let usd = +process.env.GRAM_USD || 0;
    try {
      const r = await (await fetch("https://tonapi.io/v2/rates?tokens=ton&currencies=usd")).json();
      const p = Object.values(r.rates || {})[0];
      if (p && p.prices && p.prices.USD > 0) usd = +p.prices.USD;
    } catch (e) { /* возьмём запасной курс */ }
    if (!(usd > 0)) throw new Error("нет курса");
    cached = { v: 1 / usd, t: Date.now() };
    return cached.v;
  }

  // ───── USDT TRC20: у каждой заявки своя «копеечная» добавка, по ней находим плательщика
  async function checkUsdt() {
    const since = Date.now() - ttlMin * 60000;
    db.prepare("UPDATE cd_usdt SET status='expired' WHERE status='active' AND created_at<?").run(since);
    const act = db.prepare("SELECT * FROM cd_usdt WHERE status='active'").all();
    if (!act.length) return;
    const url = `https://api.trongrid.io/v1/accounts/${trc20}/transactions/trc20?only_to=true&only_confirmed=true&limit=50&contract_address=${USDT_CONTRACT}&min_timestamp=${since - 60000}`;
    const r = await (await fetch(url, { headers: process.env.TRONGRID_KEY ? { "TRON-PRO-API-KEY": process.env.TRONGRID_KEY } : {} })).json();
    for (const t of r.data || []) {
      const value = +t.value / Math.pow(10, +(t.token_info && t.token_info.decimals) || 6);
      const d = act.find(a => Math.abs(a.amount - value) < 1e-6 && a.created_at <= t.block_timestamp + 60000);
      if (!d) continue;
      const rate = await gramPerUsdt();
      if (db.prepare("UPDATE cd_usdt SET status='paid', tx=? WHERE id=? AND status='active'").run(t.transaction_id, d.id).changes !== 1) continue;
      if (!seen(t.transaction_id)) continue;
      try { await credit(d.user_id, +(value * rate).toFixed(4), { method: "usdt-trc20", usdt: value, tx: t.transaction_id }); }
      catch (e) { // не зачислили: вернём заявку в работу
        console.error("credit usdt", e);
        db.prepare("UPDATE cd_usdt SET status='active' WHERE id=?").run(d.id);
        db.prepare("DELETE FROM cd_seen WHERE tx=?").run(t.transaction_id);
      }
    }
  }

  // ───── TON/GRAM: плательщика определяем по комментарию tu-<id>
  async function checkTon() {
    const r = await (await fetch(`https://toncenter.com/api/v3/transactions?account=${ton}&limit=40&sort=desc`,
      { headers: process.env.TONCENTER_KEY ? { "X-API-Key": process.env.TONCENTER_KEY } : {} })).json();
    for (const t of r.transactions || []) {
      const m = t.in_msg;
      if (!m || !m.source || (t.description && t.description.aborted)) continue;
      const c = m.message_content && m.message_content.decoded && m.message_content.decoded.comment;
      const who = /^tu-(\d{1,15})$/.exec(String(c || "").trim());
      const amount = +m.value / 1e9;
      if (!who || !(amount >= minTon)) continue;
      if (!seen(t.hash)) continue;
      try { await credit(who[1], +amount.toFixed(4), { method: "ton", tx: t.hash }); }
      catch (e) { console.error("credit ton", e); db.prepare("DELETE FROM cd_seen WHERE tx=?").run(t.hash); }
    }
  }

  let busy = false;
  async function tick() {
    if (busy) return;
    busy = true;
    try { await checkUsdt(); } catch (e) { console.error("usdt watch", e.message); }
    try { await checkTon(); } catch (e) { console.error("ton watch", e.message); }
    busy = false;
  }
  setInterval(tick, 15000);

  io.on("connection", socket => {
    socket.on("create_deposit_usdt", (p = {}, cb = () => {}) => {
      const u = getUser(socket);
      if (!u || !u.id) return cb({ ok: false, error: "Нет авторизации" });
      const base = +(+p.amount).toFixed(2);
      if (!(base >= minUsdt)) return cb({ ok: false, error: "Минимум " + minUsdt + " USDT" });
      if (base > maxUsdt) return cb({ ok: false, error: "Максимум " + maxUsdt + " USDT" });
      const since = Date.now() - ttlMin * 60000;
      const mine = db.prepare("SELECT * FROM cd_usdt WHERE user_id=? AND status='active' AND created_at>?").all(String(u.id), since);
      if (mine.length >= 5) return cb({ ok: false, error: "Сначала оплатите или дождитесь окончания прошлых заявок" });
      const used = new Set(db.prepare("SELECT amount FROM cd_usdt WHERE status='active' AND created_at>?").all(since).map(r => r.amount));
      const ks = Array.from({ length: 99 }, (_, i) => i + 1).sort(() => Math.random() - 0.5);
      const k = ks.find(k => !used.has(+(base + k / 1000).toFixed(3)));
      if (!k) return cb({ ok: false, error: "Сейчас много заявок, попробуйте через минуту" });
      const amount = +(base + k / 1000).toFixed(3);
      db.prepare("INSERT INTO cd_usdt(user_id, amount, status, created_at) VALUES(?,?,?,?)").run(String(u.id), amount, "active", Date.now());
      cb({ ok: true, address: trc20, amount, expires: Date.now() + ttlMin * 60000 });
    });

    socket.on("get_ton_deposit", cb => {
      const u = getUser(socket);
      if (typeof cb !== "function") return;
      if (!u || !u.id) return cb({ ok: false });
      cb({ ok: true, address: ton, comment: "tu-" + u.id, min: minTon });
    });

    socket.on("check_deposit_usdt", cb => { tick(); if (typeof cb === "function") cb({ ok: true }); });
  });

  return { tick };
};
