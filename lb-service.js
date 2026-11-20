// lb-service.js — отдельный сервис рейтинга (не требует доступа к основному бэкенду)
// Запуск: BOT_TOKEN=... node lb-service.js      (npm i better-sqlite3, Node 18+)
// Деплой: отдельный Web Service на Render (Start Command: node lb-service.js),
// затем в index.html впишите его адрес в LB_URL.
// Игрок определяется по подписи Telegram initData, подделать чужой ID нельзя.

const http = require("http");
const crypto = require("crypto");

const TOKEN = process.env.BOT_TOKEN;
if (!TOKEN) throw new Error("Задай BOT_TOKEN (токен бота, под которым открывается мини-апп)");

const lb = require("./leaderboard")({ io: null, getUser: null });

function verify(initData) {
  if (!initData) return null;
  const p = new URLSearchParams(initData), hash = p.get("hash");
  if (!hash) return null;
  p.delete("hash");
  const str = [...p.keys()].sort().map(k => k + "=" + p.get(k)).join("\n");
  const secret = crypto.createHmac("sha256", "WebAppData").update(TOKEN).digest();
  const calc = crypto.createHmac("sha256", secret).update(str).digest("hex");
  if (calc.length !== hash.length || !crypto.timingSafeEqual(Buffer.from(calc), Buffer.from(hash))) return null;
  if (Date.now() / 1000 - (+p.get("auth_date") || 0) > 86400) return null; // подпись не старше суток
  try {
    const u = JSON.parse(p.get("user") || "null");
    return u && u.id ? { id: u.id, name: u.username || u.first_name || "Игрок" } : null;
  } catch (e) { return null; }
}

const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Init-Data");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") { res.writeHead(204); return res.end(); }
  const send = (c, o) => { res.writeHead(c, { "Content-Type": "application/json" }); res.end(JSON.stringify(o)); };
  const url = req.url.split("?")[0];
  const user = verify(req.headers["x-init-data"]);

  if (req.method === "GET" && url === "/lb") return send(200, lb.snapshot(user && user.id));
  if (req.method === "POST" && url === "/lb/points") {
    if (!user) return send(401, { ok: false });
    let body = "";
    req.on("data", c => { body += c; if (body.length > 2048) req.destroy(); });
    req.on("end", () => {
      try { lb.points(user, JSON.parse(body || "{}")); send(200, { ok: true }); }
      catch (e) { send(400, { ok: false }); }
    });
    return;
  }
  send(url === "/" ? 200 : 404, { ok: url === "/" });
});

if (require.main === module) server.listen(process.env.PORT || 3000, () => console.log("lb-service запущен"));
module.exports = { verify, server };
