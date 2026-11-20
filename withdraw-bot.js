'use strict';
/*
 * WithdrawBot для TonUp. Переменные окружения:
 *   BOT_TOKEN      токен от @BotFather (ТОЛЬКО в env, не в коде)
 *   ADMIN_ID       числовой Telegram ID админа (единственный, кто может одобрять/отклонять)
 *   DATABASE_URL   та же PostgreSQL, что у казино
 *   MIN_WITHDRAW   минимум вывода, TON (по умолчанию 1)
 *   MAX_PENDING    макс. неотработанных заявок на игрока (по умолчанию 3)
 *   WEBHOOK_URL    публичный https-адрес сервиса; на Render берётся из RENDER_EXTERNAL_URL автоматически.
 *                  Нет адреса → запуск в режиме Long Polling.
 *   PGSSL=off      отключить SSL к БД (локально)
 */
const http = require('http');
const crypto = require('crypto');
const { Pool } = require('pg');
const { Telegraf, Markup } = require('telegraf');

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_ID = String(process.env.ADMIN_ID || '').trim();
const DATABASE_URL = process.env.DATABASE_URL;
const MIN_WITHDRAW = Number(process.env.MIN_WITHDRAW) || 1;
const MAX_PENDING = Number(process.env.MAX_PENDING) || 3;
const PORT = Number(process.env.PORT) || 3000;
if (!BOT_TOKEN || !/^\d+$/.test(ADMIN_ID) || !DATABASE_URL) {
  console.error('[withdraw] Нужны BOT_TOKEN, ADMIN_ID (число) и DATABASE_URL');
  process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL, ssl: process.env.PGSSL === 'off' ? false : { rejectUnauthorized: false }, max: 5 });
const bot = new Telegraf(BOT_TOKEN);

const r6 = (n) => Math.round(n * 1e6) / 1e6;
const fmt = (n) => String(+Number(n).toFixed(6));
const esc = (s) => String(s).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
const WALLET_RE = /^(EQ|UQ)[A-Za-z0-9_-]{46}$/;      // user-friendly TON-адрес (48 символов)
const AMOUNT_RE = /^\d{1,9}([.,]\d{1,6})?$/;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS withdrawals (
  id BIGSERIAL PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  amount NUMERIC(20,6) NOT NULL CHECK (amount > 0),
  wallet TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','paid','rejected')),
  username TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ,
  processed_by TEXT
);
CREATE INDEX IF NOT EXISTS withdrawals_user_idx ON withdrawals (user_id, status);
CREATE INDEX IF NOT EXISTS withdrawals_status_idx ON withdrawals (status, id DESC);`;

/* ---- диалог: userId -> { step: 'amount' | 'wallet', amount, at } (в памяти, живёт 10 минут) ---- */
const sessions = new Map();
setInterval(() => { const t = Date.now(); for (const [k, v] of sessions) if (t - v.at > 600000) sessions.delete(k); }, 60000).unref();

const isPrivate = (ctx) => ctx.chat && ctx.chat.type === 'private';
const getUser = async (id) => (await pool.query('SELECT id, balance FROM users WHERE id = $1', [id])).rows[0] || null;

/* Создание заявки: холд баланса + запись — одной транзакцией */
async function createWithdrawal(uid, amount, wallet, username) {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', ['wd:' + uid]);   // два параллельных запроса одного игрока — по очереди
    const pend = await c.query("SELECT count(*)::int AS n FROM withdrawals WHERE user_id = $1 AND status = 'pending'", [uid]);
    if (pend.rows[0].n >= MAX_PENDING) { await c.query('ROLLBACK'); return { error: 'pending' }; }
    const u = await c.query('UPDATE users SET balance = balance - $2, updated_at = now() WHERE id = $1 AND balance >= $2 RETURNING balance', [uid, amount]);
    if (!u.rowCount) { await c.query('ROLLBACK'); return { error: 'funds' }; }
    const w = await c.query('INSERT INTO withdrawals (user_id, amount, wallet, username) VALUES ($1, $2, $3, $4) RETURNING id', [uid, amount, wallet, username]);
    await c.query('COMMIT');
    return { id: Number(w.rows[0].id), balance: Number(u.rows[0].balance) };
  } catch (e) { try { await c.query('ROLLBACK'); } catch (_) { /* ignore */ } throw e; } finally { c.release(); }
}

/* Статус меняется ТОЛЬКО из pending → повторное нажатие кнопки ничего не сделает (нет двойной выплаты/возврата) */
async function approve(id, adminId) {
  const r = await pool.query("UPDATE withdrawals SET status = 'paid', processed_at = now(), processed_by = $2 WHERE id = $1 AND status = 'pending' RETURNING *", [id, adminId]);
  return r.rows[0] || null;
}
async function reject(id, adminId) {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const r = await c.query("UPDATE withdrawals SET status = 'rejected', processed_at = now(), processed_by = $2 WHERE id = $1 AND status = 'pending' RETURNING *", [id, adminId]);
    if (!r.rowCount) { await c.query('ROLLBACK'); return null; }
    await c.query('UPDATE users SET balance = balance + $2, updated_at = now() WHERE id = $1', [r.rows[0].user_id, r.rows[0].amount]); // возврат
    await c.query('COMMIT');
    return r.rows[0];
  } catch (e) { try { await c.query('ROLLBACK'); } catch (_) { /* ignore */ } throw e; } finally { c.release(); }
}

const who = (row) => (row.username ? '@' + esc(row.username) : 'ID ' + esc(row.user_id));
const adminText = (row, tail) =>
  `Заявка #${row.id} на вывод от ${who(row)} (ID ${esc(row.user_id)}) на сумму <b>${fmt(row.amount)} TON</b>.\nКошелёк: <code>${esc(row.wallet)}</code>` + (tail ? `\n\n${tail}` : '');

/* ================= /start ================= */
bot.start(async (ctx) => {
  if (!isPrivate(ctx)) return;
  // ВАЖНО: параметр ?start=USER_ID намеренно ИГНОРИРУЕТСЯ — его может подставить кто угодно.
  // Личность игрока = ctx.from.id, который подписан Telegram'ом (совпадает с users.id у Telegram-игроков).
  const uid = String(ctx.from.id);
  sessions.delete(uid);
  try {
    const u = await getUser(uid);
    if (!u) return ctx.reply('Аккаунт не найден. Сначала откройте казино TonUp через Telegram, затем вернитесь сюда.');
    sessions.set(uid, { step: 'amount', at: Date.now() });
    await ctx.reply(`💎 Ваш баланс: <b>${fmt(u.balance)} TON</b>\nМинимальный вывод: ${fmt(MIN_WITHDRAW)} TON.\n\nВведите сумму вывода (например 5 или 2.5).\nОтмена: /cancel`, { parse_mode: 'HTML' });
  } catch (e) { console.error('[start]', e.message); ctx.reply('Ошибка сервера, попробуйте позже.'); }
});

bot.command('cancel', (ctx) => { sessions.delete(String(ctx.from.id)); return ctx.reply('Отменено. Чтобы начать заново — /start'); });

/* ================= сумма → кошелёк ================= */
bot.on('text', async (ctx) => {
  if (!isPrivate(ctx)) return;
  const uid = String(ctx.from.id);
  const text = ctx.message.text.trim();
  if (text.startsWith('/')) return;
  const st = sessions.get(uid);
  if (!st) return ctx.reply('Нажмите /start, чтобы создать заявку на вывод.');
  st.at = Date.now();
  try {
    if (st.step === 'amount') {
      if (!AMOUNT_RE.test(text)) return ctx.reply('Введите сумму числом, например 5 или 2.5');
      const amount = r6(parseFloat(text.replace(',', '.')));
      if (amount < MIN_WITHDRAW) return ctx.reply(`Минимальный вывод — ${fmt(MIN_WITHDRAW)} TON.`);
      const u = await getUser(uid);
      if (!u) { sessions.delete(uid); return ctx.reply('Аккаунт не найден.'); }
      if (Number(u.balance) < amount) return ctx.reply(`Недостаточно средств. Ваш баланс: ${fmt(u.balance)} TON.`);
      st.amount = amount; st.step = 'wallet';
      return ctx.reply('Теперь отправьте адрес TON-кошелька (начинается с UQ или EQ). Проверьте его внимательно — ошибка в адресе необратима.');
    }
    // step === 'wallet'
    if (!WALLET_RE.test(text)) return ctx.reply('Неверный формат адреса. Нужен адрес вида UQ… или EQ… (48 символов).');
    sessions.delete(uid);                                         // дальше повторно эту сессию использовать нельзя
    const res = await createWithdrawal(uid, st.amount, text, ctx.from.username || null);
    if (res.error === 'funds') return ctx.reply('Недостаточно средств. Начните заново: /start');
    if (res.error === 'pending') return ctx.reply(`У вас уже ${MAX_PENDING} необработанных заявки. Дождитесь их обработки.`);
    await ctx.reply(`✅ Заявка #${res.id} создана. Сумма ${fmt(st.amount)} TON заморожена на балансе до решения администратора.`);
    const row = { id: res.id, user_id: uid, username: ctx.from.username || null, amount: st.amount, wallet: text };
    await bot.telegram.sendMessage(ADMIN_ID, adminText(row), {
      parse_mode: 'HTML',
      ...Markup.inlineKeyboard([Markup.button.callback('✅ Одобрить', `wd:ok:${res.id}`), Markup.button.callback('❌ Отклонить', `wd:no:${res.id}`)]),
    }).catch((e) => console.error('[admin notify] НЕ ДОСТАВЛЕНО админу (он должен написать боту /start):', res.id, e.message));
  } catch (e) { console.error('[text]', e.message); ctx.reply('Ошибка сервера, попробуйте позже.'); }
});

/* ================= кнопки админа ================= */
bot.action(/^wd:(ok|no):(\d+)$/, async (ctx) => {
  if (String(ctx.from.id) !== ADMIN_ID) {                         // жёсткая проверка по ID нажавшего
    console.warn('[security] кнопку нажал не админ:', ctx.from.id);
    return ctx.answerCbQuery('⛔ Нет доступа', { show_alert: true });
  }
  const approveIt = ctx.match[1] === 'ok';
  const id = Number(ctx.match[2]);
  try {
    const row = approveIt ? await approve(id, ADMIN_ID) : await reject(id, ADMIN_ID);
    if (!row) {
      await ctx.answerCbQuery('Заявка уже обработана', { show_alert: true });
      return ctx.editMessageReplyMarkup(undefined).catch(() => {});
    }
    await ctx.answerCbQuery(approveIt ? 'Готово' : 'Отклонено');
    await ctx.editMessageText(adminText(row, approveIt ? '✅ Одобрено (paid)' : '❌ Отклонено, баланс возвращён'), { parse_mode: 'HTML' }).catch(() => {});
    await bot.telegram.sendMessage(row.user_id, approveIt ? 'Вывод успешно обработан ✅' : 'Вывод отклонен администратором ❌ Средства возвращены на баланс.')
      .catch((e) => console.warn('[notify user]', row.user_id, e.message));
  } catch (e) {
    console.error('[action]', e.message);
    ctx.answerCbQuery('Ошибка, попробуйте ещё раз', { show_alert: true }).catch(() => {});
  }
});

bot.catch((e) => console.error('[bot]', e && e.message));

/* ================= запуск: webhook (если есть публичный URL) или long polling ================= */
const secret = crypto.createHash('sha256').update(BOT_TOKEN).digest('hex').slice(0, 32);   // secret_token вебхука
const hook = bot.webhookCallback('/tg', { secretToken: secret });
const server = http.createServer((req, res) => {
  if (req.method === 'POST' && req.url === '/tg') return hook(req, res);
  res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('withdraw-bot ok');            // health-check для Render
});

(async () => {
  await pool.query(SCHEMA);
  server.listen(PORT, () => console.log('[withdraw] http :' + PORT));
  const base = (process.env.WEBHOOK_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '');
  if (base) {
    await bot.telegram.setWebhook(base + '/tg', { secret_token: secret, allowed_updates: ['message', 'callback_query'] });
    console.log('[withdraw] webhook:', base + '/tg');
  } else {
    await bot.telegram.deleteWebhook();
    bot.launch().catch((e) => { console.error('[launch]', e.message); process.exit(1); });
    console.log('[withdraw] long polling');
  }
})().catch((e) => { console.error('[withdraw] старт не удался:', e.message); process.exit(1); });

const stop = () => { try { bot.stop(); } catch (_) { /* webhook-режим */ } server.close(() => pool.end().finally(() => process.exit(0))); };
process.once('SIGTERM', stop); process.once('SIGINT', stop);
