'use strict';
/**
 * TonUp realtime server  —  Ракетка (crash) + PvP Solo + PvP TvT (Ice vs Fire)
 * + PostgreSQL (балансы), пополнение TON (Crypto Pay / прямой перевод через TON Connect),
 *   задания (проверка подписки через Telegram getChatMember), приём подарков (вебхук бота).
 *
 * Раунды игр по-прежнему живут в оперативной памяти, а ВСЕ деньги — в PostgreSQL:
 * каждое списание/начисление — атомарный UPDATE users SET balance = balance ± x.
 *
 * Зависимости: socket.io, pg      (npm i)
 * Запуск:      node server.js
 *
 * Переменные окружения:
 *   DATABASE_URL        (ОБЯЗАТЕЛЬНО) строка подключения PostgreSQL
 *   PGSSL               false — отключить SSL к БД (локальная разработка). По умолчанию SSL включён
 *   PORT                порт (Render задаёт сам)
 *   BOT_TOKEN           токен Telegram-бота: проверка initData, getChatMember, вебхук подарков
 *   ALLOWED_ORIGINS     домены фронтенда через запятую (по умолчанию *)
 *   START_BALANCE       стартовый баланс нового игрока (по умолчанию 0 — теперь баланс реальный)
 *   ALLOW_DEMO_SYNC     true — разрешить клиенту задавать баланс (ОПАСНО при реальных деньгах; по умолчанию false)
 *   ALLOW_GIFT_BETS     true — ставки подарками в TvT без списания (ОПАСНО; по умолчанию false)
 *   BONUS_X40_CHANCE    шанс бонуса x40 за раунд в TvT/PvP/Ball Race, 0..1 (по умолчанию 0.001 = 0.1%; 0 — выключить)
 *   BONUS_X40_MAX       максимум доплаты бонуса сверх обычного выигрыша, TON (по умолчанию 200)
 *   ADMIN_KEY           секрет для сброса истории раундов: POST /admin/reset-history с заголовком x-admin-key
 *   HISTORY_RESET_ON_START  true — очистить историю раундов при старте (после очистки верните в false)
 *
 *   --- Пополнение через Crypto Pay (@CryptoBot) ---
 *   CRYPTO_PAY_TOKEN    токен приложения из @CryptoBot -> Crypto Pay -> Create App
 *   CRYPTO_PAY_TESTNET  true — использовать @CryptoTestnetBot
 *   (вебхук в @CryptoBot: https://ВАШ-СЕРВЕР/cryptopay-webhook)
 *
 *   --- Пополнение прямым переводом TON (TON Connect + Toncenter) ---
 *   TON_HOUSE_WALLET    адрес кошелька проекта, куда приходят переводы
 *   TONCENTER_URL       по умолчанию https://toncenter.com  (тестнет: https://testnet.toncenter.com)
 *   TONCENTER_KEY       API-ключ Toncenter (необязательно, но без него лимит 1 запрос/сек)
 *
 *   --- Задания ---
 *   TASK_CHANNEL_ID     @username или -100… id канала   (бот должен быть админом канала)
 *   TASK_CHANNEL_URL    ссылка на канал для кнопки
 *   TASK_CHAT_ID        @username или -100… id чата     (бот должен быть в чате)
 *   TASK_CHAT_URL       ссылка на чат
 *   TASK_REWARD         награда за задание (по умолчанию 0.05)
 *
 *   --- Подарки (вебхук Telegram) ---
 *   TG_WEBHOOK_SECRET   секрет для заголовка X-Telegram-Bot-Api-Secret-Token
 *   PUBLIC_URL          публичный URL сервера; если задан вместе с BOT_TOKEN — вебхук регистрируется сам
 *   STAR_TON_RATE       цена 1 звезды в TON для обычных подарков (0 — обычные подарки не принимаем)
 *
 *   --- Вывод подарков и админ ---
 *   ADMIN_TG_IDS        Telegram ID админов через запятую (по умолчанию 1404680644). Только они видят заявки на вывод,
 *                       получают сообщения с кнопками «Принять / Отклонить» и админ-панель в приложении.
 *                       Админ должен хотя бы раз нажать /start у бота, иначе бот не сможет ему написать.
 *
 *   --- Пригласить друзей (сообщение с картинкой) ---
 *   BOT_USERNAME        username бота без @ (если пусто — берётся через getMe)
 *   WEBAPP_SHORT_NAME   short name Mini App из BotFather. Пусто = ссылка t.me/БОТ?startapp=… (нужен Main Mini App у бота)
 *   SHARE_TEXT          текст сообщения; SHARE_BUTTON — текст кнопки (по умолчанию «Получить»)
 *   SHARE_IMAGE_URL     https-ссылка на свою JPEG-картинку. Пусто = встроенный баннер (сервер отдаёт его по /share.jpg)
 */
const http = require('http');
const fs = require('fs');
const nodePath = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');
const { Pool } = require('pg');

/* ================= НАСТРОЙКИ ================= */
const PORT = Number(process.env.PORT) || 3000;
const ADMIN_KEY = process.env.ADMIN_KEY || '';                       // ключ для POST /admin/reset-history (пусто = отключено)
const HISTORY_RESET_ON_START = process.env.HISTORY_RESET_ON_START === 'true'; // true — очистить историю раундов при старте сервера
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const ORIGINS = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',').map((s) => s.trim()).filter(Boolean)
  : '*';
const START_BALANCE = process.env.START_BALANCE === undefined ? 0 : Number(process.env.START_BALANCE) || 0;
const ALLOW_DEMO_SYNC = process.env.ALLOW_DEMO_SYNC === 'true';
const ALLOW_GIFT_BETS = process.env.ALLOW_GIFT_BETS === 'true';
const AUTH_MAX_AGE_S = 60 * 60 * 24 * 2;

const DATABASE_URL = process.env.DATABASE_URL || '';
const CRYPTO_PAY_TOKEN = process.env.CRYPTO_PAY_TOKEN || '';
const CRYPTO_PAY_API = process.env.CRYPTO_PAY_TESTNET === 'true' ? 'https://testnet-pay.crypt.bot/api' : 'https://pay.crypt.bot/api';
const HOUSE_WALLET = process.env.TON_HOUSE_WALLET || 'UQACY26GcYX7nuL1iFd1wcDDQOubtLA90njWFRDzo-JVFrYm'; // кошелёк, на который приходят депозиты
const TONCENTER_URL = (process.env.TONCENTER_URL || 'https://toncenter.com').replace(/\/+$/, '');
const TONCENTER_KEY = process.env.TONCENTER_KEY || '';
const TG_WEBHOOK_SECRET = process.env.TG_WEBHOOK_SECRET || '';
const PUBLIC_URL = (process.env.PUBLIC_URL || '').replace(/\/+$/, '');
const STAR_TON_RATE = Number(process.env.STAR_TON_RATE) || 0;
const TASK_REWARD = Number(process.env.TASK_REWARD) || 0.05;
const DEPOSIT = { MIN: 0.1, MAX: 10000, MAX_PENDING: 5 };

/* Админ(ы) заявок на вывод: Telegram ID через запятую (env ADMIN_TG_IDS). По умолчанию — владелец проекта. */
const ADMIN_IDS = new Set(String(process.env.ADMIN_TG_IDS || '1404680644').split(',').map((x) => x.trim()).filter((x) => /^\d{1,15}$/.test(x)));
const WD_MAX_PENDING = 5; // максимум активных заявок на вывод у одного игрока

/* Приглашение друзей: готовое сообщение с картинкой (как в Mell Gifts) */
const SHARE_TEXT = process.env.SHARE_TEXT || 'Открывай Free Box в TonUp и забирай свои NFT-подарки!\n\nЭто на 100% бесплатно, можешь испытывать удачу каждые 24 часа!';
const SHARE_BUTTON = process.env.SHARE_BUTTON || 'Получить';
const SHARE_IMAGE_URL = process.env.SHARE_IMAGE_URL || ''; // свой JPEG по https; пусто = встроенный баннер по адресу <сервер>/share.jpg
const SELF_URL = PUBLIC_URL || String(process.env.RENDER_EXTERNAL_URL || '').replace(/\/+$/, '') || 'https://tonup-backend.onrender.com';

/* Реферальная система: реферер получает REF_PERCENT % от каждого депозита приглашённого (env REF_PERCENT, 0 = выключено) */
const REF_PERCENT = process.env.REF_PERCENT === undefined ? 35 : Math.max(0, Math.min(100, Number(process.env.REF_PERCENT) || 0));
let BOT_USERNAME = (process.env.BOT_USERNAME || '').replace(/^@/, '');   // если пусто — берём через getMe при старте
const WEBAPP_SHORT_NAME = process.env.WEBAPP_SHORT_NAME || '';           // short name Mini App в BotFather (если нет Main Mini App)

/* Промокоды на баланс: PROMO_CODES="TEST:10,WELCOME:1,VIP-ABCD:1000:1" — формат код:сумма_TON[:макс_активаций].
   Один код — один раз на игрока. Третье число (необязательное) — сколько раз код можно активировать ВСЕМИ игроками вместе
   (1 = одноразовый код). Без него лимита нет. Пустая строка = промокоды выключены. */
const PROMO_MAP = new Map(String(process.env.PROMO_CODES === undefined ? 'TEST:10' : process.env.PROMO_CODES).split(',')
  .map((x) => x.split(':').map((y) => y.trim())).filter((x) => x[0] && Number(x[1]) > 0)
  .map((x) => [x[0].toUpperCase(), { amount: Number(x[1]), max: Math.floor(Number(x[2])) > 0 ? Math.floor(Number(x[2])) : 0 }]));

/* Бесплатные кейсы (призы и условия — только на сервере, клиенту не доверяем) */
const CASE_PROMO = process.env.CASE_PROMO || '333';
const CASES = {
  DAILY_COOLDOWN_MS: 24 * 3600 * 1000,
  DEP_MIN: process.env.CASE_DEP_MIN === undefined ? 5 : Math.max(0, Number(process.env.CASE_DEP_MIN) || 0), // депозит за 24 часа для Free Кейса (для теста: CASE_DEP_MIN=0.1)
  DEP_WINDOW_MS: 24 * 3600 * 1000,
  GIFT_W: 0.1,                         // суммарный шанс подарка (до 5 TON) в Free Кейсе, %
  DAILY: [[0.04,1],[0.03,2],[0.02,4],[0.01,12],[0.005,26],[0.001,55]],
  // Платные кейсы: цена и призы (TON, шанс %). Сумма шансов = 100. RTP (возврат игрокам) считается ниже в логе при старте.
  PAID: {
    luxe: { price: 33, prizes: [[7137.99,0.0001,"Plush Pepe"],[429.96,0.0601,"Durov's Cap"],[282.44,0.1574,"Precious Peach"],[193.91,0.3725,"Heroic Helmet"],[144.07,0.7356,"Nail Bracelet"],[138.99,0.7986,"Loot Bag"],[129.18,0.9443,"Mighty Arm"],[86,2.3976,"Low Rider"],[83.28,2.5807,"Astral Shard"],[74.96,3.2842,"Gem Signet"],[66.3,4.3504,"Swiss Watch"],[18.17,84.3185,"Freedom Eagle"]] },
    signet: { price: 40, prizes: [[144.07,0.01,"Nail Bracelet"],[74.96,0.02,"Gem Signet"],[49.38,0.1,"Bonded Ring"],[39.94,10,"Diamond Ring"],[39.92,89.87,"Signet Ring"]] },
  },
  DEP: [[0.001,30],[0.005,22],[0.01,18],[0.02,10],[0.03,6],[0.04,4],[0.1,6],[0.25,2.5],[0.5,1],[1,0.4]],
};

/* Задания. Награда начисляется ОДИН раз на пользователя (PRIMARY KEY в task_claims). */
const TASKS = {
  channel: { title: 'Подписка на канал', reward: TASK_REWARD, chat: process.env.TASK_CHANNEL_ID || '', url: process.env.TASK_CHANNEL_URL || '' },
  chat: { title: 'Подписка на чат', reward: TASK_REWARD, chat: process.env.TASK_CHAT_ID || '', url: process.env.TASK_CHAT_URL || '' },
};

if (!DATABASE_URL) { console.error('[tonup] DATABASE_URL не задан — сервер не может работать без PostgreSQL'); process.exit(1); }

const CRASH = {
  WAIT_MS: 5000,        // ожидание ставок
  PAUSE_MS: 2500,       // пауза после вылета
  TICK_MS: 100,         // частота crash_tick
  GROWTH: 0.12,         // mult = e^(GROWTH * секунды)
  INSTANT_CHANCE: 0.12, // шанс мгновенного вылета x1.00
  MAX_MULT: 11,         // потолок множителя
  MAX_BET: 100000,
};
const TVT = {
  BET_SECONDS: 20,      // таймер приёма ставок
  LOCK_SECONDS: 2,      // за столько секунд до конца ставки закрываются
  SPIN_MS: 11300,       // длительность розыгрыша: анимация шара у клиентов 11 с + запас
  RESULT_MS: 10000,     // показ результата до нового раунда
  RAKE: 1,              // 1 = без комиссии
  MAX_BET: 100000,
};

/* Каталог подарков (значения — только отсюда, клиенту не доверяем) */
const GIFTS = [
  ["🐍","Lunar Snake",4.9],
  ["🍨","Vice Cream",4.96],
  ["📦","Snake Box",4.96],
  ["🍭","Lol Pop",4.97],
  ["🧁","Whip Cupcake",4.97],
  ["🔥","Chill Flame",4.98],
  ["🍜","Instant Ramen",4.98],
  ["🛟","Pool Float",4.99],
  ["🎊","Big Year",4.99],
  ["🍬","Candy Cane",5],
  ["🧦","Xmas Stocking",5],
  ["🍦","Ice Cream",5.04],
  ["🪔","Mirage Lamp",5.07],
  ["🥚","Easter Egg",5.12],
  ["🐍","Pet Snake",5.32],
  ["🍫","Happy Brownie",5.35],
  ["🤡","Jester Hat",5.36],
  ["🍭","Hypno Lollipop",5.36],
  ["🎁","Jack-in-the-Box",5.38],
  ["🎒","Mood Pack",5.42],
  ["🏅","Victory Medal",5.43],
  ["🥂","Holiday Drink",5.5],
  ["🧦","Fresh Socks",5.53],
  ["📖","Timeless Book",5.59],
  ["🎮","Tama Gadget",5.66],
  ["🍯","Money Pot",5.75],
  ["🍰","Mousse Cake",5.85],
  ["🍪","Ginger Cookie",5.86],
  ["📓","Star Notepad",5.88],
  ["🍲","Hex Pot",5.93],
  ["🎂","Homemade Cake",5.94],
  ["🕯","B-Day Candle",6],
  ["🎄","Winter Wreath",6],
  ["🎇","Party Sparkler",6.1],
  ["🍷","Spiced Wine",6.12],
  ["🚀","Stellar Rocket",6.13],
  ["🍪","Cookie Heart",6.33],
  ["🧙","Witch Hat",6.38],
  ["🍀","Clover Pin",6.41],
  ["🌸","Pretty Posy",6.41],
  ["📅","Desk Calendar",6.49],
  ["🫙","Restless Jar",6.66],
  ["🧺","Spring Basket",6.84],
  ["💐","Lush Bouquet",6.84],
  ["🗽","Liberty Figure",6.85],
  ["🎀","Bow Tie",6.85],
  ["🐶","Snoop Dogg",6.87],
  ["🛍","Swag Bag",6.99],
  ["📿","Faith Amulet",6.99],
  ["🕯","Eternal Candle",7.29],
  ["🔑","Input Key",7.54],
  ["⚔️","Light Sword",7.58],
  ["🌙","Moon Pendant",7.68],
  ["🍄","Spy Agaric",8.16],
  ["🏄","Surge Board",8.18],
  ["❄️","Snow Globe",8.63],
  ["🎅","Santa Hat",9.29],
  ["🖋","Fine Pen",9.48],
  ["🎁","Joyful Bundle",9.73],
  ["🧿","Evil Eye",9.74],
  ["🐵","Jolly Chimp",9.78],
  ["🧤","Snow Mittens",9.88],
  ["🍓","Berry Box",10.74],
  ["🔔","Sleigh Bell",10.75],
  ["🧁","Bunny Muffin",10.76],
  ["🐰","Jelly Bunny",11],
  ["⭐","Hanging Star",11.42],
  ["🎩","Top Hat",11.5],
  ["🕯","Love Candle",13.28],
  ["🎃","Mad Pumpkin",13.34],
  ["💌","Valentine Box",14.42],
  ["🥀","Skull Flower",14.7],
  ["🧹","Flying Broom",14.73],
  ["📻","Record Player",15.13],
  ["🔮","Crystal Ball",15.83],
  ["🧪","Love Potion",17.96],
  ["🚬","Snoop Cigar",18.66],
  ["💨","Ionic Dryer",18.97],
  ["🥊","UFC Strike",19.97],
  ["💔","Trapped Heart",21.19],
  ["👠","Sky Stilettos",24.97],
  ["💘","Cupid Charm",27.23],
  ["🍼","Bling Binky",30.49],
  ["🪖","Khabib's Papakha",31.94],
  ["💀","Electric Skull",33.97],
  ["🌹","Eternal Rose",33.98],
  ["💍","Signet Ring",39.35],
  ["🪔","Genie Lamp",39.6],
  ["🧸","Toy Bear",41],
  ["💍","Diamond Ring",41.39],
  ["🪆","Voodoo Doll",44.95],
  ["🐱","Neko Helmet",45],
  ["🚬","Vintage Cigar",47.63],
  ["💍","Bonded Ring",52.42],
  ["👅","Sharp Tongue",54.08],
  ["🐸","Kissed Frog",57.19],
  ["⚗️","Magic Potion",69.99],
  ["💎","Gem Signet",73.44],
  ["🎬","Mini Oscar",79.57],
  ["🧱","Artisan Brick",81.89],
  ["💎","Ion Gem",86.29],
  ["🚗","Low Rider",88.44],
  ["🧴","Perfume Bottle",94.04],
  ["💪","Mighty Arm",130.42],
  ["🤟","Westside Sign",138.37],
  ["📿","Nail Bracelet",142.57],
  ["💠","Astral Shard",143.49],
  ["🕶","Durov's Glasses",145.51],
  ["💰","Loot Bag",147.98],
  ["🙀","Scared Cat",295.32],
  ["🍑","Precious Peach",295.84],
  ["🧢","Durov's Cap",428.57],
  ["🏆","Algorithm Cup",1470],
  ["🏆","Intelligence Cup",1575],
  ["💝","Heart Locket",1599.99],
  ["🐸","Plush Pepe",7139.99],
];
const GIFT_BY_NAME = new Map(GIFTS.map((g) => [g[1], { ico: g[0], name: g[1], v: g[2] }]));

/* ================= УТИЛИТЫ ================= */
const r2 = (n) => Math.round(n * 100) / 100;
const r6 = (n) => Math.round(n * 1e6) / 1e6;
const RAND_MAX = 281474976710655; // 2^48 - 1
const rnd = () => crypto.randomInt(0, RAND_MAX) / RAND_MAX; // криптостойкий random [0,1)
/* Бонус x40 (TvT, PvP, Ball Race): с шансом BONUS_X40_CHANCE за раунд победитель-ИГРОК получает свою ставку ×40.
   Разницу (до x40) платит проект, не другие игроки. Выплата ограничена BONUS_X40_MAX TON сверх обычного выигрыша. */
const BONUS = {
  X: 40,
  CHANCE: Math.max(0, Math.min(1, process.env.BONUS_X40_CHANCE === undefined ? 0.001 : Number(process.env.BONUS_X40_CHANCE) || 0)),
  MAX_EXTRA: Number(process.env.BONUS_X40_MAX) > 0 ? Number(process.env.BONUS_X40_MAX) : 200,
};
const bonusRoll = () => BONUS.CHANCE > 0 && rnd() < BONUS.CHANCE;
function bonusAward(id, game, stake) {
  const extra = r6(Math.min((BONUS.X - 1) * stake, BONUS.MAX_EXTRA));
  if (!(extra > 0)) return 0;
  creditUser(id, extra, 'win', { game, amount: extra, bonus: BONUS.X });
  io.to('u:' + id).emit('bonus_x', { game, x: BONUS.X, extra, stake: r6(stake) });
  console.log('[bonus x' + BONUS.X + '] ' + game + ' ' + id + ' ставка ' + stake + ' -> +' + extra);
  return extra;
}
const pick = (a) => a[Math.floor(rnd() * a.length)];
const hashStr = (t) => { let h = 0; for (let i = 0; i < t.length; i++) h = (h * 31 + t.charCodeAt(i)) >>> 0; return h; };
/* Имена/эмодзи попадают в innerHTML у других игроков — вычищаем всё опасное */
const clean = (v, max) => String(v == null ? '' : v).replace(/[<>&"'`\\\u0000-\u001f]/g, '').trim().slice(0, max);
const cleanEmoji = (v) => Array.from(clean(v, 16)).slice(0, 3).join('');
const throttle = (fn, ms) => { let t = null; return () => { if (t) return; t = setTimeout(() => { t = null; fn(); }, ms); }; };
const reply = (cb, obj) => { if (typeof cb === 'function') cb(obj); };

const AVATARS = ['🦊', '🐼', '🐙', '🦄', '🐲', '🐺', '🦅', '🐻', '👻', '🧔'];
const COLORS = ['#8b5cf6', '#eab308', '#22c55e', '#ef4444', '#06b6d4', '#f97316', '#ec4899', '#3b82f6'];

function parseAmount(v, max) {
  const n = Math.round(Number(v) * 100) / 100;
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.min(n, max);
}

/* ================= POSTGRESQL ================= */
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PGSSL === 'false' ? false : { rejectUnauthorized: false }, // Render/Neon/Supabase требуют SSL
  max: 10,
  idleTimeoutMillis: 30000,
});
pool.on('error', (e) => console.error('[pg] idle client error:', e.message));

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS users (
  id          TEXT PRIMARY KEY,                       -- Telegram ID (строкой) или g_<гость>
  name        TEXT NOT NULL DEFAULT 'Игрок',
  balance     NUMERIC(20,6) NOT NULL DEFAULT 0 CHECK (balance >= 0),
  ton_wallet  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS deposits (
  id         BIGSERIAL PRIMARY KEY,
  user_id    TEXT NOT NULL REFERENCES users(id),
  provider   TEXT NOT NULL CHECK (provider IN ('cryptopay','toncenter','gift')),
  ext_id     TEXT NOT NULL,                           -- invoice_id | memo | ключ подарка
  amount     NUMERIC(20,6) NOT NULL,
  status     TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','paid','expired')),
  pay_url    TEXT,
  tx_hash    TEXT UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  paid_at    TIMESTAMPTZ,
  UNIQUE (provider, ext_id)
);
CREATE INDEX IF NOT EXISTS deposits_pending_idx ON deposits (provider, status) WHERE status = 'pending';
CREATE TABLE IF NOT EXISTS task_claims (
  user_id    TEXT NOT NULL REFERENCES users(id),
  task_id    TEXT NOT NULL,
  reward     NUMERIC(20,6) NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, task_id)
);
CREATE TABLE IF NOT EXISTS round_history (
  id         BIGSERIAL PRIMARY KEY,
  game       TEXT NOT NULL,                            -- rk | solo | tvt | up | dice | mines | plinko | spins | craft
  user_id    TEXT,                                     -- NULL = общая запись раунда (rk/solo/tvt), иначе личная запись игрока
  rid        TEXT,                                     -- номер раунда (для rk/solo/tvt)
  rec        JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS round_history_pub_uq ON round_history (game, rid) WHERE user_id IS NULL AND rid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS round_history_my_uq  ON round_history (game, rid, user_id) WHERE user_id IS NOT NULL AND rid IS NOT NULL;
CREATE INDEX IF NOT EXISTS round_history_game_idx ON round_history (game, id DESC);
CREATE INDEX IF NOT EXISTS round_history_user_idx ON round_history (user_id, game, id DESC);
ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer_id TEXT;
CREATE INDEX IF NOT EXISTS users_referrer_idx ON users (referrer_id) WHERE referrer_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS referral_earnings (
  id          BIGSERIAL PRIMARY KEY,
  referrer_id TEXT NOT NULL REFERENCES users(id),
  user_id     TEXT NOT NULL REFERENCES users(id),
  deposit_id  BIGINT UNIQUE,                          -- один депозит = одно начисление
  amount      NUMERIC(20,6) NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS referral_earnings_ref_idx ON referral_earnings (referrer_id);
CREATE TABLE IF NOT EXISTS case_opens (
  id        BIGSERIAL PRIMARY KEY,
  user_id   TEXT NOT NULL REFERENCES users(id),
  case_id   TEXT NOT NULL,
  prize     NUMERIC(20,6) NOT NULL,
  gift      TEXT,
  opened_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS promo_redeems (
  user_id     TEXT NOT NULL REFERENCES users(id),
  code        TEXT NOT NULL,
  amount      NUMERIC(20,6) NOT NULL,
  redeemed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, code)
);
ALTER TABLE case_opens DROP CONSTRAINT IF EXISTS case_opens_case_id_check;  -- разрешаем платные кейсы
CREATE INDEX IF NOT EXISTS case_opens_user_idx ON case_opens (user_id, case_id, opened_at DESC);
ALTER TABLE users ADD COLUMN IF NOT EXISTS tg_username TEXT;
-- Очки игроков по периодам (период = 30 дней, считается от epoch, поэтому сброс у всех одновременно)
CREATE TABLE IF NOT EXISTS lb_scores (
  period  BIGINT NOT NULL,
  user_id TEXT   NOT NULL REFERENCES users(id),
  score   NUMERIC(20,2) NOT NULL DEFAULT 0,
  PRIMARY KEY (period, user_id)
);
CREATE INDEX IF NOT EXISTS lb_scores_top_idx ON lb_scores (period, score DESC);

-- Ежедневный бонус за вход: один раз в сутки на игрока
CREATE TABLE IF NOT EXISTS lb_login_days (
  user_id TEXT NOT NULL REFERENCES users(id),
  day     DATE NOT NULL,
  PRIMARY KEY (user_id, day)
);
CREATE TABLE IF NOT EXISTS withdrawals (
  id          BIGSERIAL PRIMARY KEY,
  user_id     TEXT NOT NULL REFERENCES users(id),
  kind        TEXT NOT NULL DEFAULT 'gift',
  gift_name   TEXT NOT NULL,
  gift_ico    TEXT,
  gift_value  NUMERIC(20,6) NOT NULL DEFAULT 0,
  verified    BOOLEAN NOT NULL DEFAULT TRUE,             -- подарок найден в серверном каталоге
  status      TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected')),
  acked       BOOLEAN NOT NULL DEFAULT FALSE,            -- клиент подтвердил, что получил решение
  admin_msgs  JSONB NOT NULL DEFAULT '[]',               -- сообщения админам в боте (для обновления после решения)
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at  TIMESTAMPTZ,
  decided_by  TEXT
);
CREATE INDEX IF NOT EXISTS withdrawals_user_idx ON withdrawals (user_id, id DESC);
CREATE INDEX IF NOT EXISTS withdrawals_pending_idx ON withdrawals (status) WHERE status = 'pending';`;
const initDb = () => pool.query(SCHEMA_SQL);

async function withTx(fn) {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const out = await fn(c);
    await c.query('COMMIT');
    return out;
  } catch (e) {
    try { await c.query('ROLLBACK'); } catch (_) { /* ignore */ }
    throw e;
  } finally { c.release(); }
}

/* Атомарное изменение баланса. delta<0 — списание (вернёт null, если не хватает денег). */
async function adjust(id, delta) {
  const r = await pool.query(
    'UPDATE users SET balance = balance + $2, updated_at = now() WHERE id = $1 AND balance + $2 >= 0 RETURNING balance',
    [id, r6(delta)]
  );
  return r.rowCount ? Number(r.rows[0].balance) : null;
}

/* ================= ИГРОКИ (кэш сокетов; деньги — только в БД) ================= */
const players = new Map(); // id -> { id, name, av, balance(кэш), tonWallet, sockets:Set, lastSeen, chain }

/* Ищем игрока в БД; нет — создаём со стартовым балансом */
async function loadPlayer(id, name, av) {
  const r = await pool.query(
    `INSERT INTO users (id, name, balance) VALUES ($1, $2, $3)
     ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()
     RETURNING balance, ton_wallet`,
    [id, name, START_BALANCE]
  );
  let p = players.get(id);
  if (!p) {
    p = { id, name, av: av || AVATARS[hashStr(id) % AVATARS.length], balance: 0, tonWallet: null, sockets: new Set(), lastSeen: Date.now(), chain: Promise.resolve() };
    players.set(id, p);
  }
  p.balance = Number(r.rows[0].balance);
  p.tonWallet = r.rows[0].ton_wallet;
  return p;
}
const isTgUser = (p) => !!BOT_TOKEN && /^\d{1,15}$/.test(p.id); // реальные деньги — только проверенным Telegram-пользователям

/* Операции с деньгами одного игрока выполняются строго по очереди (защита от двойных кликов) */
function queue(p, fn, cb) {
  p.chain = p.chain.then(fn).catch((e) => {
    console.error('[queue]', p.id, e && e.message);
    reply(cb, { ok: false, error: 'Ошибка сервера, попробуйте ещё раз' });
  });
}

const LB_PERIOD = 30 * 86400000;
const lbKey = () => Math.floor(Date.now() / LB_PERIOD);
const LB_MULT = { deposit: 10, bet: 2, case: 1 };   // как в клиенте: dep×10, pvp×2, case×1, плюс +1 за действие
let lbCache = { key: 0, at: 0, rows: [] };

async function lbAdd(id, pts) {
  pts = Math.round(pts * 100) / 100;
  if (!(pts > 0) || !/^\d{1,15}$/.test(String(id))) return; // только реальные Telegram-игроки: без гостей и ботов
  try {
    await pool.query(
      `INSERT INTO lb_scores (period, user_id, score) VALUES ($1, $2, $3)
       ON CONFLICT (period, user_id) DO UPDATE SET score = lb_scores.score + EXCLUDED.score`,
      [lbKey(), id, pts]);
  } catch (e) { console.error('[lbAdd]', e.message); }
}


function sendBalance(p, reason, extra) {
  if (extra && extra.amount > 0 && LB_MULT[reason]) lbAdd(p.id, extra.amount * LB_MULT[reason] + 1); // очки рейтинга: только за реальные ставки/депозиты
  io.to('u:' + p.id).emit('balance', Object.assign({ balance: p.balance, reason }, extra || {}));
}
/* Начисление по id (игрок может быть офлайн). Повторяет при сбое БД — выигрыш не должен потеряться. */
async function creditUser(id, amount, reason, extra) {
  amount = r6(amount);
  if (!(amount > 0)) return null;
  for (let i = 0; i < 4; i++) {
    try {
      const nb = await adjust(id, amount);
      if (nb === null) { console.error('[credit] пользователь не найден:', id, amount); return null; }
      const p = players.get(id);
      if (p) { p.balance = nb; sendBalance(p, reason, extra); }
      return nb;
    } catch (e) {
      console.error('[credit] ошибка, попытка', i + 1, id, amount, e.message);
      await new Promise((r) => setTimeout(r, 500 * (i + 1)));
    }
  }
  console.error('[credit] !!! НЕ НАЧИСЛЕНО (внести вручную):', id, amount, reason);
  return null;
}
setInterval(() => { // чистим давно ушедших игроков из кэша (в БД они остаются)
  const limit = Date.now() - 1000 * 60 * 60 * 24 * 7;
  for (const [id, p] of players) if (!p.sockets.size && p.lastSeen < limit) players.delete(id);
}, 60 * 60 * 1000).unref();

/* ================= TELEGRAM initData ================= */
function validateInitData(initData) {
  try {
    const params = new URLSearchParams(initData);
    const hash = params.get('hash');
    if (!hash) return null;
    params.delete('hash');
    const dcs = [...params.entries()].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)).map(([k, v]) => k + '=' + v).join('\n');
    const secret = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const calc = crypto.createHmac('sha256', secret).update(dcs).digest();
    const got = Buffer.from(hash, 'hex');
    if (got.length !== calc.length || !crypto.timingSafeEqual(got, calc)) return null;
    if (Date.now() / 1000 - Number(params.get('auth_date') || 0) > AUTH_MAX_AGE_S) return null;
    return JSON.parse(params.get('user') || 'null');
  } catch (e) { return null; }
}
/* Определяем, кто это. Без BOT_TOKEN доверяем id из клиента (режим разработки). */
function resolveIdentity(data) {
  data = data && typeof data === 'object' ? data : {};
  const rawId = String(data.id == null ? '' : data.id);
  if (BOT_TOKEN) {
    if (typeof data.initData === 'string' && data.initData) {
      const u = validateInitData(data.initData);
      if (!u || !u.id) return null;
      return { id: String(u.id), name: clean(data.name || u.first_name || u.username, 24) || 'Игрок', av: cleanEmoji(data.av), username: clean(u.username, 40) };
    }
    // вне Telegram: гость, id изолирован префиксом и не пересекается с реальными Telegram-ID
    if (!/^[A-Za-z0-9_\-]{4,40}$/.test(rawId)) return null;
    return { id: 'g_' + rawId, name: clean(data.name, 24) || 'Гость', av: cleanEmoji(data.av) };
  }
  if (!/^[A-Za-z0-9_\-:.]{1,40}$/.test(rawId)) return null;
  return { id: rawId, name: clean(data.name, 24) || 'Игрок', av: cleanEmoji(data.av) };
}

/* ================= ВНЕШНИЕ API (Telegram, Crypto Pay, Toncenter) ================= */
async function httpJson(url, opts) {
  const res = await fetch(url, Object.assign({ signal: AbortSignal.timeout(10000) }, opts));
  let j = null;
  try { j = await res.json(); } catch (_) { /* не JSON */ }
  if (!res.ok && !j) throw new Error('HTTP ' + res.status);
  return j;
}
const tgApi = (method, body) => httpJson('https://api.telegram.org/bot' + BOT_TOKEN + '/' + method, {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}),
});
const cpApi = (method, params, post) => httpJson(
  CRYPTO_PAY_API + '/' + method + (post ? '' : '?' + new URLSearchParams(params).toString()),
  post
    ? { method: 'POST', headers: { 'Crypto-Pay-API-Token': CRYPTO_PAY_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(params) }
    : { headers: { 'Crypto-Pay-API-Token': CRYPTO_PAY_TOKEN } }
);

/* ================= ПОПОЛНЕНИЕ ================= */
/* Зачисляет депозит ровно один раз: UPDATE ... WHERE status='pending' + начисление в одной транзакции */
async function settleDeposit(provider, extId, amount, txHash) {
  const out = await withTx(async (c) => {
    const d = await c.query(
      `UPDATE deposits SET status = 'paid', amount = COALESCE($3, amount), tx_hash = $4, paid_at = now()
       WHERE provider = $1 AND ext_id = $2 AND status = 'pending' RETURNING id, user_id, amount`,
      [provider, extId, amount == null ? null : r6(amount), txHash || null]
    );
    if (!d.rowCount) return null; // уже зачислен или неизвестен
    const u = await c.query('UPDATE users SET balance = balance + $2, updated_at = now() WHERE id = $1 RETURNING balance, referrer_id',
      [d.rows[0].user_id, d.rows[0].amount]);
    // Реферальный бонус: тот же транзакции, что и депозит — начисляется ровно один раз (UNIQUE deposit_id)
    let ref = null;
    const refId = u.rows[0].referrer_id;
    if (REF_PERCENT > 0 && refId && (provider === 'cryptopay' || provider === 'toncenter')) {
      const bonus = r6(Number(d.rows[0].amount) * REF_PERCENT / 100);
      if (bonus > 0) {
        const ins = await c.query(
          'INSERT INTO referral_earnings (referrer_id, user_id, deposit_id, amount) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING',
          [refId, d.rows[0].user_id, d.rows[0].id, bonus]);
        if (ins.rowCount) {
          const rb = await c.query('UPDATE users SET balance = balance + $2, updated_at = now() WHERE id = $1 RETURNING balance', [refId, bonus]);
          ref = { id: refId, amount: bonus, balance: Number(rb.rows[0].balance) };
        }
      }
    }
    return { userId: d.rows[0].user_id, amount: Number(d.rows[0].amount), balance: Number(u.rows[0].balance), ref };
  });
  if (out) {
    console.log('[deposit] +' + out.amount + ' TON -> ' + out.userId + ' (' + provider + ' ' + extId + ')');
    const p = players.get(out.userId);
    if (p) { p.balance = out.balance; sendBalance(p, 'deposit', { amount: out.amount }); }
    if (out.ref) {
      console.log('[referral] +' + out.ref.amount + ' TON -> ' + out.ref.id + ' (от депозита ' + out.userId + ')');
      const rp = players.get(out.ref.id);
      if (rp) { rp.balance = out.ref.balance; sendBalance(rp, 'ref', { amount: out.ref.amount }); }
    }
  }
  return out;
}

/* ---- Crypto Pay ---- */
async function createCryptoInvoice(p, amount) {
  if (!CRYPTO_PAY_TOKEN) throw new Error('Crypto Pay не настроен');
  const j = await cpApi('createInvoice', {
    asset: 'TON', amount: String(amount), description: 'Пополнение TonUp', payload: p.id,
    expires_in: 3600, allow_comments: false, allow_anonymous: false,
  }, true);
  if (!j || !j.ok) throw new Error('Crypto Pay: ' + JSON.stringify(j && j.error));
  const inv = j.result;
  const url = inv.bot_invoice_url || inv.pay_url;
  await pool.query(`INSERT INTO deposits (user_id, provider, ext_id, amount, pay_url) VALUES ($1,'cryptopay',$2,$3,$4)`,
    [p.id, String(inv.invoice_id), amount, url]);
  return url;
}
async function pollCryptoPay() {
  if (!CRYPTO_PAY_TOKEN) return;
  const r = await pool.query(`SELECT ext_id FROM deposits WHERE provider='cryptopay' AND status='pending' ORDER BY id DESC LIMIT 100`);
  if (!r.rowCount) return;
  const j = await cpApi('getInvoices', { asset: 'TON', invoice_ids: r.rows.map((x) => x.ext_id).join(',') });
  if (!j || !j.ok) return;
  const items = Array.isArray(j.result) ? j.result : (j.result && j.result.items) || [];
  for (const inv of items) {
    if (inv.status === 'paid' && inv.asset === 'TON') await settleDeposit('cryptopay', String(inv.invoice_id), null, null);
    else if (inv.status === 'expired') await pool.query(`UPDATE deposits SET status='expired' WHERE provider='cryptopay' AND ext_id=$1 AND status='pending'`, [String(inv.invoice_id)]);
  }
}
/* Подпись вебхука Crypto Pay: HMAC-SHA256(body, SHA256(token)) в заголовке crypto-pay-api-signature */
function verifyCryptoPaySignature(raw, sig) {
  if (!CRYPTO_PAY_TOKEN || !sig) return false;
  const key = crypto.createHash('sha256').update(CRYPTO_PAY_TOKEN).digest();
  const calc = crypto.createHmac('sha256', key).update(raw).digest();
  const got = Buffer.from(String(sig), 'hex');
  return got.length === calc.length && crypto.timingSafeEqual(got, calc);
}

/* ---- Прямой перевод TON (TON Connect -> кошелёк проекта, комментарий = memo) ---- */
/* BOC с одной ячейкой-комментарием (op=0 + текст). Нужен для TON Connect sendTransaction.payload */
function commentPayloadBase64(text) {
  const data = Buffer.concat([Buffer.alloc(4), Buffer.from(text, 'utf8')]);
  if (data.length > 127) throw new Error('comment too long');
  const cell = Buffer.concat([Buffer.from([0x00, data.length * 2]), data]);
  const boc = Buffer.concat([
    Buffer.from([0xb5, 0xee, 0x9c, 0x72, 0x01, 0x01, 0x01, 0x01, 0x00, cell.length, 0x00]), cell,
  ]);
  return boc.toString('base64');
}
async function createTonDeposit(p, amount) {
  if (!HOUSE_WALLET) throw new Error('Прямой перевод TON не настроен');
  const memo = 'tonup' + crypto.randomBytes(8).toString('hex');
  await pool.query(`INSERT INTO deposits (user_id, provider, ext_id, amount) VALUES ($1,'toncenter',$2,$3)`, [p.id, memo, amount]);
  const nano = BigInt(Math.round(amount * 1e9)).toString();
  return {
    address: HOUSE_WALLET, memo, nano, payload: commentPayloadBase64(memo),
    link: 'ton://transfer/' + HOUSE_WALLET + '?amount=' + nano + '&text=' + memo,
  };
}
async function pollToncenter() {
  if (!HOUSE_WALLET) return;
  const pend = await pool.query(`SELECT ext_id FROM deposits WHERE provider='toncenter' AND status='pending'`);
  if (!pend.rowCount) return;
  const memos = new Set(pend.rows.map((x) => x.ext_id));
  const j = await httpJson(TONCENTER_URL + '/api/v2/getTransactions?limit=50&archival=false&address=' + encodeURIComponent(HOUSE_WALLET),
    TONCENTER_KEY ? { headers: { 'X-API-Key': TONCENTER_KEY } } : {});
  if (!j || !j.ok || !Array.isArray(j.result)) return;
  for (const t of j.result) {
    const m = t.in_msg;
    if (!m || !m.source || !m.message || !memos.has(m.message)) continue; // только входящие переводы с нашим memo
    const ton = Number(m.value) / 1e9;
    const hash = t.transaction_id && t.transaction_id.hash;
    if (!(ton > 0) || !hash) continue;
    try { await settleDeposit('toncenter', m.message, ton, hash); } catch (e) { console.error('[toncenter]', e.message); }
  }
  await pool.query(`UPDATE deposits SET status='expired' WHERE provider='toncenter' AND status='pending' AND created_at < now() - interval '24 hours'`);
}
let pollBusy = false;
async function pollPayments() {
  if (pollBusy) return;
  pollBusy = true;
  try { await pollCryptoPay(); } catch (e) { console.error('[poll cryptopay]', e.message); }
  try { await pollToncenter(); } catch (e) { console.error('[poll toncenter]', e.message); }
  pollBusy = false;
}

/* ================= РЕФЕРАЛЫ ================= */
const startParamOf = (initData) => { try { return new URLSearchParams(initData).get('start_param') || ''; } catch (_) { return ''; } };
const refLink = (id) => BOT_USERNAME ? 'https://t.me/' + BOT_USERNAME + (WEBAPP_SHORT_NAME ? '/' + WEBAPP_SHORT_NAME : '') + '?startapp=ref_' + id : '';
/* Привязка реферера: только один раз, не к себе, без прямой петли A<->B и только пока у игрока нет оплаченных депозитов */
async function bindReferrer(userId, startParam) {
  const m = /^ref[_-]?(\d{1,15})$/i.exec(startParam || '');
  if (!m || m[1] === userId) return;
  await pool.query(
    `UPDATE users u SET referrer_id = $2, updated_at = now()
     WHERE u.id = $1 AND u.referrer_id IS NULL
       AND EXISTS (SELECT 1 FROM users r WHERE r.id = $2 AND r.referrer_id IS DISTINCT FROM $1)
       AND NOT EXISTS (SELECT 1 FROM deposits d WHERE d.user_id = $1 AND d.status = 'paid' AND d.provider <> 'gift')`,
    [userId, m[1]]);
}

/* ================= ВЫВОД ПОДАРКОВ: заявки -> админ (Telegram ID из ADMIN_TG_IDS) ================= */
/* Игрок отправляет заявку -> подарок блокируется у него в инвентаре/профиле -> админ получает сообщение в боте
   (кнопки «Принять / Отклонить») и видит заявку в админ-панели приложения. Решает только админ. */
const wdCode = (id) => 'W-' + (1000 + Number(id));
const isAdmin = (p) => !!p && isTgUser(p) && ADMIN_IDS.has(p.id);
const escHtml = (t) => String(t == null ? '' : t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const WD_SELECT = `SELECT w.*, u.name AS user_name, u.tg_username, u.balance AS user_balance,
  COALESCE((SELECT sum(d.amount) FROM deposits d WHERE d.user_id = w.user_id AND d.status = 'paid' AND d.provider <> 'gift'), 0) AS user_deposits
  FROM withdrawals w JOIN users u ON u.id = w.user_id`;
const wdOut = (r) => ({
  id: Number(r.id), code: wdCode(r.id), userId: r.user_id, userName: r.user_name || '', username: r.tg_username || '',
  name: r.gift_name, ico: r.gift_ico || '🎁', v: Number(r.gift_value), verified: !!r.verified, status: r.status,
  balance: r.user_balance == null ? null : Number(r.user_balance), deposits: r.user_deposits == null ? null : Number(r.user_deposits),
  at: new Date(r.created_at).getTime(), decidedAt: r.decided_at ? new Date(r.decided_at).getTime() : null,
});
const wdUserOut = (r) => ({ id: Number(r.id), code: wdCode(r.id), name: r.gift_name, ico: r.gift_ico || '🎁', v: Number(r.gift_value), status: r.status });

async function tgSend(chatId, text, extra) {
  if (!BOT_TOKEN) return null;
  try {
    const j = await tgApi('sendMessage', Object.assign({ chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true }, extra || {}));
    if (j && j.ok) return j.result;
    console.warn('[tg] sendMessage ->', chatId, j && j.description);
  } catch (e) { console.warn('[tg] sendMessage ->', chatId, e.message); }
  return null;
}

function wdAdminText(w, decision) {
  const who = escHtml(w.user_name || 'Игрок') + (w.tg_username ? ' (@' + escHtml(w.tg_username) + ')' : '');
  let t = '📤 <b>Заявка на вывод ' + wdCode(w.id) + '</b>\n\n'
    + '👤 ' + who + '\n'
    + '🆔 <code>' + escHtml(w.user_id) + '</code>\n'
    + '🎁 <b>' + escHtml(w.gift_ico || '') + ' ' + escHtml(w.gift_name) + '</b> — ' + r2(Number(w.gift_value)) + ' TON\n';
  if (!w.verified) t += '⚠️ Подарка нет в каталоге — стоимость указал сам игрок\n';
  t += '💰 Баланс: ' + r2(Number(w.user_balance)) + ' TON · депозитов: ' + r2(Number(w.user_deposits)) + ' TON\n';
  t += '🕒 ' + new Date(w.created_at).toISOString().replace('T', ' ').slice(0, 16) + ' UTC';
  if (decision) t += '\n\n' + decision;
  return t;
}

/* Новая заявка: сообщение админам в боте (с кнопками) + событие в админ-панель приложения */
async function notifyAdminsNewWd(id) {
  const r = await pool.query(WD_SELECT + ' WHERE w.id = $1', [id]);
  if (!r.rowCount) return;
  const w = r.rows[0];
  io.to('admin').emit('adm_new', wdOut(w));
  const decide = [{ text: '✅ Принять', callback_data: 'wd:a:' + w.id }, { text: '❌ Отклонить', callback_data: 'wd:r:' + w.id }];
  const full = { inline_keyboard: [decide, [{ text: '👤 Игрок', url: 'tg://user?id=' + w.user_id }]] };
  const base = { inline_keyboard: [decide] };
  const sent = [];
  for (const aid of ADMIN_IDS) {
    // кнопка tg://user иногда блокируется настройками приватности — тогда шлём без неё
    const m = (await tgSend(aid, wdAdminText(w), { reply_markup: full })) || (await tgSend(aid, wdAdminText(w), { reply_markup: base }));
    if (m) sent.push({ c: String(aid), m: m.message_id });
    else console.warn('[withdraw] админу ' + aid + ' не доставлено сообщение — он должен нажать /start у бота. Заявка видна в админ-панели приложения.');
  }
  if (sent.length) await pool.query('UPDATE withdrawals SET admin_msgs = $2::jsonb WHERE id = $1', [id, JSON.stringify(sent)]);
}

/* Игрок: создать заявку. Подарок на стороне клиента блокируется сразу после успешного ответа. */
async function wdCreate(p, data) {
  data = data && typeof data === 'object' ? data : {};
  const rawName = String(data.name == null ? '' : data.name).slice(0, 60);
  const cat = GIFT_BY_NAME.get(rawName) || null; // имя ищем «как есть» (в каталоге есть имена с апострофом)
  const name = cat ? cat.name : clean(rawName, 40);
  if (!name) return { ok: false, error: 'Не выбран подарок' };
  const value = cat ? cat.v : Math.max(0, Math.min(1e6, r2(Number(data.value) || 0)));
  const ico = cat ? cat.ico : (cleanEmoji(data.ico) || '🎁');
  const pend = await pool.query(`SELECT count(*)::int AS n FROM withdrawals WHERE user_id = $1 AND status = 'pending'`, [p.id]);
  if (pend.rows[0].n >= WD_MAX_PENDING) return { ok: false, error: 'Слишком много активных заявок (максимум ' + WD_MAX_PENDING + ')' };
  const ins = await pool.query(
    'INSERT INTO withdrawals (user_id, gift_name, gift_ico, gift_value, verified) VALUES ($1,$2,$3,$4,$5) RETURNING id',
    [p.id, name, ico, value, !!cat]);
  const id = Number(ins.rows[0].id);
  console.log('[withdraw] новая ' + wdCode(id) + ' от ' + p.id + ': ' + name + ' (' + value + ' TON)');
  notifyAdminsNewWd(id).catch((e) => console.error('[withdraw notify]', e.message));
  return { ok: true, item: { id, code: wdCode(id), name, ico, v: value, status: 'pending' } };
}

/* Админ: принять / отклонить. UPDATE ... WHERE status='pending' — решение принимается ровно один раз. */
async function wdDecide(id, action, adminId) {
  const st = action === 'approve' ? 'approved' : 'rejected';
  const up = await pool.query(
    `UPDATE withdrawals SET status = $2, decided_at = now(), decided_by = $3 WHERE id = $1 AND status = 'pending' RETURNING id`,
    [id, st, String(adminId)]);
  if (!up.rowCount) {
    const ex = await pool.query('SELECT status FROM withdrawals WHERE id = $1', [id]);
    if (!ex.rowCount) return { ok: false, error: 'Заявка не найдена' };
    return { ok: false, error: 'Заявка уже обработана (' + (ex.rows[0].status === 'approved' ? 'принята' : 'отклонена') + ')' };
  }
  const r = await pool.query(WD_SELECT + ' WHERE w.id = $1', [id]);
  const w = r.rows[0];
  console.log('[withdraw] ' + wdCode(id) + ' -> ' + st + ' (админ ' + adminId + ', игрок ' + w.user_id + ', ' + w.gift_name + ')');
  io.to('u:' + w.user_id).emit('wd_update', wdUserOut(w)); // игроку: подарок разблокируется / возвращается
  io.to('admin').emit('adm_done', wdOut(w));                // остальным админам: обновить список
  tgSend(w.user_id, st === 'approved'
    ? '✅ Заявка <b>' + wdCode(id) + '</b> одобрена. Подарок «' + escHtml(w.gift_name) + '» будет отправлен вам.'
    : '❌ Заявка <b>' + wdCode(id) + '</b> отклонена. Подарок «' + escHtml(w.gift_name) + '» возвращён в ваш инвентарь.').catch(() => {});
  const stamp = st === 'approved' ? '✅ <b>ПРИНЯТО</b>' : '❌ <b>ОТКЛОНЕНО</b>';
  for (const m of (Array.isArray(w.admin_msgs) ? w.admin_msgs : [])) {
    tgApi('editMessageText', { chat_id: m.c, message_id: m.m, text: wdAdminText(w, stamp), parse_mode: 'HTML', reply_markup: { inline_keyboard: [] } }).catch(() => {});
  }
  return { ok: true, item: wdOut(w) };
}

/* Кнопки «Принять / Отклонить» под сообщением в боте (нужен вебхук: BOT_TOKEN + PUBLIC_URL + TG_WEBHOOK_SECRET) */
async function handleCallback(cq) {
  const from = String(cq.from && cq.from.id);
  if (!ADMIN_IDS.has(from)) { await tgApi('answerCallbackQuery', { callback_query_id: cq.id, text: 'Нет доступа', show_alert: true }); return; }
  const m = /^wd:([ar]):(\d{1,12})$/.exec(String(cq.data || ''));
  if (!m) { await tgApi('answerCallbackQuery', { callback_query_id: cq.id }); return; }
  const out = await wdDecide(Number(m[2]), m[1] === 'a' ? 'approve' : 'reject', from);
  await tgApi('answerCallbackQuery', { callback_query_id: cq.id, text: out.ok ? (m[1] === 'a' ? 'Заявка принята ✅' : 'Заявка отклонена ❌') : out.error, show_alert: !out.ok });
}

/* ================= ПРИГЛАСИТЬ ДРУЗЕЙ: готовое сообщение с картинкой (как в Mell Gifts) ================= */
/* Bot API 8.0: savePreparedInlineMessage -> клиент вызывает Telegram.WebApp.shareMessage(id).
   Игрок выбирает чаты, в сообщении картинка + текст + кнопка со ссылкой на Mini App (ref_<ID>). */
async function ensureBotUsername() {
  if (BOT_USERNAME || !BOT_TOKEN) return BOT_USERNAME;
  try {
    const j = await tgApi('getMe');
    if (j && j.ok && j.result.username) { BOT_USERNAME = j.result.username; console.log('[tonup] bot @' + BOT_USERNAME); }
    else console.warn('[tg] getMe:', j && j.description);
  } catch (e) { console.error('[tg] getMe:', e.message); }
  return BOT_USERNAME;
}
let shareImgBuf = null;
function shareImage() {
  if (shareImgBuf) return shareImgBuf;
  for (const f of [nodePath.join(__dirname, 'share.jpg'), nodePath.join(__dirname, 'public', 'share.jpg')]) {
    try { shareImgBuf = fs.readFileSync(f); return shareImgBuf; } catch (_) { /* нет файла — берём встроенный */ }
  }
  shareImgBuf = Buffer.from(SHARE_IMG_B64, 'base64');
  return shareImgBuf;
}
async function savePreparedShare(p, link) {
  const photo = SHARE_IMAGE_URL || (SELF_URL + '/share.jpg');
  const result = {
    type: 'photo', id: 'ref' + p.id, photo_url: photo, thumbnail_url: photo, photo_width: 1280, photo_height: 714,
    title: 'TonUp Free Box', caption: SHARE_TEXT,
    reply_markup: { inline_keyboard: [[{ text: SHARE_BUTTON, url: link }]] },
  };
  const j = await tgApi('savePreparedInlineMessage', {
    user_id: Number(p.id), result, allow_user_chats: true, allow_bot_chats: true, allow_group_chats: true, allow_channel_chats: true,
  });
  if (!j || !j.ok) throw new Error('savePreparedInlineMessage: ' + (j && j.description));
  return j.result && j.result.id;
}

/* ================= БЕСПЛАТНЫЕ КЕЙСЫ ================= */
const CASE_POOLS = (() => {
  const gifts = GIFTS.filter((g) => g[2] <= 5);
  return {
    daily: CASES.DAILY.map(([v, w]) => ({ v, w })),
    dep: CASES.DEP.map(([v, w]) => ({ v, w })).concat(gifts.map((g) => ({ v: g[2], w: CASES.GIFT_W / gifts.length, gift: g[1] }))),
  };
})();
for (const [k, c] of Object.entries(CASES.PAID)) {
  CASE_POOLS[k] = c.prizes.map(([v, w, gift]) => ({ v, w, gift: gift || null }));
  const tw = c.prizes.reduce((a, x) => a + x[1], 0), ev = c.prizes.reduce((a, x) => a + x[0] * x[1], 0) / tw;
  console.log('[case] платный кейс ' + k + ': цена ' + c.price + ' TON, RTP ' + (ev / c.price * 100).toFixed(1) + '%');
}
function casePick(list) {
  const total = list.reduce((a, x) => a + x.w, 0);
  let r = rnd() * total;
  for (const x of list) { r -= x.w; if (r < 0) return x; }
  return list[list.length - 1];
}
async function caseState(userId) {
  const o = await pool.query('SELECT case_id, max(opened_at) AS t FROM case_opens WHERE user_id = $1 GROUP BY case_id', [userId]);
  const last = { daily: 0, dep: 0 };
  o.rows.forEach((r) => { last[r.case_id] = new Date(r.t).getTime(); });
  const since = new Date(Math.max(last.dep, Date.now() - CASES.DEP_WINDOW_MS));
  const d = await pool.query(
    `SELECT COALESCE(sum(amount),0) AS s FROM deposits WHERE user_id = $1 AND status = 'paid' AND provider IN ('cryptopay','toncenter') AND paid_at > $2`,
    [userId, since]);
  const now = Date.now();
  return { dailyAgo: last.daily ? now - last.daily : null, depSum: Number(d.rows[0].s), depMin: CASES.DEP_MIN };
}
async function openPaidCase(p, id) {
  const c = CASES.PAID[id], price = r6(c.price);
  const prize = casePick(CASE_POOLS[id]);
  // списание цены и начисление приза — одна атомарная операция; баланс не может уйти в минус
  const nb = await withTx(async (cl) => {
    const u = await cl.query('UPDATE users SET balance = balance - $2 + $3, updated_at = now() WHERE id = $1 AND balance >= $2 RETURNING balance', [p.id, price, r6(prize.v)]);
    if (!u.rowCount) return null;
    await cl.query('INSERT INTO case_opens (user_id, case_id, prize, gift) VALUES ($1,$2,$3,$4)', [p.id, id, prize.v, prize.gift || null]);
    return Number(u.rows[0].balance);
  });
  if (nb === null) return { ok: false, code: 'balance', error: 'Недостаточно средств. Пополните баланс' };
  p.balance = nb;
  console.log('[case] ' + id + ' -> ' + p.id + ' цена ' + price + ' приз ' + prize.v + ' TON');
  return { ok: true, prize: { v: prize.v, gift: prize.gift || null }, balance: nb };
}
async function openCase(p, id, data) {
  if (CASES.PAID[id]) return openPaidCase(p, id);
  const st = await caseState(p.id);
  if (id === 'daily') {
    if (st.dailyAgo !== null && st.dailyAgo < CASES.DAILY_COOLDOWN_MS) return Object.assign({ ok: false, code: 'cooldown', error: 'Кейс уже открыт, подождите' }, st);
    const need = [['ch', TASKS.channel], ['chat', TASKS.chat]].filter((x) => x[1].chat); // если каналы не настроены в env — проверка пропускается
    if (need.length) {
      const status = { ch: true, chat: true };
      for (const [k, t] of need) {
        try { status[k] = await isSubscribed(t.chat, p.id); }
        catch (e) { console.error('[open_case]', e.message); return { ok: false, error: 'Не удалось проверить подписку, попробуйте позже' }; }
      }
      if (!status.ch || !status.chat) return { ok: false, code: 'tasks', error: 'Не все условия выполнены', status };
    }
  } else {
    const promo = clean(data && data.promo, 40);
    if (!promo || promo.toLowerCase() !== CASE_PROMO.toLowerCase()) return { ok: false, code: 'promo', error: 'Неверный промокод' };
    if (st.depSum + 1e-9 < CASES.DEP_MIN) return Object.assign({ ok: false, code: 'deposit', error: 'Нужен депозит ' + CASES.DEP_MIN + ' TON за последние 24 часа' }, st);
  }
  const prize = casePick(CASE_POOLS[id]);
  // приз начисляется на баланс и фиксируется в case_opens одной транзакцией (подарок — по его стоимости в TON)
  const nb = await withTx(async (c) => {
    await c.query('INSERT INTO case_opens (user_id, case_id, prize, gift) VALUES ($1,$2,$3,$4)', [p.id, id, prize.v, prize.gift || null]);
    const u = await c.query('UPDATE users SET balance = balance + $2, updated_at = now() WHERE id = $1 RETURNING balance', [p.id, r6(prize.v)]);
    return Number(u.rows[0].balance);
  });
  p.balance = nb;
  console.log('[case] ' + id + ' -> ' + p.id + ' +' + prize.v + ' TON' + (prize.gift ? ' (' + prize.gift + ')' : ''));
  return { ok: true, prize: { v: prize.v, gift: prize.gift || null }, balance: nb, dailyAgo: id === 'daily' ? 0 : st.dailyAgo, depSum: id === 'dep' ? 0 : st.depSum };
}

/* ================= ЗАДАНИЯ ================= */
async function isSubscribed(chatId, userId) {
  const j = await tgApi('getChatMember', { chat_id: chatId, user_id: Number(userId) });
  if (!j || !j.ok) { console.error('[getChatMember]', chatId, userId, j && j.description); return false; }
  const m = j.result;
  return ['creator', 'administrator', 'member'].includes(m.status) || (m.status === 'restricted' && m.is_member === true);
}
/* Награда начисляется один раз: вставка в task_claims и баланс — в одной транзакции */
async function claimTask(userId, taskId, reward) {
  return withTx(async (c) => {
    const ins = await c.query('INSERT INTO task_claims (user_id, task_id, reward) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [userId, taskId, reward]);
    if (!ins.rowCount) return null;
    const u = await c.query('UPDATE users SET balance = balance + $2, updated_at = now() WHERE id = $1 RETURNING balance', [userId, reward]);
    return Number(u.rows[0].balance);
  });
}
async function tasksList(userId) {
  const r = await pool.query('SELECT task_id FROM task_claims WHERE user_id = $1', [userId]);
  const done = new Set(r.rows.map((x) => x.task_id));
  return Object.keys(TASKS).map((id) => ({ id, title: TASKS[id].title, reward: TASKS[id].reward, url: TASKS[id].url, enabled: !!TASKS[id].chat, done: done.has(id) }));
}

/* ================= ПОДАРКИ (вебхук Telegram) ================= */
/* Подарок приходит сервисным сообщением (message.gift / message.unique_gift) в business_message
   бизнес-аккаунта, к которому подключён бот. Отправитель (message.from.id) = игрок, которому зачисляем. */
/* ===== Приветствие на /start (видео/картинка + текст + кнопка «Открыть») =====
 *   WEBAPP_URL      адрес фронтенда (GitHub Pages) — открывается кнопкой «Открыть»
 *   WELCOME_TEXT    текст приветствия (HTML; \n — перенос строки). Пусто = стандартный
 *   WELCOME_MEDIA   https-ссылка на .mp4/.gif/.jpg/.png ИЛИ file_id из Telegram.
 *                   Пусто = берём файл welcome.mp4 (или welcome.jpg/.png), лежащий рядом с server.js; нет файла = только текст
 *   WELCOME_BTN     подпись кнопки (по умолчанию «Открыть») */
const WEBAPP_URL = process.env.WEBAPP_URL || '';
const WELCOME_MEDIA = process.env.WELCOME_MEDIA || '';
const WELCOME_BTN = process.env.WELCOME_BTN || 'Открыть';
const WELCOME_TEXT = (process.env.WELCOME_TEXT ||
  'TonUp — игровая платформа с NFT-подарками и режимами прямо в Telegram\n\n' +
  'Канал с новостями и раздачей подарков: @TopUp_Pro\n' +
  'Обратная связь: @tonupsupport').replace(/\\n/g, '\n');
/* Файл приветствия рядом с server.js: загружаем в Telegram один раз, дальше шлём по file_id */
const WELCOME_FILE = ['welcome.mp4', 'welcome.jpg', 'welcome.jpeg', 'welcome.png'].map((n) => nodePath.join(__dirname, n)).find((f) => fs.existsSync(f)) || '';
let welcomeFileId = '';
async function sendWelcomeFile(chatId, extra) {
  const isImg = /\.(jpe?g|png)$/i.test(WELCOME_FILE);
  const field = isImg ? 'photo' : 'video';
  const send = async (withId) => {
    const fd = new FormData();
    fd.append('chat_id', String(chatId));
    fd.append('caption', WELCOME_TEXT);
    fd.append('parse_mode', 'HTML');
    if (!isImg) fd.append('supports_streaming', 'true');
    if (extra && extra.reply_markup) fd.append('reply_markup', JSON.stringify(extra.reply_markup));
    if (withId) fd.append(field, welcomeFileId);
    else fd.append(field, new Blob([fs.readFileSync(WELCOME_FILE)], { type: isImg ? 'image/' + (/png$/i.test(WELCOME_FILE) ? 'png' : 'jpeg') : 'video/mp4' }), nodePath.basename(WELCOME_FILE));
    const res = await fetch('https://api.telegram.org/bot' + BOT_TOKEN + '/' + (isImg ? 'sendPhoto' : 'sendVideo'), { method: 'POST', body: fd, signal: AbortSignal.timeout(60000) });
    return res.json().catch(() => null);
  };
  let j = welcomeFileId ? await send(true) : null;
  if (!j || !j.ok) j = await send(false); // нет file_id или он не подошёл — грузим файл заново
  if (j && j.ok) {
    const r = j.result || {};
    welcomeFileId = isImg ? ((r.photo || []).slice(-1)[0] || {}).file_id || '' : (r.video && r.video.file_id) || '';
  }
  return j;
}
async function sendWelcome(chatId) {
  if (!BOT_TOKEN) return;
  const kb = WEBAPP_URL
    ? { inline_keyboard: [[{ text: WELCOME_BTN, web_app: { url: WEBAPP_URL } }]] }
    : (BOT_USERNAME ? { inline_keyboard: [[{ text: WELCOME_BTN, url: 'https://t.me/' + BOT_USERNAME + '?startapp' }]] } : undefined);
  const extra = kb ? { reply_markup: kb } : {};
  try {
    if (!WELCOME_MEDIA && WELCOME_FILE) {
      const j = await sendWelcomeFile(chatId, extra);
      if (j && j.ok) return;
      console.warn('[tg] welcome file ->', chatId, j && j.description, '— шлём текстом');
    } else if (WELCOME_MEDIA) {
      const isImg = /\.(jpe?g|png|webp)(\?|$)/i.test(WELCOME_MEDIA);
      const method = isImg ? 'sendPhoto' : 'sendVideo';
      const j = await tgApi(method, Object.assign({ chat_id: chatId, [isImg ? 'photo' : 'video']: WELCOME_MEDIA, caption: WELCOME_TEXT, parse_mode: 'HTML' }, extra));
      if (j && j.ok) return;
      console.warn('[tg] welcome media ->', chatId, j && j.description, '— шлём текстом');
    }
    await tgSend(chatId, WELCOME_TEXT, extra);
  } catch (e) { console.warn('[tg] welcome ->', chatId, e.message); }
}
async function handleTelegramUpdate(u) {
  if (u.callback_query) return handleCallback(u.callback_query); // кнопки админа «Принять / Отклонить»
  const msg = u.business_message || u.message;
  if (!msg || !msg.from || msg.from.is_bot) return;
  if (u.message && msg.chat && msg.chat.type === 'private' && typeof msg.text === 'string' && /^\/start(@\w+)?(\s|$)/i.test(msg.text)) {
    return sendWelcome(msg.chat.id); // /start -> приветствие с кнопкой «Открыть»
  }
  const uniq = msg.unique_gift, reg = msg.gift;
  if (!uniq && !reg) return;
  let value = 0, label = '';
  if (uniq && uniq.gift) {
    label = uniq.gift.base_name || uniq.gift.name || '';
    const g = GIFT_BY_NAME.get(label) || [...GIFT_BY_NAME.values()].find((x) => x.name.toLowerCase() === String(label).toLowerCase());
    if (g) value = g.v; // стоимость только из серверного каталога
  } else if (reg && reg.gift && STAR_TON_RATE > 0) {
    label = 'gift#' + reg.gift.id;
    value = r6((Number(reg.gift.star_count) || 0) * STAR_TON_RATE);
  }
  if (!(value > 0)) { console.warn('[gift] не оценён, пропущен:', label, 'от', msg.from.id); return; }
  const key = 'gift:' + (msg.business_connection_id || 'bot') + ':' + msg.chat.id + ':' + msg.message_id;
  const userId = String(msg.from.id);
  let out;
  try {
  out = await withTx(async (c) => {
    // FK на users: отправитель должен был хотя бы раз открыть игру
    const ins = await c.query(
      `INSERT INTO deposits (user_id, provider, ext_id, amount, status, paid_at) VALUES ($1,'gift',$2,$3,'paid',now()) ON CONFLICT DO NOTHING`,
      [userId, key, value]);
    if (!ins.rowCount) return null; // дубль вебхука
    const r = await c.query('UPDATE users SET balance = balance + $2, updated_at = now() WHERE id = $1 RETURNING balance', [userId, value]);
    return Number(r.rows[0].balance);
  });
  } catch (e) {
    if (e.code === '23503') { console.warn('[gift] отправитель', userId, 'не зарегистрирован в игре — зачислить вручную:', label, value); return; }
    throw e; // прочее — 500, Telegram повторит доставку
  }
  if (out === null) return;
  console.log('[gift] +' + value + ' TON -> ' + userId + ' (' + label + ')');
  const p = players.get(userId);
  if (p) { p.balance = out; sendBalance(p, 'gift', { amount: value, name: label }); }
}
async function registerTelegramWebhook() {
  if (!BOT_TOKEN || !PUBLIC_URL || !TG_WEBHOOK_SECRET) return;
  const j = await tgApi('setWebhook', {
    url: PUBLIC_URL + '/tg-webhook', secret_token: TG_WEBHOOK_SECRET,
    allowed_updates: ['message', 'business_message', 'business_connection', 'callback_query'],
  });
  console.log('[tg] setWebhook:', j && (j.ok ? 'ok' : j.description));
  const wi = await tgApi('getWebhookInfo').catch(() => null); // диагностика: видно, доходят ли обновления от Telegram
  if (wi && wi.ok) console.log('[tg] webhook: ' + (wi.result.url || '— не задан —') + (wi.result.last_error_message ? ' | ОШИБКА Telegram: ' + wi.result.last_error_message : ' | ошибок нет') + ' | в очереди: ' + (wi.result.pending_update_count || 0));
}

/* ================= HTTP + SOCKET.IO ================= */
function readBody(req, max) {
  return new Promise((resolve, reject) => {
    const chunks = []; let n = 0;
    req.on('data', (c) => { n += c.length; if (n > max) { reject(new Error('too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}
const safeEq = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const json = (res, code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' }); res.end(JSON.stringify(obj)); };

const GIMG = new Map(); // кэш картинок подарков для /gimg/<slug>
const httpServer = http.createServer(async (req, res) => {
  const path = (req.url || '/').split('?')[0];
  try {
    if (req.method === 'POST' && path === '/cryptopay-webhook') {
      const raw = await readBody(req, 64 * 1024);
      if (!verifyCryptoPaySignature(raw, req.headers['crypto-pay-api-signature'])) return json(res, 401, { ok: false });
      const upd = JSON.parse(raw.toString('utf8'));
      const inv = upd && upd.payload;
      if (upd.update_type === 'invoice_paid' && inv && inv.status === 'paid' && inv.asset === 'TON') {
        await settleDeposit('cryptopay', String(inv.invoice_id), null, null); // сумму берём из НАШЕЙ записи, не из вебхука
      }
      return json(res, 200, { ok: true });
    }
    if (req.method === 'POST' && path === '/tg-webhook') {
      if (!TG_WEBHOOK_SECRET || !safeEq(req.headers['x-telegram-bot-api-secret-token'] || '', TG_WEBHOOK_SECRET)) return json(res, 401, { ok: false });
      const raw = await readBody(req, 256 * 1024);
      await handleTelegramUpdate(JSON.parse(raw.toString('utf8')));
      return json(res, 200, { ok: true });
    }
    if (req.method === 'POST' && path === '/admin/reset-history') {
      if (!ADMIN_KEY || !safeEq(req.headers['x-admin-key'] || '', ADMIN_KEY)) return json(res, 401, { ok: false });
      await histResetAll();
      return json(res, 200, { ok: true, reset: true });
    }
    if (req.method === 'GET' && path.indexOf('/gimg/') === 0) { // картинка подарка с fragment.com + CORS (нужно, чтобы клиент мог вырезать фон)
      const slug = path.slice(6).toLowerCase();
      if (!/^[a-z0-9]{1,40}$/.test(slug)) return json(res, 400, { ok: false });
      const hit = GIMG.get(slug);
      if (hit) { res.writeHead(200, { 'Content-Type': hit.type, 'Content-Length': hit.buf.length, 'Cache-Control': 'public, max-age=86400', 'Access-Control-Allow-Origin': '*' }); return res.end(hit.buf); }
      const got = await new Promise((ok) => {
        const rq = require('https').get('https://nft.fragment.com/gift/' + slug + '-1.webp', (r) => {
          if (r.statusCode !== 200) { r.resume(); return ok(null); }
          const parts = []; let n = 0;
          r.on('data', (c) => { n += c.length; if (n > 2 * 1024 * 1024) { rq.destroy(); ok(null); } else parts.push(c); });
          r.on('end', () => ok({ buf: Buffer.concat(parts), type: r.headers['content-type'] || 'image/webp' }));
          r.on('error', () => ok(null));
        });
        rq.on('error', () => ok(null));
        rq.setTimeout(8000, () => { rq.destroy(); ok(null); });
      });
      if (!got) return json(res, 404, { ok: false });
      if (GIMG.size > 300) GIMG.clear();
      GIMG.set(slug, got);
      res.writeHead(200, { 'Content-Type': got.type, 'Content-Length': got.buf.length, 'Cache-Control': 'public, max-age=86400', 'Access-Control-Allow-Origin': '*' });
      return res.end(got.buf);
    }
    if (req.method === 'GET' && path === '/share.jpg') { // баннер для сообщения «Пригласить друзей» (Telegram скачивает его по этому адресу)
      const img = shareImage();
      res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Content-Length': img.length, 'Cache-Control': 'public, max-age=3600' });
      return res.end(img);
    }
    return json(res, 200, { ok: true, service: 'tonup', players: players.size, crash: crash.phase, tvt: tvt.phase, uptime: Math.round(process.uptime()) });
  } catch (e) {
    console.error('[http]', path, e.message);
    return json(res, 500, { ok: false }); // 5xx — платёжка/Telegram повторит доставку
  }
});
const io = new Server(httpServer, {
  cors: { origin: ORIGINS, methods: ['GET', 'POST'] },
  pingInterval: 20000,
  pingTimeout: 25000,
});

/* ================= ИСТОРИЯ РАУНДОВ (общая для всех устройств, хранится в PostgreSQL) ================= */
const HIST_PREFIX = { rk: 'RKT', solo: 'PVP', tvt: 'TVT', up: 'UPG', dice: 'DCE', mines: 'MNS', plinko: 'PLK', spins: 'SPN', craft: 'CRF', orbit: 'ORB' };
const HIST_GLOBAL = new Set(['rk', 'solo', 'tvt']); // раунды ведёт сервер, запись общая; у остальных игр запись создаёт игрок
const HIST_LIMIT = 40;
const hnum = (v) => { const n = Number(v); return Number.isFinite(n) ? Math.max(-1e9, Math.min(1e9, Math.round(n * 100) / 100)) : 0; };

/* Приводим запись к безопасному виду (клиенту не доверяем: только короткие строки и числа) */
function histSanitize(r) {
  r = r && typeof r === 'object' ? r : {};
  const out = {
    chip: clean(r.chip, 16), label: clean(r.label, 32), name: clean(r.name, 32), av: cleanEmoji(r.av) || '🎲',
    bank: hnum(r.bank), bl: clean(r.bl, 16),
    stats: (Array.isArray(r.stats) ? r.stats : []).slice(0, 6).map((x) => [clean(x && x[0], 20), clean(x && x[1], 24)]),
    people: (Array.isArray(r.people) ? r.people : []).slice(0, 30).map((x) => clean(x, 24)).filter(Boolean),
  };
  if (r.mine && typeof r.mine === 'object') out.mine = { bet: hnum(r.mine.bet), res: hnum(r.mine.res) };
  return out;
}
const histCode = (game, row) => HIST_PREFIX[game] + '-' + (row.rid != null ? row.rid : 100000 + Number(row.id));
function histOut(game, row, keepMine, withDetail) {
  const rec = Object.assign({}, row.rec, { code: histCode(game, row), t: new Date(row.created_at).getTime() });
  if (!keepMine) delete rec.mine;
  if (!withDetail) { if (rec.d) rec.fair = 1; delete rec.d; } // подробности (игроки, сид, ролл) грузятся отдельно: hist_detail
  return rec;
}

/* Общая запись раунда (создаёт сервер в конце раунда) и рассылка всем клиентам */
function histSavePublic(game, rid, rec, detail) {
  const clean_ = histSanitize(rec);
  delete clean_.mine;
  if (detail) clean_.d = detail; // detail строит только сервер
  pool.query(
    'INSERT INTO round_history (game, user_id, rid, rec) VALUES ($1, NULL, $2, $3) ON CONFLICT DO NOTHING RETURNING id, rid, rec, created_at',
    [game, String(rid), JSON.stringify(clean_)]
  ).then((r) => {
    if (r.rowCount) io.to('game').emit('hist_new', { game, rec: histOut(game, r.rows[0], false) });
  }).catch((e) => console.error('[hist] save', e.message));
  return clean_;
}

/* Личная запись игрока, которую создаёт СЕРВЕР в конце раунда (не зависит от клиента: закрыл приложение — запись всё равно будет) */
async function histSaveUser(uid, game, rid, base, mine) {
  try {
    const rec = Object.assign({}, base, { mine: { bet: hnum(mine.bet), res: hnum(mine.res) } });
    if (rec.d) rec.fair = 1;
    delete rec.d;
    const r = await pool.query(
      'INSERT INTO round_history (game, user_id, rid, rec) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING RETURNING id, rid, rec, created_at',
      [game, String(uid), String(rid), JSON.stringify(rec)]
    );
    if (r.rowCount) io.to('u:' + uid).emit('hist_mine', { game, rec: histOut(game, r.rows[0], true) });
  } catch (e) { console.error('[hist] user', e.message); }
}

/* Личная запись игрока (для игр, где раунд идёт у клиента, и для «Мои ставки» в rk/solo/tvt) */
async function histSaveMine(player, game, raw) {
  const rec = histSanitize(raw);
  if (!rec.mine) return null; // без ставки игрока записывать нечего
  const global = HIST_GLOBAL.has(game);
  const rid = global ? String(Math.floor(Number(raw && raw.id)) || '') : null;
  if (global && !rid) return null;
  const r = await pool.query(
    'INSERT INTO round_history (game, user_id, rid, rec) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING RETURNING id, rid, rec, created_at',
    [game, player.id, rid, JSON.stringify(rec)]
  );
  if (!r.rowCount) return null; // дубль
  const row = r.rows[0];
  if (!global) io.to('game').emit('hist_new', { game, rec: histOut(game, row, false) }); // остальным — без суммы ставки
  return histOut(game, row, true);
}

async function histLoad(game, userId) {
  const all = HIST_GLOBAL.has(game)
    ? await pool.query('SELECT id, rid, rec, created_at FROM round_history WHERE game = $1 AND user_id IS NULL ORDER BY id DESC LIMIT $2', [game, HIST_LIMIT])
    : await pool.query('SELECT id, rid, rec, created_at FROM round_history WHERE game = $1 ORDER BY id DESC LIMIT $2', [game, HIST_LIMIT]);
  const me = await pool.query('SELECT id, rid, rec, created_at FROM round_history WHERE game = $1 AND user_id = $2 ORDER BY id DESC LIMIT $3', [game, userId, HIST_LIMIT]);
  return { all: all.rows.map((r) => histOut(game, r, false)), me: me.rows.map((r) => histOut(game, r, true)) };
}

/* Полная очистка истории (админ-эндпоинт или HISTORY_RESET_ON_START=true) */
async function histResetAll() {
  await pool.query('TRUNCATE round_history RESTART IDENTITY');
  crash.history.length = 0;
  io.to('game').emit('hist_reset', true);
  io.to('game').emit('crash_state', crashSnapshot());
}
setInterval(() => { // чистим записи старше 30 дней
  pool.query("DELETE FROM round_history WHERE created_at < now() - interval '30 days'").catch(() => {});
}, 6 * 3600 * 1000).unref();

/* ================= РАКЕТКА (CRASH) ================= */
const CRASH_BOTS = [['الياس', '🦅', '#111111'], ['gdeloka', '🪨', '#7d7a74'], ['ВіТаЛя Ґа...', '🌅', '#9a6a3a'], ['Fist', '✊', '#b9a56a'], ['Nova', '🦊', '#5a3a8a']];
const crash = {
  phase: 'wait',   // wait | fly | bust
  round: 132400,
  crashAt: 1,
  mult: 1,
  t0: 0,
  waitEnd: 0,
  rows: new Map(), // id -> строка ставки
  history: [],
  timer: null,
};
const crashPubRow = (r) => ({ id: r.id, name: r.name, av: r.av, c: r.c, bet: r.bet, cash: r.cash, bot: r.bot, gi: r.gift ? r.gift.ico : undefined });
const crashRows = () => [...crash.rows.values()].map(crashPubRow);
function crashSnapshot() {
  return {
    phase: crash.phase,
    round: crash.round,
    left: crash.phase === 'wait' ? Math.max(0, (crash.waitEnd - Date.now()) / 1000) : 0,
    mult: crash.phase === 'wait' ? 1 : crash.mult,
    rows: crashRows(),
    history: crash.history.slice(0, 12),
  };
}
const emitCrashRowsSoon = throttle(() => io.to('game').emit('crash_players', crashRows()), 120);

function genCrash() {
  if (rnd() < CRASH.INSTANT_CHANCE) return 1;
  const m = 1.01 + (CRASH.MAX_MULT - 1.01) * Math.pow(rnd(), 3.2);
  return Math.min(CRASH.MAX_MULT, r2(m));
}
const liveMult = () => Math.exp(CRASH.GROWTH * (Date.now() - crash.t0) / 1000);

function crashStartWait() {
  clearInterval(crash.timer);
  crash.phase = 'wait';
  crash.round++;
  crash.mult = 1;
  crash.rows = new Map();
  // боты, чтобы список не был пустым
  const n = 3 + Math.floor(rnd() * 3);
  CRASH_BOTS.slice().sort(() => rnd() - 0.5).slice(0, n).forEach((b, i) => {
    crash.rows.set('bot' + i, { id: 'bot' + i, name: b[0], av: b[1], c: b[2], bet: pick([0.5, 1, 1.5, 2.5, 5]), cash: null, bot: true, target: r2(1.05 + rnd() * rnd() * 6) });
  });
  crash.waitEnd = Date.now() + CRASH.WAIT_MS;
  io.to('game').emit('crash_state', crashSnapshot());
  crash.timer = setInterval(() => {
    const left = (crash.waitEnd - Date.now()) / 1000;
    if (left <= 0) return crashStartFly();
    io.to('game').emit('crash_wait', Math.round(left * 10) / 10);
  }, 100);
}
function crashStartFly() {
  clearInterval(crash.timer);
  crash.phase = 'fly';
  crash.crashAt = genCrash();
  crash.mult = 1;
  crash.t0 = Date.now();
  io.to('game').emit('crash_state', crashSnapshot());
  crash.timer = setInterval(crashTick, CRASH.TICK_MS);
}
function crashTick() {
  const m = liveMult();
  if (m >= crash.crashAt) return crashBust();
  crash.mult = r2(m);
  let changed = false;
  for (const r of crash.rows.values()) {
    if (r.bot && r.cash === null && crash.mult >= r.target) { r.cash = r.target; changed = true; }
  }
  io.to('game').emit('crash_tick', crash.mult);
  if (changed) emitCrashRowsSoon();
}
function crashBust() {
  clearInterval(crash.timer);
  crash.phase = 'bust';
  crash.mult = crash.crashAt;
  crash.history.unshift(crash.crashAt);
  if (crash.history.length > 30) crash.history.pop();
  io.to('game').emit('crash_bust', crash.crashAt);
  io.to('game').emit('crash_players', crashRows());
  { // запись раунда в общую историю
    const pr = [...crash.rows.values()].map((r) => ({ n: r.name, e: r.av, p: r.cash != null ? r.bet * r.cash : 0, bet: r.bet }));
    const top = pr.slice().sort((a, b) => b.p - a.p)[0];
    const ok = !!top && top.p > 0;
    histSavePublic('rk', crash.round, {
      chip: 'x' + crash.crashAt.toFixed(2), label: ok ? 'Лучший выигрыш' : 'Победителей нет', name: ok ? top.n : '—', av: ok ? top.e : '💥',
      bank: r2(pr.reduce((a, x) => a + x.bet, 0)), bl: 'Банк',
      stats: [['Множитель', 'x' + crash.crashAt.toFixed(2)], ['Игроки', pr.length], ['Забрали', pr.filter((x) => x.p > 0).length]],
      people: pr.map((x) => x.n),
    });
  }
  crash.timer = setTimeout(crashStartWait, CRASH.PAUSE_MS);
}

/* ===== Подарки ботов в PvP ===== */
const BOT_GIFT_MAX = Number(process.env.PVP_BOT_GIFT_MAX) || 150;      // самый дорогой подарок, который бот может поставить (TON); поднимите, чтобы боты ставили любые подарки
const BOT_GIFT_CHANCE = process.env.PVP_BOT_GIFT_CHANCE ? Number(process.env.PVP_BOT_GIFT_CHANCE) : 0.8; // шанс, что бот ставит подарками, а не TON
const giftOf = (name, v) => { const g = GIFT_BY_NAME.get(name); return { name, ico: g ? g.ico : '🎁', v: v != null ? v : (g ? g.v : 0) }; };
const botGiftCat = () => GIFTS.filter((g) => g[2] <= BOT_GIFT_MAX).map((g) => ({ name: g[1], ico: g[0], v: g[2] }));
const isOnline = (id) => { const p = players.get(id); return !!(p && p.sockets && p.sockets.size); }; // подарки выдаём только тем, кто сейчас в игре (инвентарь хранится у клиента)

/* ================= PvP TvT ================= */
const tvt = {
  rid: 45240,
  phase: 'wait',   // wait | bet | spin | res
  left: TVT.BET_SECONDS,
  ice: 0,
  fire: 0,
  rows: [],        // { id, name, av, team, amount, bot, gs:[{name,v}] }
  last: '',
  spin: null,      // { winner, rnd:[...], startedAt }
  result: null,
  timer: null,
};
const tvtPubRow = (r) => ({ id: r.id, name: r.name, av: r.av, team: r.team, amount: r.amount, bot: r.bot, gs: r.gs });
function tvtPublic() {
  const s = { rid: tvt.rid, phase: tvt.phase, left: tvt.left, ice: tvt.ice, fire: tvt.fire, rake: TVT.RAKE, last: tvt.last, hash: tvt.hash, rows: tvt.rows.map(tvtPubRow) };
  if (tvt.spin) s.spin = { winner: tvt.spin.winner, rnd: tvt.spin.rnd, elapsed: Date.now() - tvt.spin.startedAt };
  if (tvt.phase === 'res' && tvt.result) s.result = tvt.result;
  return s;
}
const emitTvt = () => io.to('game').emit('tvt_state', tvtPublic());
const emitTvtSoon = throttle(emitTvt, 80);
const tvtOpen = () => tvt.phase === 'wait' || (tvt.phase === 'bet' && tvt.left > TVT.LOCK_SECONDS);

function tvtAddBet(team, who, amount, gift) {
  amount = r2(amount);
  let row = tvt.rows.find((x) => x.id === who.id && x.team === team);
  if (row) row.amount = r2(row.amount + amount);
  else { row = { id: who.id, name: who.name, av: who.av, team, amount, bot: !!who.bot, gs: [] }; tvt.rows.push(row); }
  if (gift) row.gs.push({ name: gift.name, v: gift.v });
  tvt[team] = r2(tvt[team] + amount);
  tvt.last = who.name + ' +' + amount.toFixed(2);
  if (tvt.phase === 'wait' && tvt.ice > 0 && tvt.fire > 0) tvtStartBetting();
  emitTvtSoon();
}
function tvtStartBetting() {
  tvt.phase = 'bet';
  tvt.left = TVT.BET_SECONDS;
  clearInterval(tvt.timer);
  tvt.timer = setInterval(() => {
    tvt.left--;
    if (tvt.left <= 0) { clearInterval(tvt.timer); tvtStartSpin(); } else emitTvt();
  }, 1000);
  emitTvt();
}
function tvtStartSpin() {
  tvt.phase = 'spin';
  tvt.left = 0;
  const total = tvt.ice + tvt.fire;
  /* Доказуемая честность: roll = первые 13 hex от HMAC-SHA256(сид, «раунд:банкIce:банкFire» в nanoTON).
     Победа Ice, если roll / 2^52 * (Ice + Fire) < Ice. Сид зафиксирован хешем до первой ставки, банки известны только в конце. */
  const iceN = Math.round(tvt.ice * 1e9), fireN = Math.round(tvt.fire * 1e9);
  const roll = crypto.createHmac('sha256', tvt.seed).update(tvt.rid + ':' + iceN + ':' + fireN).digest('hex').slice(0, 13);
  const winner = BigInt('0x' + roll) * BigInt(iceN + fireN) < BigInt(iceN) * (2n ** 52n) ? 'ice' : 'fire';
  tvt.proof = { seed: tvt.seed, hash: tvt.hash, rid: tvt.rid, iceN, fireN, roll };
  // rnd[] нужен клиентам, чтобы все видели одинаковую траекторию шара (на исход не влияет)
  tvt.spin = { winner, rnd: Array.from({ length: 12 }, rnd), startedAt: Date.now() };
  emitTvt();
  setTimeout(tvtResolve, TVT.SPIN_MS);
}
function tvtResolve() {
  const { winner } = tvt.spin;
  const total = r2(tvt.ice + tvt.fire);
  const pool = tvt[winner];
  const payouts = [];
  tvt.rows.filter((r) => r.team === winner).forEach((r) => {
    const pay = r6((r.amount / pool) * total * TVT.RAKE);
    payouts.push({ id: r.id, name: r.name, av: r.av, bot: r.bot, amount: r.amount, pay, gs: r.gs });
  });
  /* Подарки ботов проигравшей команды достаются выигравшим игрокам ПОДАРКАМИ (в пределах их прибыли), остаток выплачивается TON.
     Стоимость для игрока не меняется: pay = стоимость подарков + TON. */
  const giftPool = [];
  tvt.rows.forEach((r) => { if (r.bot && r.team !== winner) (r.gs || []).forEach((g) => giftPool.push(giftOf(g.name, g.v))); });
  giftPool.sort((x, y) => y.v - x.v);
  const room = new Map();
  payouts.forEach((p) => { if (!p.bot && isOnline(p.id)) room.set(p.id, r6((room.get(p.id) || 0) + p.pay - p.amount)); });
  const won = new Map();
  for (const g of giftPool) {
    let best = null;
    for (const [id, rm] of room) if (rm >= g.v && (best === null || rm > room.get(best))) best = id;
    if (best === null) continue;
    room.set(best, r6(room.get(best) - g.v));
    if (!won.has(best)) won.set(best, []);
    won.get(best).push(g);
  }
  const credit = new Map();
  payouts.forEach((p) => {
    if (p.bot) return;
    const gv = (won.get(p.id) || []).reduce((sum, g) => sum + g.v, 0);
    credit.set(p.id, r6((credit.get(p.id) || 0) + Math.max(0, p.pay - gv)));
  });
  const gifts = {};
  for (const [id, list] of won) gifts[id] = list;
  payouts.sort((a, b) => b.amount - a.amount);
  tvt.phase = 'res';
  tvt.result = { winner, total, pool, payouts: payouts.slice(0, 30), gifts };
  if (bonusRoll()) { // бонус x40: каждый игрок победившей команды получает свою ставку ×40 (платит проект)
    let any = false;
    payouts.forEach((p) => { if (!p.bot && p.amount > 0) { p.bonus = bonusAward(p.id, 'tvt', p.amount); any = any || p.bonus > 0; } });
    if (any) tvt.result.bonus = { x: BONUS.X };
  }
  for (const [id, sum] of credit) creditUser(id, sum, 'win', { game: 'tvt', amount: sum }); // в БД, даже если игрок офлайн
  {
    const loseTeam = winner === 'ice' ? 'fire' : 'ice';
    const names = [...new Set(tvt.rows.map((r) => r.name))];
    const gl = (a) => (a || []).slice(0, 12).map((g) => ({ name: String(g.name == null ? '' : g.name).slice(0, 40), v: hnum(g.v) }));
    const pr = (x, pay) => ({ name: clean(x.name, 24), av: cleanEmoji(x.av) || '🧑', bot: !!x.bot, amount: hnum(x.amount), pay: hnum(pay), gs: gl(x.gs) });
    const pf = tvt.proof || {};
    const detail = {
      v: 1, rid: tvt.rid, winner, ice: r2(tvt.ice), fire: r2(tvt.fire), total, pool: r2(pool), rake: TVT.RAKE,
      seedHash: pf.hash, seed: pf.seed, roll: pf.roll, iceN: pf.iceN, fireN: pf.fireN,
      winners: payouts.slice(0, 30).map((p) => pr(p, p.pay)),
      losers: tvt.rows.filter((r) => r.team === loseTeam).sort((a, b) => b.amount - a.amount).slice(0, 30).map((r) => pr(r, 0)),
    };
    const base = histSavePublic('tvt', tvt.rid, {
      chip: (total > 0 ? Math.round((pool / total) * 100) : 0) + '%', label: 'Победа', name: winner === 'ice' ? 'Ice' : 'Fire', av: winner === 'ice' ? '❄' : '🔥',
      bank: total, bl: 'Банк', stats: [['Ice', tvt.ice.toFixed(2)], ['Fire', tvt.fire.toFixed(2)], ['Игроки', names.length]], people: names,
    }, detail);
    /* «Моя история»: каждому реальному игроку раунда пишем его ставку и итог */
    const mineBy = new Map();
    tvt.rows.forEach((r) => { if (r.bot) return; const m = mineBy.get(r.id) || { bet: 0, got: 0 }; m.bet = r2(m.bet + r.amount); mineBy.set(r.id, m); });
    payouts.forEach((p) => { if (p.bot) return; const m = mineBy.get(p.id); if (m) m.got = r2(m.got + p.pay); });
    for (const [uid, m] of mineBy) histSaveUser(uid, 'tvt', tvt.rid, base, { bet: m.bet, res: r2(m.got - m.bet) });
  }
  emitTvt();
  setTimeout(tvtNewRound, TVT.RESULT_MS);
}
function tvtBot(team) {
  const who = { id: 'bot_' + team, name: team === 'ice' ? 'Ice' : 'Fire', av: team === 'ice' ? '❄' : '🔥', bot: true };
  const target = 25 + rnd() * 75;
  const cat = botGiftCat().filter((g) => g.v >= 3);
  let got = 0, n = 0;
  while (cat.length && n < 3 && got < target * 0.6) {
    const opts = cat.filter((g) => got + g.v <= target);
    if (!opts.length) break;
    const g = pick(opts);
    tvtAddBet(team, who, g.v, g);
    got += g.v; n++;
  }
  if (!n) tvtAddBet(team, who, target, null);
}
function tvtNewRound() {
  clearInterval(tvt.timer);
  tvt.rid++;
  tvt.seed = crypto.randomBytes(32).toString('hex');                      // серверный сид раунда (секрет до конца раунда)
  tvt.hash = crypto.createHash('sha256').update(tvt.seed).digest('hex');  // его хеш публикуется сразу
  tvt.proof = null;
  tvt.phase = 'wait';
  tvt.left = TVT.BET_SECONDS;
  tvt.ice = 0; tvt.fire = 0; tvt.rows = []; tvt.last = ''; tvt.spin = null; tvt.result = null;
  emitTvt();
  const id = tvt.rid;
  // если команда пустует — подставляем бота (как в оригинале: Ice через 1.5 с, Fire через 3.2 с)
  if (process.env.PVP_BOTS === 'false') return; // боты в TvT включены по умолчанию (выключить: PVP_BOTS=false)
  setTimeout(() => { if (tvt.rid === id && tvt.phase === 'wait' && !tvt.ice) tvtBot('ice'); }, 1500);
  setTimeout(() => { if (tvt.rid === id && tvt.phase === 'wait' && !tvt.fire) tvtBot('fire'); }, 3200);
}

/* ================= PvP SOLO (рулетка-битва) ================= */
const BATTLE = {
  BET_SECONDS: 20,
  LOCK_SECONDS: 1,   // за столько секунд до конца ставки закрываются
  SPIN_MS: 10300,    // анимация ленты у клиентов 10 с + запас
  RESULT_MS: 6000,
  RAKE: 1,           // 1 = без комиссии
  MAX_BET: 100000,
  STRIP: 56,         // карточек в ленте рулетки
  BOTS: process.env.PVP_BOTS !== 'false',  // боты в Solo включены по умолчанию; PVP_BOTS=false — выключить
};
const BATTLE_BOT_NAMES = ['Silent Viking', 'Voskech', 'konhu', 'Rahmatullayev', 'errorhd67', 'Creator_valiev', 'Nova', 'Frost', 'Ember', 'Kuro', 'Mila', 'Zed', 'Rex', 'Luna', 'Tonotdogs', 'Misty Galaxy'];
const BATTLE_BOT_BETS = [0.1, 0.2, 0.3, 0.5, 1, 1, 2, 5, 10, 25];
const battle = {
  sid: 490912, phase: 'wait', left: BATTLE.BET_SECONDS, ci: 0, rows: [], spin: null, result: null,
  last: { n: '—', a: 0, c: 0 }, best: { n: '—', a: 0, c: 0 }, timer: null,
};
const battleTotal = () => r2(battle.rows.reduce((s, r) => s + r.amount, 0));
function battlePublic() {
  const s = {
    sid: battle.sid, phase: battle.phase, left: battle.left, rake: BATTLE.RAKE, last: battle.last, best: battle.best,
    rows: battle.rows.map((r) => ({ id: r.id, name: r.name, av: r.av, c: r.c, amount: r.amount, bot: r.bot, gs: r.gs || [] })),
  };
  if (battle.spin) s.spin = { winner: battle.spin.winner, wi: battle.spin.wi, off: battle.spin.off, strip: battle.spin.strip, elapsed: Date.now() - battle.spin.startedAt };
  if (battle.phase === 'res' && battle.result) s.result = battle.result;
  return s;
}
const emitBattle = () => io.to('game').emit('solo_state', battlePublic());
const emitBattleSoon = throttle(emitBattle, 80);
const battleOpen = () => battle.phase === 'wait' || (battle.phase === 'bet' && battle.left > BATTLE.LOCK_SECONDS);

function battleAddBet(who, amount, gs) {
  amount = r2(amount);
  let row = battle.rows.find((x) => x.id === who.id);
  if (row) row.amount = r2(row.amount + amount);
  else { row = { id: who.id, name: who.name, av: who.av, c: COLORS[battle.ci++ % COLORS.length], amount, bot: !!who.bot, gs: [] }; battle.rows.push(row); }
  if (gs && gs.length) gs.forEach((g) => row.gs.push({ name: g.name, ico: g.ico, v: g.v }));
  if (battle.phase === 'wait' && battle.rows.length >= 2) battleStartBetting();
  emitBattleSoon();
}
function battleStartBetting() {
  battle.phase = 'bet';
  battle.left = BATTLE.BET_SECONDS;
  clearInterval(battle.timer);
  battle.timer = setInterval(() => {
    battle.left--;
    if (battle.left <= 0) { clearInterval(battle.timer); battleStartSpin(); } else emitBattle();
  }, 1000);
  emitBattle();
}
function battleStartSpin() {
  battle.phase = 'spin';
  battle.left = 0;
  const total = battleTotal();
  const pickRow = () => { // шанс пропорционален ставке
    const x = rnd() * total; let acc = 0;
    for (const r of battle.rows) { acc += r.amount; if (x < acc) return r; }
    return battle.rows[battle.rows.length - 1];
  };
  const winner = pickRow();
  const wi = 44 + Math.floor(rnd() * 6);
  const strip = Array.from({ length: BATTLE.STRIP }, (_, k) => (k === wi ? winner : pickRow()).id);
  battle.spin = { winner: winner.id, wi, strip, off: r2((rnd() - 0.5) * 50), startedAt: Date.now() };
  emitBattle();
  setTimeout(battleResolve, BATTLE.SPIN_MS);
}
function battleResolve() {
  const w = battle.rows.find((r) => r.id === battle.spin.winner);
  const total = battleTotal();
  const prize = r6(total * BATTLE.RAKE);
  const pct = total ? (w.amount / total) * 100 : 0;
  battle.last = { n: w.name, a: r2(prize - w.amount), c: pct };
  if (battle.last.a > battle.best.a) battle.best = { n: w.name, a: battle.last.a, c: pct };
  /* Победитель-игрок забирает подарки ботов ПОДАРКАМИ, остаток банка — TON (стоимость та же: подарки + TON = банк) */
  const gifts = [];
  let ton = prize;
  if (!w.bot && isOnline(w.id)) {
    const all = [];
    battle.rows.forEach((r) => { if (r.bot) (r.gs || []).forEach((g) => all.push(giftOf(g.name, g.v))); });
    all.sort((x, y) => y.v - x.v);
    let sum = 0;
    for (const g of all) if (r6(sum + g.v) <= prize) { gifts.push(g); sum = r6(sum + g.v); }
    ton = r6(prize - sum);
  }
  battle.result = { winner: w.id, prize, total, pct, gifts, ton };
  battle.phase = 'res';
  if (!w.bot) creditUser(w.id, ton, 'win', { game: 'solo', amount: ton });
  if (!w.bot && bonusRoll()) { const ex = bonusAward(w.id, 'solo', w.amount); if (ex) battle.result.bonus = { x: BONUS.X, extra: ex }; }
  histSavePublic('solo', battle.sid, {
    chip: pct.toFixed(1) + '%', label: 'Победитель', name: w.name, av: w.av, bank: prize, bl: 'Банк',
    stats: [['Шанс', pct.toFixed(2) + '%'], ['Игроки', battle.rows.length], ['Ставка', w.amount.toFixed(2)]], people: battle.rows.map((r) => r.name),
  });
  emitBattle();
  setTimeout(battleNewRound, BATTLE.RESULT_MS);
}
function battleBot() {
  const used = new Set(battle.rows.map((r) => r.name));
  const free = BATTLE_BOT_NAMES.filter((n) => !used.has(n));
  if (!free.length) return;
  const name = pick(free);
  const who = { id: 'sbot_' + name, name, av: AVATARS[hashStr(name) % AVATARS.length], bot: true };
  const cat = botGiftCat();
  if (cat.length && rnd() < BOT_GIFT_CHANCE) { // бот ставит 1–3 случайных подарка из каталога
    const n = rnd() < 0.65 ? 1 : (rnd() < 0.7 ? 2 : 3);
    const gs = []; let sum = 0;
    for (let i = 0; i < n; i++) { const g = pick(cat); gs.push(g); sum += g.v; }
    battleAddBet(who, sum, gs);
  } else {
    battleAddBet(who, Math.max(0.1, pick(BATTLE_BOT_BETS) * (0.8 + rnd() * 0.6)));
  }
}
function battleNewRound() {
  clearInterval(battle.timer);
  battle.sid++; battle.phase = 'wait'; battle.left = BATTLE.BET_SECONDS;
  battle.ci = 0; battle.rows = []; battle.spin = null; battle.result = null;
  emitBattle();
  if (!BATTLE.BOTS) return;
  const id = battle.sid; // если игроков меньше двух — подставляем ботов (через 1.5 и 3.2 с)
  [1500, 3200].forEach((ms) => setTimeout(() => {
    if (battle.sid === id && battle.phase === 'wait' && battle.rows.length < 2) battleBot();
  }, ms));
}

/* ================= BALL RACE: гонка шариков. Победитель выбирается по ставкам (как в Solo), клиент только рисует гонку ================= */
const RACE = {
  BET_SECONDS: 20,
  LOCK_SECONDS: 1,
  RUN_MS: 12500,     // длительность гонки у клиентов + запас
  RESULT_MS: 6000,
  RAKE: 1,           // 1 = без комиссии
  MAX_BET: 100000,
  BOTS: process.env.RACE_BOTS !== 'false',  // RACE_BOTS=false — выключить ботов
};
const race = { sid: 1, phase: 'wait', left: RACE.BET_SECONDS, ci: 0, rows: [], run: null, result: null, last: { n: '—', a: 0, c: 0 }, timer: null };
const raceTotal = () => r2(race.rows.reduce((s, r) => s + r.amount, 0));
function racePublic() {
  const s = {
    sid: race.sid, phase: race.phase, left: race.left, rake: RACE.RAKE, last: race.last,
    rows: race.rows.map((r) => ({ id: r.id, name: r.name, av: r.av, c: r.c, amount: r.amount, bot: r.bot })),
  };
  if (race.run) s.run = { winner: race.run.winner, times: race.run.times, seed: race.run.seed, ms: RACE.RUN_MS, elapsed: Date.now() - race.run.startedAt };
  if (race.phase === 'res' && race.result) s.result = race.result;
  return s;
}
const emitRace = () => io.to('game').emit('race_state', racePublic());
const emitRaceSoon = throttle(emitRace, 80);
const raceOpen = () => race.phase === 'wait' || (race.phase === 'bet' && race.left > RACE.LOCK_SECONDS);

function raceAddBet(who, amount) {
  amount = r2(amount);
  let row = race.rows.find((x) => x.id === who.id);
  if (row) row.amount = r2(row.amount + amount);
  else { row = { id: who.id, name: who.name, av: who.av, c: COLORS[race.ci++ % COLORS.length], amount, bot: !!who.bot }; race.rows.push(row); }
  if (race.phase === 'wait' && race.rows.length >= 2) raceStartBetting();
  emitRaceSoon();
}
function raceStartBetting() {
  race.phase = 'bet';
  race.left = RACE.BET_SECONDS;
  clearInterval(race.timer);
  race.timer = setInterval(() => {
    race.left--;
    if (race.left <= 0) { clearInterval(race.timer); raceStartRun(); } else emitRace();
  }, 1000);
  emitRace();
}
function raceStartRun() {
  race.phase = 'run';
  race.left = 0;
  const total = raceTotal();
  const x = rnd() * total; let acc = 0, winner = race.rows[race.rows.length - 1];
  for (const r of race.rows) { acc += r.amount; if (x < acc) { winner = r; break; } } // шанс пропорционален ставке
  const times = {};
  race.rows.forEach((r) => { times[r.id] = r === winner ? 10500 : Math.round(10900 + rnd() * 1500); }); // победитель финиширует первым
  race.run = { winner: winner.id, times, seed: Math.floor(rnd() * 1e9), startedAt: Date.now() };
  emitRace();
  setTimeout(raceResolve, RACE.RUN_MS);
}
function raceResolve() {
  const w = race.rows.find((r) => r.id === race.run.winner);
  const total = raceTotal();
  const prize = r6(total * RACE.RAKE);
  const pct = total ? (w.amount / total) * 100 : 0;
  race.last = { n: w.name, a: r2(prize - w.amount), c: pct };
  race.result = { winner: w.id, prize, total, pct, mult: w.amount ? r2(prize / w.amount) : 0 };
  race.phase = 'res';
  if (!w.bot) creditUser(w.id, prize, 'win', { game: 'race', amount: prize });
  if (!w.bot && bonusRoll()) { const ex = bonusAward(w.id, 'race', w.amount); if (ex) race.result.bonus = { x: BONUS.X, extra: ex }; }
  emitRace();
  setTimeout(raceNewRound, RACE.RESULT_MS);
}
function raceBot() {
  const used = new Set(race.rows.map((r) => r.name));
  const free = BATTLE_BOT_NAMES.filter((n) => !used.has(n));
  if (!free.length) return;
  const name = pick(free);
  raceAddBet({ id: 'rbot_' + name, name, av: AVATARS[hashStr(name) % AVATARS.length], bot: true }, Math.max(0.1, pick(BATTLE_BOT_BETS) * (0.8 + rnd() * 0.6)));
}
function raceNewRound() {
  clearInterval(race.timer);
  race.sid++; race.phase = 'wait'; race.left = RACE.BET_SECONDS;
  race.ci = 0; race.rows = []; race.run = null; race.result = null;
  emitRace();
  if (!RACE.BOTS) return;
  raceSeedBots();
}
function raceSeedBots() { // если игроков меньше двух — подставляем ботов (в т.ч. при первом запуске сервера)
  if (!RACE.BOTS) return;
  const id = race.sid;
  [1500, 3200].forEach((ms) => setTimeout(() => {
    if (race.sid === id && race.phase === 'wait' && race.rows.length < 2) raceBot();
  }, ms));
}
setTimeout(raceSeedBots, 3000); // первый раунд после старта сервера: без этого гонка ждала игроков вечно

/* Есть ли у игрока деньги «в игре» (тогда клиентский demo_bal не принимаем) */
function hasLiveStake(id) {
  const c = crash.rows.get(id);
  if (c && !c.bot && c.cash === null && (crash.phase === 'wait' || crash.phase === 'fly')) return true;
  if (tvt.phase !== 'res' && tvt.rows.some((r) => r.id === id)) return true;
  if (battle.phase !== 'res' && battle.rows.some((r) => r.id === id)) return true;
  if (race.phase !== 'res' && race.rows.some((r) => r.id === id)) return true;
  return false;
}

/* ================= СОБЫТИЯ КЛИЕНТОВ ================= */

io.on('connection', (socket) => {
  // простой антиспам: не более 30 событий в секунду с одного сокета
  let bucket = 0, bucketAt = Date.now();
  socket.use((packet, next) => {
    const now = Date.now();
    if (now - bucketAt > 1000) { bucket = 0; bucketAt = now; }
    if (++bucket > 30) return next(new Error('rate_limit'));
    next();
  });
  socket.on('error', () => {});

  const me = () => socket.data.player || null;
  const needAuth = (cb) => { reply(cb, { ok: false, error: 'Нет авторизации' }); };

  const capsFor = (p) => ({
    deposit: isTgUser(p) && !!(CRYPTO_PAY_TOKEN || HOUSE_WALLET),
    cryptopay: !!CRYPTO_PAY_TOKEN, ton: !!HOUSE_WALLET, min: DEPOSIT.MIN,
    tasks: isTgUser(p),
    ref: isTgUser(p) && REF_PERCENT > 0, refPercent: REF_PERCENT, wd: isTgUser(p), admin: isAdmin(p),
    cases: isTgUser(p), promo: isTgUser(p) && PROMO_MAP.size > 0,
  });
  socket.on('get_caps', (cb) => { // клиент мог пропустить auth_ok — даём возможность запросить повторно
    const p = me(); if (!p) return needAuth(cb);
    reply(cb, { ok: true, caps: capsFor(p), wallet: p.tonWallet });
  });

  socket.on('auth', async (data, cb) => {
    const idn = resolveIdentity(data);
    if (!idn) { reply(cb, { ok: false, error: 'Не удалось авторизоваться' }); socket.emit('error_msg', 'Не удалось авторизоваться'); return; }
    let p;
    try { p = await loadPlayer(idn.id, idn.name, idn.av); } // ← пользователь ищется/создаётся в PostgreSQL
    catch (e) { console.error('[auth]', e.message); reply(cb, { ok: false, error: 'База данных недоступна' }); return; }
    p.name = idn.name;
    if (idn.av) p.av = idn.av;
    if (idn.username !== undefined) pool.query('UPDATE users SET tg_username = $2 WHERE id = $1', [p.id, idn.username || null]).catch(() => {});
    if (isTgUser(p) && data && typeof data.initData === 'string' && data.initData) {
      try { await bindReferrer(p.id, startParamOf(data.initData)); } catch (e) { console.error('[bindReferrer]', e.message); }
    }
    p.sockets.add(socket.id);
    p.lastSeen = Date.now();
    socket.data.player = p;
    socket.join('u:' + p.id);
    socket.join('game');
    if (isAdmin(p)) socket.join('admin');
    const caps = capsFor(p);
    reply(cb, { ok: true, id: p.id, name: p.name, balance: p.balance, wallet: p.tonWallet, caps });
    socket.emit('auth_ok', { id: p.id, name: p.name, balance: p.balance, wallet: p.tonWallet, caps });
    socket.emit('crash_state', crashSnapshot());
    socket.emit('tvt_state', tvtPublic());
    socket.emit('solo_state', battlePublic());
    socket.emit('race_state', racePublic());
  });

  socket.on('identity', (data) => { // смена анонимного режима
    const p = me();
    if (!p || !data) return;
    p.name = clean(data.name, 24) || p.name;
    p.av = cleanEmoji(data.av) || AVATARS[hashStr(p.id) % AVATARS.length];
  });

  /* ---- Кошелёк (TON Connect): сохраняем адрес в users.ton_wallet. Только для отображения — деньги по нему НЕ начисляются ---- */
  socket.on('set_wallet', async (address, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    const a = address === null || address === '' ? null : String(address);
    if (a !== null && !/^[A-Za-z0-9_\-:]{40,80}$/.test(a)) return reply(cb, { ok: false, error: 'Неверный адрес' });
    try {
      await pool.query('UPDATE users SET ton_wallet = $2, updated_at = now() WHERE id = $1', [p.id, a]);
      p.tonWallet = a;
      reply(cb, { ok: true });
    } catch (e) { console.error('[set_wallet]', e.message); reply(cb, { ok: false, error: 'Ошибка сервера' }); }
  });

  /* ---- Пополнение ---- */
  // { amount, method: 'cryptopay' | 'ton' }  ->  { ok, url } | { ok, ton:{address,nano,payload,memo,link} }
  socket.on('create_deposit', async (data, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    if (!isTgUser(p)) return reply(cb, { ok: false, error: 'Пополнение доступно только в Telegram' });
    data = data && typeof data === 'object' ? data : {};
    const amount = parseAmount(data.amount, DEPOSIT.MAX);
    if (amount < DEPOSIT.MIN) return reply(cb, { ok: false, error: 'Минимум ' + DEPOSIT.MIN + ' TON' });
    try {
      const pend = await pool.query(`SELECT count(*)::int AS n FROM deposits WHERE user_id=$1 AND status='pending' AND created_at > now() - interval '1 hour'`, [p.id]);
      if (pend.rows[0].n >= DEPOSIT.MAX_PENDING) return reply(cb, { ok: false, error: 'Слишком много неоплаченных счетов, подождите' });
      if (data.method === 'ton') return reply(cb, { ok: true, ton: await createTonDeposit(p, amount) });
      return reply(cb, { ok: true, url: await createCryptoInvoice(p, amount) });
    } catch (e) {
      console.error('[create_deposit]', e.message);
      reply(cb, { ok: false, error: /не настроен/.test(e.message) ? e.message : 'Не удалось создать счёт' });
    }
  });
  // Кнопка «Я оплатил»: не ждём фоновый опрос, проверяем сразу (не чаще раза в 3 с)
  socket.on('check_deposit', async (cb) => {
    const p = me(); if (!p) return needAuth(cb);
    if (Date.now() - (p.lastDepCheck || 0) < 3000) return reply(cb, { ok: true });
    p.lastDepCheck = Date.now();
    await pollPayments();
    reply(cb, { ok: true });
  });

  /* ---- Рефералы ---- */
  socket.on('get_ref', async (cb) => {
    const p = me(); if (!p) return needAuth(cb);
    if (!isTgUser(p) || !REF_PERCENT) return reply(cb, { ok: false, error: 'Рефералы доступны только в Telegram' });
    try {
      await ensureBotUsername(); // если getMe при старте не удался — пробуем снова, иначе ссылка была бы пустой
      const [a, b, l] = await Promise.all([
        pool.query('SELECT count(*)::int AS n FROM users WHERE referrer_id = $1', [p.id]),
        pool.query('SELECT COALESCE(sum(amount),0) AS s FROM referral_earnings WHERE referrer_id = $1', [p.id]),
        pool.query(`SELECT u.id, u.name, COALESCE(sum(e.amount), 0) AS earned
                    FROM users u LEFT JOIN referral_earnings e ON e.user_id = u.id AND e.referrer_id = $1
                    WHERE u.referrer_id = $1 GROUP BY u.id, u.name, u.created_at
                    ORDER BY earned DESC, u.created_at DESC LIMIT 20`, [p.id]),
      ]);
      reply(cb, { ok: true, percent: REF_PERCENT, id: p.id, link: refLink(p.id), count: a.rows[0].n, earned: Number(b.rows[0].s),
        list: l.rows.map((x) => ({ name: x.name, earned: Number(x.earned) })) });
    } catch (e) { console.error('[get_ref]', e.message); reply(cb, { ok: false, error: 'Ошибка сервера' }); }
  });

  /* ---- Пригласить друзей: готовое сообщение (картинка + текст + кнопка) ---- */
  // -> { ok, link, msgId? }.  msgId нет — клиент делает обычный шэринг ссылкой (t.me/share/url).
  socket.on('ref_share', async (cb) => {
    const p = me(); if (!p) return needAuth(cb);
    if (!isTgUser(p) || !REF_PERCENT) return reply(cb, { ok: false, error: 'Рефералы доступны только в Telegram' });
    if (Date.now() - (p.lastShare || 0) < 1500) return reply(cb, { ok: false, error: 'Подождите секунду' });
    p.lastShare = Date.now();
    try {
      await ensureBotUsername();
      const link = refLink(p.id);
      if (!link) return reply(cb, { ok: false, error: 'На сервере не задан BOT_USERNAME (или неверный BOT_TOKEN)', code: 'no_bot' });
      let msgId = null, err;
      try { msgId = await savePreparedShare(p, link); } catch (e) { err = e.message; console.error('[ref_share]', e.message); }
      reply(cb, { ok: true, link, msgId: msgId || null, text: SHARE_TEXT, error: err });
    } catch (e) { console.error('[ref_share]', e.message); reply(cb, { ok: false, error: 'Ошибка сервера' }); }
  });

  /* ---- Вывод подарков (игрок) ---- */
  // { name, ico?, value? } -> { ok, item:{ id, code, name, ico, v, status } } | { ok:false, error }
  socket.on('wd_create', (data, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    if (!isTgUser(p)) return reply(cb, { ok: false, error: 'Вывод доступен только в Telegram' });
    queue(p, async () => { reply(cb, await wdCreate(p, data)); }, cb);
  });
  // активные заявки + решённые, о которых клиент ещё не подтвердил получение
  socket.on('wd_list', async (cb) => {
    const p = me(); if (!p) return needAuth(cb);
    try {
      const r = await pool.query(`SELECT * FROM withdrawals WHERE user_id = $1 AND (status = 'pending' OR acked = FALSE) ORDER BY id DESC LIMIT 50`, [p.id]);
      reply(cb, { ok: true, list: r.rows.map(wdUserOut) });
    } catch (e) { console.error('[wd_list]', e.message); reply(cb, { ok: false, error: 'Ошибка сервера' }); }
  });
  socket.on('wd_ack', async (ids) => {
    const p = me(); if (!p) return;
    const list = (Array.isArray(ids) ? ids : []).map(Number).filter((n) => Number.isInteger(n) && n > 0).slice(0, 50);
    if (!list.length) return;
    try { await pool.query(`UPDATE withdrawals SET acked = TRUE WHERE user_id = $1 AND status <> 'pending' AND id = ANY($2::bigint[])`, [p.id, list]); }
    catch (e) { console.error('[wd_ack]', e.message); }
  });

  /* ---- Админ-панель (только Telegram ID из ADMIN_TG_IDS, проверка на сервере) ---- */
  socket.on('adm_state', async (cb) => {
    const p = me(); if (!isAdmin(p)) return reply(cb, { ok: false, error: 'Нет доступа' });
    try {
      const pend = await pool.query(WD_SELECT + ` WHERE w.status = 'pending' ORDER BY w.id ASC LIMIT 100`);
      const rec = await pool.query(WD_SELECT + ` WHERE w.status <> 'pending' ORDER BY w.decided_at DESC NULLS LAST LIMIT 20`);
      reply(cb, { ok: true, pending: pend.rows.map(wdOut), recent: rec.rows.map(wdOut) });
    } catch (e) { console.error('[adm_state]', e.message); reply(cb, { ok: false, error: 'Ошибка сервера' }); }
  });
  // { id, action: 'approve' | 'reject' }
  socket.on('adm_decide', async (data, cb) => {
    const p = me(); if (!isAdmin(p)) return reply(cb, { ok: false, error: 'Нет доступа' });
    const id = Number(data && data.id), action = data && data.action;
    if (!Number.isInteger(id) || id <= 0 || (action !== 'approve' && action !== 'reject')) return reply(cb, { ok: false, error: 'Неверный запрос' });
    try { reply(cb, await wdDecide(id, action, p.id)); }
    catch (e) { console.error('[adm_decide]', e.message); reply(cb, { ok: false, error: 'Ошибка сервера' }); }
  });

  /* ---- Промокоды ---- */
  socket.on('redeem_promo', (data, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    if (!isTgUser(p)) return reply(cb, { ok: false, error: 'Промокоды доступны только в Telegram' });
    const code = clean(data && data.code, 40).toUpperCase();
    const promo = PROMO_MAP.get(code);
    if (!promo) return reply(cb, { ok: false, error: 'Неверный промокод' });
    const amount = promo.amount;
    queue(p, async () => {
      const nb = await withTx(async (c) => {
        if (promo.max) { // общий лимит активаций: блокируем код на время транзакции, чтобы два игрока не успели одновременно
          await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', ['promo:' + code]);
          const used = await c.query('SELECT count(*)::int AS n FROM promo_redeems WHERE code = $1', [code]);
          if (used.rows[0].n >= promo.max) return 'limit';
        }
        const ins = await c.query('INSERT INTO promo_redeems (user_id, code, amount) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [p.id, code, amount]);
        if (!ins.rowCount) return null;
        const u = await c.query('UPDATE users SET balance = balance + $2, updated_at = now() WHERE id = $1 RETURNING balance', [p.id, amount]);
        return Number(u.rows[0].balance);
      });
      if (nb === 'limit') return reply(cb, { ok: false, error: 'Промокод закончился' });
      if (nb === null) return reply(cb, { ok: false, error: 'Промокод уже использован' });
      p.balance = nb;
      console.log('[promo] ' + code + ' +' + amount + ' TON -> ' + p.id);
      sendBalance(p, 'promo', { amount });
      reply(cb, { ok: true, amount, balance: nb });
    }, cb);
  });

  /* ---- Бесплатные кейсы ---- */
  socket.on('case_state', async (cb) => {
    const p = me(); if (!p) return needAuth(cb);
    if (!isTgUser(p)) return reply(cb, { ok: false, error: 'Кейсы доступны только в Telegram' });
    try { reply(cb, Object.assign({ ok: true }, await caseState(p.id))); }
    catch (e) { console.error('[case_state]', e.message); reply(cb, { ok: false, error: 'Ошибка сервера' }); }
  });
  // { case: 'daily' | 'dep', promo? }  ->  { ok, prize:{v,gift}, balance, ... } | { ok:false, code, error }
  socket.on('open_case', (data, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    if (!isTgUser(p)) return reply(cb, { ok: false, error: 'Кейсы доступны только в Telegram' });
    const id = data && (data.case === 'daily' || data.case === 'dep' || (typeof data.case === 'string' && Object.prototype.hasOwnProperty.call(CASES.PAID, data.case))) ? data.case : null;
    if (!id) return reply(cb, { ok: false, error: 'Неизвестный кейс' });
    queue(p, async () => { reply(cb, await openCase(p, id, data)); }, cb);
  });

  /* ---- Задания ---- */
  socket.on('get_tasks', async (cb) => {
    const p = me(); if (!p) return needAuth(cb);
    try { reply(cb, { ok: true, tasks: await tasksList(p.id) }); }
    catch (e) { console.error('[get_tasks]', e.message); reply(cb, { ok: false, error: 'Ошибка сервера' }); }
  });
  // { taskId: 'channel' | 'chat' }
  socket.on('check_task', (data, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    const fail = (error) => { reply(cb, { ok: false, error }); socket.emit('error_msg', error); };
    const taskId = data && typeof data.taskId === 'string' ? data.taskId : '';
    const t = Object.prototype.hasOwnProperty.call(TASKS, taskId) ? TASKS[taskId] : null;
    if (!t || !t.chat) return fail('Задание недоступно');
    if (!isTgUser(p)) return fail('Задания доступны только в Telegram');
    queue(p, async () => {
      let ok;
      try { ok = await isSubscribed(t.chat, p.id); }
      catch (e) { console.error('[check_task]', e.message); return fail('Не удалось проверить подписку, попробуйте позже'); }
      if (!ok) return fail('Вы ещё не подписаны');
      const nb = await claimTask(p.id, taskId, t.reward); // null — награда уже была получена
      if (nb === null) return fail('Награда уже получена');
      p.balance = nb;
      sendBalance(p, 'task', { taskId, amount: t.reward });
      socket.emit('task_done', { taskId, reward: t.reward });
      reply(cb, { ok: true, taskId, reward: t.reward, balance: nb });
    }, cb);
  });

  /* ---- Ракетка ---- */
  /* Ставка подарком (общая для Ракетки и PvP): баланс не списывается — только при ALLOW_GIFT_BETS=true */
  const giftFrom = (arg) => {
    if (!arg || typeof arg !== 'object' || !arg.gift) return null;
    if (!ALLOW_GIFT_BETS) return { error: 'Ставки подарками отключены' };
    const g = GIFT_BY_NAME.get(String(arg.gift.name));
    return g ? { g } : { error: 'Подарок не найден' };
  };
  socket.on('bet_crash', (amount, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    const gf = giftFrom(amount);
    queue(p, async () => {
      if (crash.phase !== 'wait') return reply(cb, { ok: false, error: 'Ставки сейчас закрыты' });
      if (crash.rows.has(p.id)) return reply(cb, { ok: false, error: 'Ставка уже сделана' });
      if (gf) {
        if (gf.error) return reply(cb, { ok: false, error: gf.error });
        const g = gf.g;
        crash.rows.set(p.id, { id: p.id, name: p.name, av: p.av, c: COLORS[hashStr(p.id) % COLORS.length], bet: r2(g.v), cash: null, bot: false, gift: { name: g.name, ico: g.ico, v: g.v } });
        emitCrashRowsSoon();
        return reply(cb, { ok: true, bet: r2(g.v), gift: true });
      }
      const v = parseAmount(amount, CRASH.MAX_BET);
      if (!v) return reply(cb, { ok: false, error: 'Введите сумму' });
      const round = crash.round;
      const nb = await adjust(p.id, -v); // атомарное списание в БД
      if (nb === null) return reply(cb, { ok: false, error: 'Недостаточно средств' });
      if (crash.phase !== 'wait' || crash.round !== round) { // пока ждали БД — раунд ушёл
        p.balance = await adjust(p.id, v);
        sendBalance(p, 'refund', { game: 'crash' });
        return reply(cb, { ok: false, error: 'Ставки сейчас закрыты' });
      }
      p.balance = nb;
      crash.rows.set(p.id, { id: p.id, name: p.name, av: p.av, c: COLORS[hashStr(p.id) % COLORS.length], bet: v, cash: null, bot: false });
      sendBalance(p, 'bet', { game: 'crash', amount: v });
      emitCrashRowsSoon();
      reply(cb, { ok: true, bet: v });
    }, cb);
  });
  socket.on('cancel_crash', (cb) => {
    const p = me(); if (!p) return needAuth(cb);
    queue(p, async () => {
      const r = crash.rows.get(p.id);
      if (crash.phase !== 'wait' || !r || r.bot) return reply(cb, { ok: false, error: 'Нельзя отменить ставку' });
      crash.rows.delete(p.id); // сначала убираем ставку, потом возвращаем деньги — двойной возврат невозможен
      emitCrashRowsSoon();
      if (r.gift) return reply(cb, { ok: true, gift: { name: r.gift.name, v: r.gift.v } }); // подарок возвращается игроку
      await creditUser(p.id, r.bet, 'refund', { game: 'crash' });
      reply(cb, { ok: true });
    }, cb);
  });
  socket.on('cashout_crash', (cb) => {
    const p = me(); if (!p) return needAuth(cb);
    // множитель фиксируется в момент получения запроса, а не после очереди/БД
    const m = liveMult();
    queue(p, async () => {
      const r = crash.rows.get(p.id);
      if (crash.phase !== 'fly' || !r || r.bot || r.cash !== null) return reply(cb, { ok: false, error: 'Нечего забирать' });
      if (m >= crash.crashAt) return reply(cb, { ok: false, error: 'Ракета уже улетела' });
      const mult = Math.max(1, r2(m));
      const win = r6(r.bet * mult);
      r.cash = mult; // помечаем сразу — второй cashout невозможен
      emitCrashRowsSoon();
      await creditUser(p.id, win, 'win', { game: 'crash', amount: win });
      reply(cb, { ok: true, mult, win });
    }, cb);
  });

  /* ---- TvT ---- */
  socket.on('bet_tvt', (data, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    if (!data || (data.team !== 'ice' && data.team !== 'fire')) return reply(cb, { ok: false, error: 'Выберите команду' });
    queue(p, async () => {
      if (!tvtOpen()) return reply(cb, { ok: false, error: 'Ставки сейчас закрыты' });
      const who = { id: p.id, name: p.name, av: p.av, bot: false };
      if (data.gift) { // ставка подарком: баланс не списывается — по умолчанию ВЫКЛЮЧЕНО (ALLOW_GIFT_BETS)
        if (!ALLOW_GIFT_BETS) return reply(cb, { ok: false, error: 'Ставки подарками отключены' });
        const g = GIFT_BY_NAME.get(String(data.gift.name));
        if (!g) return reply(cb, { ok: false, error: 'Подарок не найден' });
        tvtAddBet(data.team, who, g.v, g);
        return reply(cb, { ok: true, bet: g.v, gift: true });
      }
      const v = parseAmount(data.amount, TVT.MAX_BET);
      if (!v) return reply(cb, { ok: false, error: 'Введите сумму' });
      const rid = tvt.rid;
      const nb = await adjust(p.id, -v);
      if (nb === null) return reply(cb, { ok: false, error: 'Недостаточно средств' });
      if (tvt.rid !== rid || !tvtOpen()) {
        p.balance = await adjust(p.id, v);
        sendBalance(p, 'refund', { game: 'tvt' });
        return reply(cb, { ok: false, error: 'Ставки сейчас закрыты' });
      }
      p.balance = nb;
      sendBalance(p, 'bet', { game: 'tvt', amount: v });
      tvtAddBet(data.team, who, v, null);
      reply(cb, { ok: true, bet: v });
    }, cb);
  });

  socket.on('bet_solo', (amount, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    const gf = giftFrom(amount);
    queue(p, async () => {
      if (!battleOpen()) return reply(cb, { ok: false, error: 'Ставки сейчас закрыты' });
      if (gf) {
        if (gf.error) return reply(cb, { ok: false, error: gf.error });
        const g = gf.g;
        battleAddBet({ id: p.id, name: p.name, av: p.av, bot: false }, g.v, [{ name: g.name, ico: g.ico, v: g.v }]);
        return reply(cb, { ok: true, bet: r2(g.v), gift: true });
      }
      const v = parseAmount(amount, BATTLE.MAX_BET);
      if (!v) return reply(cb, { ok: false, error: 'Введите сумму' });
      const sid = battle.sid;
      const nb = await adjust(p.id, -v);
      if (nb === null) return reply(cb, { ok: false, error: 'Недостаточно средств' });
      if (battle.sid !== sid || !battleOpen()) {
        p.balance = await adjust(p.id, v);
        sendBalance(p, 'refund', { game: 'solo' });
        return reply(cb, { ok: false, error: 'Ставки сейчас закрыты' });
      }
      p.balance = nb;
      sendBalance(p, 'bet', { game: 'solo', amount: v });
      battleAddBet({ id: p.id, name: p.name, av: p.av, bot: false }, v);
      reply(cb, { ok: true, bet: v });
    }, cb);
  });

  /* ---- Ball Race: ставка ---- */
  socket.on('bet_race', (amount, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    queue(p, async () => {
      if (!raceOpen()) return reply(cb, { ok: false, error: 'Ставки сейчас закрыты' });
      const v = parseAmount(amount, RACE.MAX_BET);
      if (!v) return reply(cb, { ok: false, error: 'Введите сумму' });
      const sid = race.sid;
      const nb = await adjust(p.id, -v);
      if (nb === null) return reply(cb, { ok: false, error: 'Недостаточно средств' });
      if (race.sid !== sid || !raceOpen()) {
        p.balance = await adjust(p.id, v);
        sendBalance(p, 'refund', { game: 'race' });
        return reply(cb, { ok: false, error: 'Ставки сейчас закрыты' });
      }
      p.balance = nb;
      sendBalance(p, 'bet', { game: 'race', amount: v });
      raceAddBet({ id: p.id, name: p.name, av: p.av, bot: false }, v);
      reply(cb, { ok: true, bet: v });
    }, cb);
  });

  /* ---- Баланс локальных демо-игр. С реальными деньгами по умолчанию ОТКЛЮЧЕНО: клиенту нельзя доверять баланс ---- */
  /* ---- История раундов ---- */
  let histBucket = 0, histAt = Date.now();
  socket.on('hist_get', async (data, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    const game = data && String(data.game);
    if (!HIST_PREFIX[game]) return reply(cb, { ok: false });
    try { reply(cb, Object.assign({ ok: true }, await histLoad(game, p.id))); }
    catch (e) { console.error('[hist] load', e.message); reply(cb, { ok: false }); }
  });
  socket.on('hist_add', async (data, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    const now = Date.now();
    if (now - histAt > 10000) { histBucket = 0; histAt = now; }
    if (++histBucket > 20) return reply(cb, { ok: false });
    const game = data && String(data.game);
    if (!HIST_PREFIX[game]) return reply(cb, { ok: false });
    try { reply(cb, { ok: true, rec: await histSaveMine(p, game, data.rec) }); }
    catch (e) { console.error('[hist] add', e.message); reply(cb, { ok: false }); }
  });

  socket.on('hist_detail', async (data, cb) => {
    const p = me(); if (!p) return needAuth(cb);
    const game = data && String(data.game);
    const rid = String((data && data.rid) || '').replace(/[^0-9]/g, '').slice(0, 15);
    if (!HIST_PREFIX[game] || !rid) return reply(cb, { ok: false, error: 'Раунд не найден' });
    try {
      const r = await pool.query('SELECT id, rid, rec, created_at FROM round_history WHERE game = $1 AND user_id IS NULL AND rid = $2 LIMIT 1', [game, rid]);
      if (!r.rowCount) return reply(cb, { ok: false, error: 'Раунд не найден' });
      const rec = histOut(game, r.rows[0], false, true);
      const d = rec.d || null;
      delete rec.d;
      reply(cb, { ok: true, d, rec });
    } catch (e) { console.error('[hist] detail', e.message); reply(cb, { ok: false, error: 'Ошибка загрузки' }); }
  });

  socket.on('demo_bal', (value) => {
    const p = me(); if (!p) return;
    queue(p, async () => {
      const v = Number(value);
      if (!ALLOW_DEMO_SYNC || !Number.isFinite(v) || v < 0 || v > 1e9 || hasLiveStake(p.id)) {
        sendBalance(p, 'sync'); // откат клиента к серверному значению из БД
        if (hasLiveStake(p.id)) socket.emit('error_msg', 'Дождитесь окончания ставки в Ракетке/TvT');
        return;
      }
      await pool.query('UPDATE users SET balance = $2, updated_at = now() WHERE id = $1', [p.id, r6(v)]);
      p.balance = r6(v);
      sendBalance(p, 'sync');
    }, null);
  });

  /* ===== ТАБЛИЦА ЛИДЕРОВ ===== */
  socket.on('get_lb', async (cb) => {
    try {
      const p = me();
      const key = lbKey();

      // бонус за ежедневный вход (+5), один раз в сутки, считает сервер
      if (p) {
        const d = await pool.query('INSERT INTO lb_login_days (user_id, day) VALUES ($1, CURRENT_DATE) ON CONFLICT DO NOTHING RETURNING 1', [p.id]);
        if (d.rowCount) await lbAdd(p.id, 5);
      }

      // топ-50 (кэш 5 сек, чтобы не нагружать БД)
      if (lbCache.key !== key || Date.now() - lbCache.at > 5000) {
        const t = await pool.query(
          `SELECT s.user_id, u.name, s.score FROM lb_scores s JOIN users u ON u.id = s.user_id
           WHERE s.period = $1 AND s.score > 0 ORDER BY s.score DESC, s.user_id LIMIT 50`, [key]);
        lbCache = { key, at: Date.now(), rows: t.rows.map((r) => ({ id: r.user_id, name: r.name, score: Number(r.score) })) };
      }

      // клиент хранит строки по имени → одинаковые имена склеились бы; делаем их уникальными, чтобы каждый игрок был в списке
      const cnt = {};
      lbCache.rows.forEach((r) => { cnt[r.name] = (cnt[r.name] || 0) + 1; });
      const rows = lbCache.rows.map((r) => ({
        name: cnt[r.name] > 1 ? r.name + ' #' + (hashStr(r.id) % 1000) : r.name,
        score: r.score,
        me: !!p && r.id === p.id,
      }));

      // я вне топ-50 → отдельной строкой + мой реальный ранг
      let rank = null;
      if (p) {
        const i = lbCache.rows.findIndex((r) => r.id === p.id);
        if (i >= 0) rank = i + 1;
        else {
          const m = await pool.query('SELECT score FROM lb_scores WHERE period = $1 AND user_id = $2', [key, p.id]);
          const sc = m.rowCount ? Number(m.rows[0].score) : 0;
          if (sc > 0) {
            const r = await pool.query('SELECT count(*) + 1 AS r FROM lb_scores WHERE period = $1 AND score > $2', [key, sc]);
            rank = Number(r.rows[0].r);
            rows.push({ name: p.name, score: sc, me: true });
          }
        }
      }
      reply(cb, { ok: true, rows, rank, start: key * LB_PERIOD });
    } catch (e) { console.error('[get_lb]', e.message); reply(cb, { ok: false, error: 'Ошибка сервера' }); }
  });

  /* Клиентский 'lb_points' сервер намеренно игнорирует (нет обработчика): очки начисляются только за реальные действия. */

  socket.on('disconnect', () => {
    const p = me();
    if (p) { p.sockets.delete(socket.id); p.lastSeen = Date.now(); }
  });
});

/* ================= СТАРТ ================= */
(async () => {
  try { await initDb(); console.log('[tonup] PostgreSQL OK, таблицы готовы'); if (HISTORY_RESET_ON_START) { await pool.query('TRUNCATE round_history RESTART IDENTITY'); console.log('[tonup] история раундов очищена'); } }
  catch (e) { console.error('[tonup] не удалось подключиться к PostgreSQL:', e.message); process.exit(1); }
  /* Номера раундов продолжаем с последнего сохранённого в БД. Раньше после каждого рестарта сервера они начинались заново,
     совпадали со старыми записями, и ON CONFLICT DO NOTHING молча не сохранял новые раунды: история «замирала». */
  try {
    const mx = await pool.query("SELECT game, MAX(CASE WHEN rid ~ '^[0-9]{1,15}$' THEN rid::bigint END) AS m FROM round_history WHERE user_id IS NULL AND game IN ('rk','tvt','solo') GROUP BY game");
    for (const r of mx.rows) {
      const m = Number(r.m) || 0;
      if (r.game === 'tvt') tvt.rid = Math.max(tvt.rid, m);
      else if (r.game === 'rk') crash.round = Math.max(crash.round, m);
      else if (r.game === 'solo') battle.sid = Math.max(battle.sid, m);
    }
    console.log('[hist] номера раундов: tvt ' + tvt.rid + ', rk ' + crash.round + ', solo ' + battle.sid);
  } catch (e) { console.error('[hist] sync rid', e.message); }
  crashStartWait();
  tvtNewRound();
  battleNewRound();
  raceNewRound();
  setInterval(pollPayments, 20000).unref();   // страховка на случай, если вебхук платёжки не дошёл
  registerTelegramWebhook().catch((e) => console.error('[tg] setWebhook:', e.message));
  ensureBotUsername().then(() => {
    if (!BOT_USERNAME) console.warn('[ref] BOT_USERNAME неизвестен (задайте env BOT_USERNAME или проверьте BOT_TOKEN) — реферальная ссылка будет пустой');
  });
  if (!WEBAPP_SHORT_NAME) console.warn('[ref] WEBAPP_SHORT_NAME не задан: ссылка t.me/БОТ?startapp=… откроет Mini App, только если он назначен Main Mini App в BotFather (иначе задайте short name)');
  console.log('[tonup] админы заявок на вывод: ' + [...ADMIN_IDS].join(', '));
  console.log('[tonup] промокоды загружены: ' + PROMO_MAP.size + (PROMO_MAP.size ? ' (' + [...PROMO_MAP].map(([k, v]) => k.slice(0, 4) + '…' + k.slice(-2) + ' ' + v.amount + 'TON' + (v.max ? ' x' + v.max : '')).join(', ') + ')' : ''));
  console.log('[tg] приветствие на /start: ' + (WELCOME_MEDIA ? 'медиа из WELCOME_MEDIA' : WELCOME_FILE ? 'файл ' + nodePath.basename(WELCOME_FILE) : 'только текст') + (WEBAPP_URL ? ', кнопка открывает ' + WEBAPP_URL : ', кнопка ведёт на t.me/БОТ?startapp'));
  if (!(BOT_TOKEN && PUBLIC_URL && TG_WEBHOOK_SECRET)) console.warn('[tg] ⚠ бот НЕ ответит на /start: нужны env BOT_TOKEN + PUBLIC_URL + TG_WEBHOOK_SECRET (без вебхука Telegram не присылает сообщения). Также кнопки «Принять/Отклонить» в чате бота не работают — заявки доступны в админ-панели приложения');
  httpServer.listen(PORT, () => console.log('[tonup] listening on :' + PORT + (BOT_TOKEN ? ' (Telegram initData check ON)' : ' (initData check OFF: пополнение и задания недоступны)')));
})();
process.on('SIGTERM', () => { io.close(); httpServer.close(() => pool.end().finally(() => process.exit(0))); });

/* Встроенный баннер для «Пригласить друзей» (JPEG 1280x714). Чтобы заменить — положите рядом с server.js файл share.jpg или задайте SHARE_IMAGE_URL. */
const SHARE_IMG_B64 = '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';
