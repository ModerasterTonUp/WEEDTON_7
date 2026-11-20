-- Выполнять не обязательно: server.js создаёт эти таблицы сам при старте (IF NOT EXISTS).
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
