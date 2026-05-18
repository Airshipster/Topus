CREATE TABLE IF NOT EXISTS projects (
  code TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  bot_token TEXT NOT NULL,
  webhook_secret TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
  project_code TEXT NOT NULL,
  category_id TEXT NOT NULL,
  parent_id TEXT,
  title TEXT NOT NULL,
  sort_order INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (project_code, category_id),
  FOREIGN KEY (project_code) REFERENCES projects(code) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS channels (
  project_code TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  title TEXT NOT NULL,
  category_id TEXT NOT NULL DEFAULT 'root',
  status TEXT NOT NULL DEFAULT 'green',
  sort_order INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (project_code, channel_id),
  FOREIGN KEY (project_code) REFERENCES projects(code) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_channels_project_category
ON channels(project_code, category_id, sort_order, title);

CREATE TABLE IF NOT EXISTS users (
  project_code TEXT NOT NULL,
  user_id TEXT NOT NULL,
  username TEXT,
  first_name TEXT,
  is_paid INTEGER NOT NULL DEFAULT 0,
  is_allowlisted INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (project_code, user_id)
);

CREATE TABLE IF NOT EXISTS user_subscriptions (
  project_code TEXT NOT NULL,
  user_id TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (project_code, user_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_user_subscriptions_channel
ON user_subscriptions(project_code, channel_id, active);

CREATE TABLE IF NOT EXISTS allowlist (
  project_code TEXT NOT NULL,
  user_id TEXT NOT NULL,
  note TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY (project_code, user_id)
);
