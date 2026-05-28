ALTER TABLE users ADD COLUMN access_source TEXT NOT NULL DEFAULT 'none';
ALTER TABLE users ADD COLUMN payment_method TEXT;
ALTER TABLE users ADD COLUMN boost_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE users ADD COLUMN boost_checked_at TEXT;
ALTER TABLE users ADD COLUMN boost_expires_at TEXT;
ALTER TABLE users ADD COLUMN star_paid_until TEXT;
ALTER TABLE users ADD COLUMN hide_inactive_year INTEGER NOT NULL DEFAULT 0;
ALTER TABLE channels ADD COLUMN last_video_at TEXT;
