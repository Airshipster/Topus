# Individual Telegram Bot Architecture

The existing Python publisher stays responsible for YouTube detection and main
Telegram channel publishing.

The interactive subscription menu is separate:

1. Google Sheets remains the admin source of truth.
2. Projects with `Бот = 🟢` are synced to Cloudflare Worker D1.
3. Channel rows with `🟢` and `🔴` are both synced for bot menus.
4. Telegram `/start` and button clicks go directly to Cloudflare Worker.
5. Worker reads/writes D1 only during user interaction, so clicks do not wait on
   Google Sheets.
6. Future monetization uses `users.is_paid` plus `allowlist` for manual free
   access.

Cloudflare usage is kept low:

- One Worker request per Telegram update.
- No polling.
- No Sheets calls on user clicks.
- Bulk subscribe/unsubscribe is one callback and batched D1 writes.

Current integration points:

- `src/sync_worker_config.py`: syncs Sheets project/channel config to Worker.
- `cloudflare/telegram-subscriptions-worker`: webhook Worker.
- `/admin/notify`: endpoint the publisher can call later to send a detected
  video to paid/allowlisted subscribers of a specific YouTube channel.
