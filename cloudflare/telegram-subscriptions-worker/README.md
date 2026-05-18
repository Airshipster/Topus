# Topus Telegram Subscriptions Worker

Interactive Telegram webhook for per-user YouTube channel subscriptions.

Architecture:

- Telegram sends `/start` and inline button callbacks directly to this Worker.
- D1 stores projects, categories, channels, users, allowlist, and user channel choices.
- KV is reserved for menu/config cache. The current implementation uses D1 directly for correctness and keeps KV binding ready for cache without changing API shape.
- Google Sheets remains the admin document. The Python sync script pushes project/channel config to `/admin/sync`.
- The current Python publisher is not changed by this Worker. Later it can call `/admin/notify` after detecting a new video.

Cost-aware behavior:

- One Worker request per Telegram message/click.
- No Google Sheets API calls on clicks.
- D1 reads/writes are small and local to Cloudflare bindings.
- Bulk subscribe/unsubscribe is one callback request and batched D1 statements.

Required secrets:

- `ADMIN_SECRET`: shared secret for `/admin/sync` and `/admin/notify`.

Deploy outline:

```powershell
npm install
npx wrangler d1 create topus-telegram-subscriptions
npx wrangler kv namespace create TOPUS_TELEGRAM_CACHE
```

Put the returned D1 database ID and KV namespace ID into `wrangler.jsonc`, then:

```powershell
npx wrangler secret put ADMIN_SECRET
npx wrangler d1 migrations apply topus-telegram-subscriptions --remote
npx wrangler deploy
```

Telegram webhook URL pattern:

```text
https://<worker-host>/telegram/<project_code>/<project_webhook_secret>
```
