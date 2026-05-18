interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  ADMIN_SECRET: string;
}

type TelegramUser = {
  id: number;
  username?: string;
  first_name?: string;
};

type TelegramMessage = {
  message_id: number;
  text?: string;
  chat: { id: number };
  from?: TelegramUser;
};

type TelegramCallbackQuery = {
  id: string;
  data?: string;
  from: TelegramUser;
  message?: TelegramMessage;
};

type TelegramUpdate = {
  message?: TelegramMessage;
  callback_query?: TelegramCallbackQuery;
};

type Project = {
  code: string;
  name: string;
  bot_token: string;
  webhook_secret: string;
  active: number;
};

type Channel = {
  channel_id: string;
  title: string;
  status: string;
};

type Category = {
  category_id: string;
  parent_id: string | null;
  title: string;
};

type SyncProject = {
  code: string;
  name: string;
  botToken: string;
  webhookSecret: string;
  active?: boolean;
  categories: Array<{
    id: string;
    parentId?: string | null;
    title: string;
    sortOrder?: number;
  }>;
  channels: Array<{
    id: string;
    title: string;
    categoryId?: string;
    status?: 'green' | 'red';
    sortOrder?: number;
  }>;
};

const jsonHeaders = { 'content-type': 'application/json; charset=utf-8' };

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname.split('/').filter(Boolean);

    try {
      if (request.method === 'GET' && path.length === 0) {
        return json({ ok: true, service: 'topus-telegram-subscriptions' });
      }

      if (request.method === 'POST' && path[0] === 'admin' && path[1] === 'sync') {
        return handleAdminSync(request, env, ctx);
      }

      if (request.method === 'POST' && path[0] === 'admin' && path[1] === 'notify') {
        return handleAdminNotify(request, env, ctx);
      }

      if (request.method === 'POST' && path[0] === 'telegram' && path.length === 3) {
        return handleTelegramWebhook(request, env, path[1], path[2]);
      }

      return json({ ok: false, error: 'not_found' }, 404);
    } catch (error) {
      console.error(JSON.stringify({ level: 'error', error: String(error) }));
      return json({ ok: false, error: 'internal_error' }, 500);
    }
  },
};

async function handleAdminSync(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  if (!isAuthorizedAdmin(request, env)) {
    return json({ ok: false, error: 'unauthorized' }, 401);
  }

  const body = await request.json<{ projects: SyncProject[] }>();
  const projects = Array.isArray(body.projects) ? body.projects : [];
  const now = new Date().toISOString();

  for (const project of projects) {
    await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO projects (code, name, bot_token, webhook_secret, active, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(code) DO UPDATE SET
           name = excluded.name,
           bot_token = excluded.bot_token,
           webhook_secret = excluded.webhook_secret,
           active = excluded.active,
           updated_at = excluded.updated_at`,
      ).bind(project.code, project.name, project.botToken, project.webhookSecret, project.active === false ? 0 : 1, now),
      env.DB.prepare('DELETE FROM categories WHERE project_code = ?').bind(project.code),
      env.DB.prepare('DELETE FROM channels WHERE project_code = ?').bind(project.code),
    ]);

    const categoryRows = [
      { id: 'root', parentId: null, title: 'Все каналы', sortOrder: 0 },
      ...project.categories,
    ];
    const categoryStatements = categoryRows.map((category) =>
      env.DB.prepare(
        `INSERT INTO categories (project_code, category_id, parent_id, title, sort_order, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
      ).bind(project.code, category.id, category.parentId ?? null, category.title, category.sortOrder ?? 0, now),
    );
    const channelStatements = project.channels.map((channel) =>
      env.DB.prepare(
        `INSERT INTO channels (project_code, channel_id, title, category_id, status, sort_order, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      ).bind(
        project.code,
        channel.id,
        channel.title,
        channel.categoryId || 'root',
        channel.status || 'green',
        channel.sortOrder ?? 0,
        now,
      ),
    );

    for (const chunk of chunks([...categoryStatements, ...channelStatements], 50)) {
      await env.DB.batch(chunk);
    }

    ctx.waitUntil(env.CACHE.delete(menuCacheKey(project.code)));
  }

  return json({ ok: true, projects: projects.length });
}

async function handleAdminNotify(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  if (!isAuthorizedAdmin(request, env)) {
    return json({ ok: false, error: 'unauthorized' }, 401);
  }

  const body = await request.json<{
    projectCode: string;
    channelId: string;
    text: string;
    parseMode?: string;
  }>();
  const project = await getProject(env, body.projectCode);
  if (!project) {
    return json({ ok: false, error: 'project_not_found' }, 404);
  }

  const recipients = await env.DB.prepare(
    `SELECT s.user_id
     FROM user_subscriptions s
     LEFT JOIN users u ON u.project_code = s.project_code AND u.user_id = s.user_id
     LEFT JOIN allowlist a ON a.project_code = s.project_code AND a.user_id = s.user_id
     WHERE s.project_code = ?
       AND s.channel_id = ?
       AND s.active = 1
       AND (COALESCE(u.is_paid, 0) = 1 OR COALESCE(u.is_allowlisted, 0) = 1 OR a.user_id IS NOT NULL)`,
  ).bind(body.projectCode, body.channelId).all<{ user_id: string }>();

  ctx.waitUntil(sendNotifications(project, recipients.results || [], body.text, body.parseMode || 'HTML'));
  return json({ ok: true, queued: recipients.results?.length || 0 });
}

async function handleTelegramWebhook(request: Request, env: Env, projectCode: string, secret: string): Promise<Response> {
  const project = await getProject(env, projectCode);
  if (!project || project.active !== 1 || !constantTimeEqual(project.webhook_secret, secret)) {
    return json({ ok: false, error: 'not_found' }, 404);
  }

  const update = await request.json<TelegramUpdate>();
  if (update.message) {
    await handleMessage(env, project, update.message);
  } else if (update.callback_query) {
    await handleCallback(env, project, update.callback_query);
  }

  return json({ ok: true });
}

async function handleMessage(env: Env, project: Project, message: TelegramMessage): Promise<void> {
  const text = (message.text || '').trim().toLowerCase();
  if (!message.from || !['/start', '/channels', '/subscriptions', 'каналы', 'подписки'].includes(text)) {
    return;
  }

  await upsertUser(env, project.code, message.from);
  const menu = await renderMenu(env, project.code, String(message.from.id), 'root');
  await telegram(project.bot_token, 'sendMessage', {
    chat_id: message.chat.id,
    text: 'Выберите категории и каналы:',
    reply_markup: menu,
  });
}

async function handleCallback(env: Env, project: Project, callback: TelegramCallbackQuery): Promise<void> {
  const data = callback.data || '';
  const message = callback.message;
  await upsertUser(env, project.code, callback.from);

  if (data === 'noop') {
    await answer(project.bot_token, callback.id);
    return;
  }

  const [action, value = 'root'] = data.split(':', 2);
  let categoryId = value || 'root';
  let toast = '';

  if (action === 'cat') {
    categoryId = value;
  } else if (action === 'back') {
    categoryId = (await parentCategory(env, project.code, value)) || 'root';
  } else if (action === 'toggle') {
    const channel = await getChannel(env, project.code, value);
    if (channel) {
      const active = await toggleSubscription(env, project.code, String(callback.from.id), channel.channel_id);
      categoryId = await channelCategory(env, project.code, channel.channel_id);
      toast = active ? 'Подписка включена' : 'Подписка отключена';
    }
  } else if (action === 'all' || action === 'none') {
    const channelIds = await descendantChannelIds(env, project.code, value);
    await setBulkSubscriptions(env, project.code, String(callback.from.id), channelIds, action === 'all');
    categoryId = value;
    toast = action === 'all' ? 'Включено на этом уровне' : 'Отключено на этом уровне';
  }

  await answer(project.bot_token, callback.id, toast);
  if (message) {
    const menu = await renderMenu(env, project.code, String(callback.from.id), categoryId);
    await telegram(project.bot_token, 'editMessageReplyMarkup', {
      chat_id: message.chat.id,
      message_id: message.message_id,
      reply_markup: menu,
    });
  }
}

async function renderMenu(env: Env, projectCode: string, userId: string, categoryId: string): Promise<object> {
  const [categories, channels, selected] = await Promise.all([
    childCategories(env, projectCode, categoryId),
    childChannels(env, projectCode, categoryId),
    selectedChannels(env, projectCode, userId),
  ]);

  const rows: Array<Array<{ text: string; callback_data: string }>> = [];

  for (const category of categories) {
    rows.push([{ text: `📁 ${category.title}`, callback_data: `cat:${category.category_id}` }]);
  }

  for (const channel of channels) {
    const marker = selected.has(channel.channel_id) ? '✅' : '☐';
    const status = channel.status === 'red' ? ' 🔴' : '';
    rows.push([{ text: `${marker} ${channel.title}${status}`, callback_data: `toggle:${channel.channel_id}` }]);
  }

  rows.push([
    { text: '✅ Все здесь', callback_data: `all:${categoryId}` },
    { text: '☐ Никого здесь', callback_data: `none:${categoryId}` },
  ]);

  if (categoryId !== 'root') {
    rows.push([{ text: '← Назад', callback_data: `back:${categoryId}` }]);
  }

  if (rows.length === 1) {
    rows.unshift([{ text: 'Пока нет каналов', callback_data: 'noop' }]);
  }

  return { inline_keyboard: rows };
}

async function upsertUser(env: Env, projectCode: string, user: TelegramUser): Promise<void> {
  const now = new Date().toISOString();
  const allowlisted = await env.DB.prepare(
    'SELECT 1 AS found FROM allowlist WHERE project_code = ? AND user_id = ? LIMIT 1',
  ).bind(projectCode, String(user.id)).first<{ found: number }>();

  await env.DB.prepare(
    `INSERT INTO users (project_code, user_id, username, first_name, is_allowlisted, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(project_code, user_id) DO UPDATE SET
       username = excluded.username,
       first_name = excluded.first_name,
       is_allowlisted = CASE WHEN users.is_allowlisted = 1 THEN 1 ELSE excluded.is_allowlisted END,
       updated_at = excluded.updated_at`,
  ).bind(
    projectCode,
    String(user.id),
    user.username || null,
    user.first_name || null,
    allowlisted ? 1 : 0,
    now,
    now,
  ).run();
}

async function toggleSubscription(env: Env, projectCode: string, userId: string, channelId: string): Promise<boolean> {
  const existing = await env.DB.prepare(
    'SELECT active FROM user_subscriptions WHERE project_code = ? AND user_id = ? AND channel_id = ?',
  ).bind(projectCode, userId, channelId).first<{ active: number }>();
  const nextActive = existing?.active === 1 ? 0 : 1;
  const now = new Date().toISOString();

  await env.DB.prepare(
    `INSERT INTO user_subscriptions (project_code, user_id, channel_id, active, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(project_code, user_id, channel_id) DO UPDATE SET
       active = excluded.active,
       updated_at = excluded.updated_at`,
  ).bind(projectCode, userId, channelId, nextActive, now, now).run();

  return nextActive === 1;
}

async function setBulkSubscriptions(
  env: Env,
  projectCode: string,
  userId: string,
  channelIds: string[],
  active: boolean,
): Promise<void> {
  if (channelIds.length === 0) {
    return;
  }
  const now = new Date().toISOString();
  const statements = channelIds.map((channelId) =>
    env.DB.prepare(
      `INSERT INTO user_subscriptions (project_code, user_id, channel_id, active, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(project_code, user_id, channel_id) DO UPDATE SET
         active = excluded.active,
         updated_at = excluded.updated_at`,
    ).bind(projectCode, userId, channelId, active ? 1 : 0, now, now),
  );
  for (const chunk of chunks(statements, 50)) {
    await env.DB.batch(chunk);
  }
}

async function getProject(env: Env, projectCode: string): Promise<Project | null> {
  return env.DB.prepare(
    'SELECT code, name, bot_token, webhook_secret, active FROM projects WHERE code = ?',
  ).bind(projectCode).first<Project>();
}

async function childCategories(env: Env, projectCode: string, parentId: string): Promise<Category[]> {
  const result = await env.DB.prepare(
    `SELECT category_id, parent_id, title
     FROM categories
     WHERE project_code = ? AND COALESCE(parent_id, 'root') = ?
     ORDER BY sort_order, title`,
  ).bind(projectCode, parentId).all<Category>();
  return result.results || [];
}

async function childChannels(env: Env, projectCode: string, categoryId: string): Promise<Channel[]> {
  const result = await env.DB.prepare(
    `SELECT channel_id, title, status
     FROM channels
     WHERE project_code = ? AND category_id = ?
     ORDER BY sort_order, title`,
  ).bind(projectCode, categoryId).all<Channel>();
  return result.results || [];
}

async function selectedChannels(env: Env, projectCode: string, userId: string): Promise<Set<string>> {
  const result = await env.DB.prepare(
    `SELECT channel_id
     FROM user_subscriptions
     WHERE project_code = ? AND user_id = ? AND active = 1`,
  ).bind(projectCode, userId).all<{ channel_id: string }>();
  return new Set((result.results || []).map((row) => row.channel_id));
}

async function getChannel(env: Env, projectCode: string, channelId: string): Promise<Channel | null> {
  return env.DB.prepare(
    'SELECT channel_id, title, status FROM channels WHERE project_code = ? AND channel_id = ?',
  ).bind(projectCode, channelId).first<Channel>();
}

async function channelCategory(env: Env, projectCode: string, channelId: string): Promise<string> {
  const row = await env.DB.prepare(
    'SELECT category_id FROM channels WHERE project_code = ? AND channel_id = ?',
  ).bind(projectCode, channelId).first<{ category_id: string }>();
  return row?.category_id || 'root';
}

async function parentCategory(env: Env, projectCode: string, categoryId: string): Promise<string | null> {
  const row = await env.DB.prepare(
    'SELECT parent_id FROM categories WHERE project_code = ? AND category_id = ?',
  ).bind(projectCode, categoryId).first<{ parent_id: string | null }>();
  return row?.parent_id || 'root';
}

async function descendantChannelIds(env: Env, projectCode: string, categoryId: string): Promise<string[]> {
  const channels = await childChannels(env, projectCode, categoryId);
  const categories = await childCategories(env, projectCode, categoryId);
  const nested = await Promise.all(categories.map((category) => descendantChannelIds(env, projectCode, category.category_id)));
  return [...channels.map((channel) => channel.channel_id), ...nested.flat()];
}

async function sendNotifications(project: Project, recipients: Array<{ user_id: string }>, text: string, parseMode: string): Promise<void> {
  for (const recipient of recipients) {
    await telegram(project.bot_token, 'sendMessage', {
      chat_id: recipient.user_id,
      text,
      parse_mode: parseMode,
      disable_web_page_preview: false,
    });
  }
}

async function telegram(botToken: string, method: string, payload: object): Promise<unknown> {
  const response = await fetch(`https://api.telegram.org/bot${botToken}/${method}`, {
    method: 'POST',
    headers: jsonHeaders,
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    console.error(JSON.stringify({ level: 'warn', method, status: response.status, body: await response.text() }));
    return null;
  }
  return response.json();
}

async function answer(botToken: string, callbackQueryId: string, text = ''): Promise<void> {
  await telegram(botToken, 'answerCallbackQuery', {
    callback_query_id: callbackQueryId,
    text,
  });
}

function isAuthorizedAdmin(request: Request, env: Env): boolean {
  const provided = request.headers.get('x-admin-secret') || '';
  return constantTimeEqual(provided, env.ADMIN_SECRET);
}

function constantTimeEqual(a: string, b: string): boolean {
  const left = new TextEncoder().encode(a);
  const right = new TextEncoder().encode(b);
  if (left.length !== right.length) {
    return false;
  }
  let diff = 0;
  for (let i = 0; i < left.length; i += 1) {
    diff |= left[i] ^ right[i];
  }
  return diff === 0;
}

function chunks<T>(items: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    result.push(items.slice(index, index + size));
  }
  return result;
}

function menuCacheKey(projectCode: string): string {
  return `menu:${projectCode}`;
}

function json(body: object, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: jsonHeaders,
  });
}
