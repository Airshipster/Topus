interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  ADMIN_SECRET: string;
  PAYMENTS_REQUIRED?: string;
  BOOSTER_MIN_BOOSTS?: string;
  STAR_PRICE_MONTHLY?: string;
}

type TelegramUser = {
  id: number;
  username?: string;
  first_name?: string;
};

type TelegramChat = {
  id: number;
  type?: string;
};

type TelegramMessage = {
  message_id: number;
  text?: string;
  chat: TelegramChat;
  from?: TelegramUser;
  new_chat_members?: TelegramUser[];
  successful_payment?: TelegramSuccessfulPayment;
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
  pre_checkout_query?: TelegramPreCheckoutQuery;
  chat_boost?: TelegramChatBoostUpdated;
  removed_chat_boost?: TelegramChatBoostRemoved;
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
  last_video_at?: string | null;
};

type Category = {
  category_id: string;
  parent_id: string | null;
  title: string;
  sort_order?: number;
};

type SyncProject = {
  code: string;
  name: string;
  botToken: string;
  botUsername?: string;
  webhookSecret: string;
  mainChannel?: string;
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
    lastVideoAt?: string;
  }>;
};

type FreeGrant = {
  userId: string;
  grant: AccessGrant;
  label: string;
};

type FreeGrantInput = {
  userId: string;
  details: string;
};

type AccessGrant = {
  expiresAt: string | null;
  months?: number;
};

type SubscriptionAccess = {
  status: 'free' | 'paid' | 'trial' | 'booster' | 'none';
  expiresAt: string | null;
  boostCount?: number;
};

type TelegramPreCheckoutQuery = {
  id: string;
  from: TelegramUser;
  currency?: string;
  total_amount?: number;
  invoice_payload?: string;
};

type TelegramSuccessfulPayment = {
  currency: string;
  total_amount: number;
  invoice_payload: string;
  telegram_payment_charge_id: string;
  provider_payment_charge_id?: string;
  subscription_expiration_date?: number;
  is_recurring?: boolean;
  is_first_recurring?: boolean;
};

type TelegramChatBoostUpdated = {
  chat: TelegramChat;
  boost?: TelegramChatBoost;
};

type TelegramChatBoostRemoved = {
  chat: TelegramChat;
  boost_id?: string;
  source?: TelegramChatBoostSource;
};

type TelegramChatBoost = {
  boost_id?: string;
  add_date?: number;
  expiration_date?: number;
  source?: TelegramChatBoostSource;
};

type TelegramChatBoostSource = {
  source?: string;
  user?: TelegramUser;
};

const jsonHeaders = { 'content-type': 'application/json; charset=utf-8' };
const CHANNELS_PER_PAGE = 20;
const SELECTED_MARK = '✅';
const UNSELECTED_MARK = '➕';
const BOOSTER_MIN_BOOSTS_DEFAULT = 3;
const DEFAULT_STAR_PRICE_MONTHLY = 100;
const CLOUDFLARE_MONTHLY_REQUEST_LIMIT = 100000;
const MAX_GROUP_HUMANS = 3;
const MAX_GROUP_TOTAL_MEMBERS = MAX_GROUP_HUMANS + 1;
const GROUP_LIMIT_TEXT = 'Бот работает только в личных чатах или небольших чатах до 3 человек. Сам бот не считается: максимум 3 участника плюс бот.';
type TelegramButton = { text: string; callback_data?: string; url?: string };
const WELCOME_TEXT = [
  'Добро пожаловать в бот SciTopus.',
  '',
  'В наш <a href="https://t.me/SciTopus">Telegram-канал</a> попадает только около 20% научпоп-каналов из базы SciTopus. Если вы хотите получать уведомления по большему числу каналов или собрать свою личную ленту из всего списка, настройте подписки здесь.',
  '',
  'Знак @ после названия канала означает, что этот канал уже выходит в основном Telegram-канале SciTopus. Каналы без @ полезны для личной ленты через бота.',
  '',
  'Можно выбрать отдельные каналы, категории или подписаться на всё, а потом отключить лишнее.',
  '',
  'Для работы бота нужна подписка на основной Telegram-канал @SciTopus.',
].join('\n');

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname.split('/').filter(Boolean);
    ctx.waitUntil(recordCloudflareRequest(env).catch((error) => {
      console.error(JSON.stringify({ level: 'warn', error: String(error), source: 'usage-counter' }));
    }));

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

      if (path[0] === 'admin' && (path[1] === 'sheet-state' || path[1] === 'state')) {
        return handleAdminSheetState(request, env);
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
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(auditBoosterAccess(env));
    if (event.cron === '0 15 * * SUN') {
      ctx.waitUntil(sendWeeklySubscriptionReminders(env));
    }
  },
};

async function handleAdminSync(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  if (!isAuthorizedAdmin(request, env)) {
    return json({ ok: false, error: 'unauthorized' }, 401);
  }
  await ensureAccessSchema(env);

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
    const channelActivityStatements = project.channels
      .filter((channel) => channel.lastVideoAt)
      .map((channel) =>
        env.DB.prepare(
          `UPDATE channels
           SET last_video_at = ?
           WHERE project_code = ? AND channel_id = ?`,
        ).bind(channel.lastVideoAt || null, project.code, channel.id),
      );

    for (const chunk of chunks([...categoryStatements, ...channelStatements, ...channelActivityStatements], 50)) {
      await env.DB.batch(chunk);
    }

    const mainChannel = normalizeTelegramChannel(project.mainChannel || '');
    const botUsername = normalizeTelegramChannel(project.botUsername || '');
    ctx.waitUntil(Promise.all([
      mainChannel
        ? env.CACHE.put(requiredChannelKey(project.code), mainChannel)
        : env.CACHE.delete(requiredChannelKey(project.code)),
      botUsername
        ? env.CACHE.put(botUsernameKey(project.code), botUsername)
        : env.CACHE.delete(botUsernameKey(project.code)),
      env.CACHE.delete(menuCacheKey(project.code)),
    ]));
  }

  return json({ ok: true, projects: projects.length });
}

async function handleAdminNotify(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  if (!isAuthorizedAdmin(request, env)) {
    return json({ ok: false, error: 'unauthorized' }, 401);
  }
  await ensureAccessSchema(env);

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

  const paymentCondition = paymentsRequired(env)
    ? `AND (
         (
           COALESCE(u.is_paid, 0) = 1
           AND (u.access_expires_at IS NULL OR u.access_expires_at = '' OR u.access_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
         )
         OR (
           COALESCE(u.is_allowlisted, 0) = 1
           AND (u.access_expires_at IS NULL OR u.access_expires_at = '' OR u.access_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
         )
         OR a.user_id IS NOT NULL
         OR (
           COALESCE(u.access_source, '') = 'booster'
           AND COALESCE(u.boost_count, 0) >= ${boosterMinBoosts(env)}
           AND (u.boost_expires_at IS NULL OR u.boost_expires_at = '' OR u.boost_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
         )
       )`
    : '';
  const recipients = await env.DB.prepare(
    `SELECT s.user_id
     FROM user_subscriptions s
     LEFT JOIN users u ON u.project_code = s.project_code AND u.user_id = s.user_id
     LEFT JOIN allowlist a ON a.project_code = s.project_code AND a.user_id = s.user_id
     WHERE s.project_code = ?
       AND s.channel_id = ?
       AND s.active = 1
       ${paymentCondition}`,
  ).bind(body.projectCode, body.channelId).all<{ user_id: string }>();

  const deliveries = await sendNotifications(project, recipients.results || [], body.text, body.parseMode || 'HTML', body.channelId);
  return json({
    ok: true,
    queued: recipients.results?.length || 0,
    sent: deliveries.filter((delivery) => delivery.messageId !== null).length,
    deliveries,
  });
}

async function handleAdminSheetState(request: Request, env: Env): Promise<Response> {
  if (!isAuthorizedAdmin(request, env)) {
    return json({ ok: false, error: 'unauthorized' }, 401);
  }
  await ensureAccessSchema(env);

  if (request.method === 'GET') {
    await ensureWeeklyReminderColumns(env);
    const [projectsResult, users, subscriptions, allowlist, channels, usage] = await Promise.all([
      env.DB.prepare(
        `SELECT code, name, active, updated_at
         FROM projects
         ORDER BY code`,
      ).all<{ code: string; name: string; active: number; updated_at: string }>(),
      env.DB.prepare(
        `SELECT project_code, user_id, username, first_name, is_paid, is_allowlisted, COALESCE(is_admin, 0) AS is_admin, access_expires_at,
                access_source, payment_method, boost_count, boost_checked_at, boost_expires_at, star_paid_until, hide_inactive_year,
                created_at, updated_at
         FROM users
         ORDER BY project_code, user_id`,
      ).all(),
      env.DB.prepare(
        `SELECT s.project_code, s.user_id, s.channel_id, c.title AS channel_title, s.active, s.created_at, s.updated_at
         FROM user_subscriptions s
         LEFT JOIN channels c ON c.project_code = s.project_code AND c.channel_id = s.channel_id
         ORDER BY s.project_code, s.user_id, c.sort_order, c.title`,
      ).all(),
      env.DB.prepare(
        `SELECT project_code, user_id, note, 1 AS active, created_at, created_at AS updated_at
         FROM allowlist
         ORDER BY project_code, user_id`,
      ).all(),
      env.DB.prepare(
        `SELECT project_code, channel_id, title, status, sort_order, last_video_at, updated_at
         FROM channels
         ORDER BY project_code, sort_order, title`,
      ).all(),
      getCloudflareUsage(env),
    ]);
    const projects = await Promise.all((projectsResult.results || []).map(async (project) => ({
      ...project,
      bot_username: await env.CACHE.get(botUsernameKey(String(project.code || ''))) || '',
    })));

    return json({
      ok: true,
      projects,
      users: users.results || [],
      subscriptions: subscriptions.results || [],
      allowlist: allowlist.results || [],
      channels: channels.results || [],
      usage,
      exportedAt: new Date().toISOString(),
    });
  }

  if (request.method !== 'POST') {
    return json({ ok: false, error: 'method_not_allowed' }, 405);
  }

  const body = await request.json<{
    users?: Array<{ projectCode: string; userId: string; username?: string; firstName?: string; isPaid?: boolean; isAllowlisted?: boolean; isAdmin?: boolean; accessExpiresAt?: string | null; accessSource?: string | null; paymentMethod?: string | null; boostCount?: number; boostCheckedAt?: string | null; boostExpiresAt?: string | null; starPaidUntil?: string | null; hideInactiveYear?: boolean }>;
    subscriptions?: Array<{ projectCode: string; userId: string; channelId: string; active?: boolean; delete?: boolean }>;
    allowlist?: Array<{ projectCode: string; userId: string; note?: string; active?: boolean; delete?: boolean }>;
  }>();
  const now = new Date().toISOString();
  const statements: D1PreparedStatement[] = [];

  for (const user of body.users || []) {
    if (!user.projectCode || !user.userId) {
      continue;
    }
    statements.push(env.DB.prepare(
      `INSERT INTO users (project_code, user_id, username, first_name, is_paid, is_allowlisted, is_admin, access_expires_at, access_source, payment_method, boost_count, boost_checked_at, boost_expires_at, star_paid_until, hide_inactive_year, weekly_reminders, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(project_code, user_id) DO UPDATE SET
         username = COALESCE(excluded.username, users.username),
         first_name = COALESCE(excluded.first_name, users.first_name),
         is_paid = excluded.is_paid,
         is_allowlisted = excluded.is_allowlisted,
         is_admin = excluded.is_admin,
         access_expires_at = excluded.access_expires_at,
         access_source = COALESCE(excluded.access_source, users.access_source),
         payment_method = excluded.payment_method,
         boost_count = excluded.boost_count,
         boost_checked_at = excluded.boost_checked_at,
         boost_expires_at = excluded.boost_expires_at,
         star_paid_until = excluded.star_paid_until,
         hide_inactive_year = excluded.hide_inactive_year,
         weekly_reminders = users.weekly_reminders,
         updated_at = excluded.updated_at`,
    ).bind(
      user.projectCode,
      user.userId,
      user.username || null,
      user.firstName || null,
      user.isPaid ? 1 : 0,
      user.isAllowlisted ? 1 : 0,
      user.isAdmin ? 1 : 0,
      user.accessExpiresAt || null,
      normalizeAccessSource(user.accessSource || (user.isPaid ? 'paid' : user.isAllowlisted ? 'free' : 'none')),
      user.paymentMethod || null,
      user.boostCount || 0,
      user.boostCheckedAt || null,
      user.boostExpiresAt || null,
      user.starPaidUntil || null,
      user.hideInactiveYear ? 1 : 0,
      0,
      now,
      now,
    ));
  }

  for (const subscription of body.subscriptions || []) {
    if (!subscription.projectCode || !subscription.userId || !subscription.channelId) {
      continue;
    }
    if (subscription.delete) {
      statements.push(env.DB.prepare(
        'DELETE FROM user_subscriptions WHERE project_code = ? AND user_id = ? AND channel_id = ?',
      ).bind(subscription.projectCode, subscription.userId, subscription.channelId));
    } else {
      statements.push(env.DB.prepare(
        `INSERT INTO user_subscriptions (project_code, user_id, channel_id, active, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(project_code, user_id, channel_id) DO UPDATE SET
           active = excluded.active,
           updated_at = excluded.updated_at`,
      ).bind(subscription.projectCode, subscription.userId, subscription.channelId, subscription.active === false ? 0 : 1, now, now));
    }
  }

  for (const entry of body.allowlist || []) {
    if (!entry.projectCode || !entry.userId) {
      continue;
    }
    if (entry.delete || entry.active === false) {
      statements.push(env.DB.prepare(
        'DELETE FROM allowlist WHERE project_code = ? AND user_id = ?',
      ).bind(entry.projectCode, entry.userId));
    } else {
      statements.push(env.DB.prepare(
        `INSERT INTO allowlist (project_code, user_id, note, created_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(project_code, user_id) DO UPDATE SET
           note = excluded.note,
           created_at = excluded.created_at`,
      ).bind(entry.projectCode, entry.userId, entry.note || '', now));
    }
  }

  for (const chunk of chunks(statements, 50)) {
    await env.DB.batch(chunk);
  }
  return json({ ok: true, applied: statements.length });
}

async function handleTelegramWebhook(request: Request, env: Env, projectCode: string, secret: string): Promise<Response> {
  await ensureAccessSchema(env);
  const project = await getProject(env, projectCode);
  if (!project || project.active !== 1 || !constantTimeEqual(project.webhook_secret, secret)) {
    return json({ ok: false, error: 'not_found' }, 404);
  }

  const update = await request.json<TelegramUpdate>();
  if (update.message) {
    await handleMessage(env, project, update.message);
  } else if (update.callback_query) {
    await handleCallback(env, project, update.callback_query);
  } else if (update.pre_checkout_query) {
    await handlePreCheckoutQuery(project, update.pre_checkout_query);
  } else if (update.chat_boost) {
    await handleChatBoostUpdate(env, project, update.chat_boost);
  } else if (update.removed_chat_boost) {
    await handleRemovedChatBoost(env, project, update.removed_chat_boost);
  }

  return json({ ok: true });
}

async function handleMessage(env: Env, project: Project, message: TelegramMessage): Promise<void> {
  if (!await enforceSmallChatLimit(project, message.chat)) {
    return;
  }

  const text = (message.text || '').trim().toLowerCase();
  if (!message.from) {
    return;
  }

  await upsertUser(env, project.code, message.from);

  if (message.successful_payment) {
    await handleSuccessfulPayment(env, project, message);
    return;
  }

  if (await handlePendingAdminMessage(env, project, message)) {
    return;
  }

  if (!['/start', '/menu', 'меню', '/channels', '/subscriptions', 'каналы', 'подписки'].includes(text)) {
    return;
  }

  const requiredChannel = await requiredChannelForProject(env, project.code);
  if (requiredChannel && !await hasRequiredChannelMembership(project, String(message.from.id), requiredChannel)) {
    await telegram(project.bot_token, 'sendMessage', {
      chat_id: message.chat.id,
      text: 'Чтобы пользоваться подписками, подпишитесь на основной канал проекта.',
      reply_markup: renderJoinChannelMenu(requiredChannel),
    });
    return;
  }

  const menu = await renderMainMenu(env, project.code, String(message.from.id));
  await telegram(project.bot_token, 'sendMessage', {
    chat_id: message.chat.id,
    text: WELCOME_TEXT,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    reply_markup: menu,
  });
}

async function handleCallback(env: Env, project: Project, callback: TelegramCallbackQuery): Promise<void> {
  const data = callback.data || '';
  const message = callback.message;
  if (message && !await enforceSmallChatLimit(project, message.chat)) {
    await answer(project.bot_token, callback.id, 'Бот работает только в чатах до 3 человек');
    return;
  }

  await upsertUser(env, project.code, callback.from);

  if (data === 'noop') {
    await answer(project.bot_token, callback.id);
    return;
  }

  const [action, value = 'root'] = data.split(':', 2);
  let categoryId = value || 'root';
  let page = 0;
  let toast = '';
  let menu: object | null = null;
  let text: string | null = null;

  if (action === 'unsub') {
    const channel = await getChannel(env, project.code, value);
    let subscribed = false;
    if (channel) {
      subscribed = await toggleSubscription(env, project.code, String(callback.from.id), channel.channel_id);
    }
    await answer(project.bot_token, callback.id, channel ? (subscribed ? 'Вы подписались на этот канал' : 'Вы отписались от этого канала') : 'Канал не найден');
    if (message && channel) {
      await telegram(project.bot_token, 'editMessageReplyMarkup', {
        chat_id: message.chat.id,
        message_id: message.message_id,
        reply_markup: renderNotificationMenu(channel.channel_id, subscribed),
      });
    }
    return;
  }

  if (action === 'adminstats' || action === 'grantfree' || action === 'adminsyncinfo') {
    const admin = await isAdmin(env, project.code, String(callback.from.id));
    if (!admin) {
      await answer(project.bot_token, callback.id, 'Недостаточно прав');
      return;
    }
    if (action === 'adminsyncinfo') {
      await answer(project.bot_token, callback.id, 'Синхронизация запускается из меню Topus в Google Sheets');
      if (message) {
        await telegram(project.bot_token, 'editMessageText', {
          chat_id: message.chat.id,
          message_id: message.message_id,
          text: 'Список категорий и каналов для бота обновляется из Google Sheets через пункт меню Topus -> Синхронизировать меню ботов. Автоматически это также запускается каждый день в 10:00 GMT+4.',
          reply_markup: renderAdminStatsMenu(),
        });
      }
      return;
    }
    if (action === 'adminstats') {
      await answer(project.bot_token, callback.id);
      if (message) {
        await telegram(project.bot_token, 'editMessageText', {
          chat_id: message.chat.id,
          message_id: message.message_id,
          text: await renderAdminStatsText(env, project.code),
          reply_markup: renderAdminStatsMenu(),
        });
      }
      return;
    }
    const pendingAdminKey = pendingAdminActionKey(project.code, String(callback.from.id));
    if (!value || value === 'root') {
      await env.CACHE.put(pendingAdminKey, 'grantfree:user', { expirationTtl: 600 });
      await answer(project.bot_token, callback.id);
      if (message) {
        await telegram(project.bot_token, 'editMessageText', {
          chat_id: message.chat.id,
          message_id: message.message_id,
          text: [
            'Отправьте ID пользователя, которому нужно выдать free-доступ.',
            '',
            'Можно отправить только ID, а срок выбрать кнопкой следующим шагом.',
            'Можно сразу указать срок: 123456789 до 20.06.2026',
          ].join('\n'),
          reply_markup: renderAdminCancelMenu(),
        });
      }
      return;
    }
    const pendingAction = await env.CACHE.get(pendingAdminKey);
    if (!pendingAction?.startsWith('grantfree:duration:')) {
      await env.CACHE.put(pendingAdminKey, 'grantfree:user', { expirationTtl: 600 });
      await answer(project.bot_token, callback.id, 'Сначала отправьте ID пользователя');
      if (message) {
        await telegram(project.bot_token, 'editMessageText', {
          chat_id: message.chat.id,
          message_id: message.message_id,
          text: 'Сначала отправьте ID пользователя, потом выберите срок.',
          reply_markup: renderAdminCancelMenu(),
        });
      }
      return;
    }
    await env.CACHE.delete(pendingAdminKey);
    const targetUserId = pendingAction.slice('grantfree:duration:'.length);
    const grant = parseFreeGrantGrant(value);
    const result = await grantFreeAccess(env, project.code, targetUserId, String(callback.from.id), grant);
    await answer(project.bot_token, callback.id, 'Free-доступ обновлён');
    if (message) {
      await telegram(project.bot_token, 'editMessageText', {
        chat_id: message.chat.id,
        message_id: message.message_id,
        text: `${result.statusLabel} для пользователя ${targetUserId}. Лист «Боты» обновится при следующей синхронизации.`,
        reply_markup: renderAdminStatsMenu(),
      });
    }
    return;
  }

  const requiredChannel = await requiredChannelForProject(env, project.code);

  if (action === 'checkjoin') {
    if (!requiredChannel || await hasRequiredChannelMembership(project, String(callback.from.id), requiredChannel)) {
      menu = await renderMainMenu(env, project.code, String(callback.from.id));
      text = WELCOME_TEXT;
      toast = 'Подписка на канал подтверждена';
    } else {
      menu = renderJoinChannelMenu(requiredChannel);
      text = 'Сначала подпишитесь на основной канал проекта.';
      toast = 'Подписка на канал не найдена';
    }
  } else if (requiredChannel && !await hasRequiredChannelMembership(project, String(callback.from.id), requiredChannel)) {
    menu = renderJoinChannelMenu(requiredChannel);
    text = 'Чтобы пользоваться подписками, подпишитесь на основной канал проекта.';
    toast = 'Сначала подпишитесь на канал';
  } else

  if (action === 'menu') {
    menu = await renderMainMenu(env, project.code, String(callback.from.id));
    text = WELCOME_TEXT;
  } else if (action === 'cats') {
    menu = await renderMenu(env, project.code, String(callback.from.id), 'root', 0);
    text = 'Выберите категорию';
  } else if (action === 'cat') {
    categoryId = value;
  } else if (action === 'page') {
    const parts = data.split(':');
    categoryId = parts[1] || 'root';
    page = Math.max(0, Number.parseInt(parts[2] || '0', 10) || 0);
  } else if (action === 'allch') {
    page = Math.max(0, Number.parseInt(value || '0', 10) || 0);
    menu = await renderAllChannels(env, project.code, String(callback.from.id), page);
    text = 'Все каналы';
  } else if (action === 'subs') {
    page = Math.max(0, Number.parseInt(value || '0', 10) || 0);
    menu = await renderSubscriptions(env, project.code, String(callback.from.id), page);
    text = 'Мои подписки';
  } else if (action === 'plan') {
    menu = await renderPlan(env, project.code, String(callback.from.id));
    text = await renderPlanText(env, project.code, String(callback.from.id));
  } else if (action === 'starsbuy') {
    await answer(project.bot_token, callback.id);
    const months = Math.max(1, Number.parseInt(value || '1', 10) || 1);
    if (message) {
      await sendStarsInvoice(env, project, message.chat.id, String(callback.from.id), months);
    }
    return;
  } else if (action === 'boostcheck') {
    const result = await refreshUserBoostAccess(env, project, String(callback.from.id), true);
    menu = await renderPlan(env, project.code, String(callback.from.id));
    text = [
      result.ok
        ? `Бусты подтверждены: ${result.count}/${boosterMinBoosts(env)}. Доступ активен как Booster.`
        : `Сейчас у нас видно ${result.count}/${boosterMinBoosts(env)} активных бустов от вас. После передачи бустов нажмите проверку ещё раз.`,
      '',
      await renderPlanText(env, project.code, String(callback.from.id)),
    ].join('\n');
    toast = result.ok ? 'Booster-доступ активен' : 'Бустов пока недостаточно';
  } else if (action === 'reminders') {
    menu = await renderReminderMenu(env, project.code, String(callback.from.id));
    text = REMINDER_SETTINGS_TEXT;
  } else if (action === 'settings') {
    menu = await renderSettingsMenu(env, project.code, String(callback.from.id));
    text = 'Настройки';
  } else if (action === 'hideold') {
    const enabled = !await hideInactiveYearEnabled(env, project.code, String(callback.from.id));
    await setHideInactiveYear(env, project.code, String(callback.from.id), enabled);
    menu = await renderSettingsMenu(env, project.code, String(callback.from.id));
    const visibility = await channelVisibilityStats(env, project.code);
    text = enabled
      ? `Каналы без публикаций больше года скрыты: ${visibility.hidden}/${visibility.total}. В списках осталось ${visibility.visible}. Если такой канал внезапно оживёт, он снова появится после обновления списка.`
      : 'Неактивные больше года каналы снова показываются в списках.';
    toast = enabled ? 'Скрытие включено' : 'Скрытие отключено';
  } else if (action === 'remtoggle' || action === 'remon' || action === 'remoff') {
    const enabled = action === 'remtoggle'
      ? !await weeklyRemindersEnabled(env, project.code, String(callback.from.id))
      : action === 'remon';
    await setWeeklyReminders(env, project.code, String(callback.from.id), enabled);
    menu = await renderReminderMenu(env, project.code, String(callback.from.id));
    text = enabled
      ? 'Еженедельные напоминания включены.'
      : 'Еженедельные напоминания отключены. Вы можете снова включить их через меню бота.';
    toast = enabled ? 'Напоминания включены' : 'Напоминания отключены';
  } else if (action === 'extra') {
    menu = renderExtraMenu();
    text = 'Дополнительно';
  } else if (action === 'back') {
    categoryId = (await parentCategory(env, project.code, value)) || 'root';
  } else if (action === 'toggle') {
    const parts = data.split(':');
    const channel = await getChannel(env, project.code, value);
    if (channel) {
      const active = await toggleSubscription(env, project.code, String(callback.from.id), channel.channel_id);
      categoryId = await channelCategory(env, project.code, channel.channel_id);
      page = Math.max(0, Number.parseInt(parts[2] || '0', 10) || 0);
      toast = active ? 'Подписка включена' : 'Подписка отключена';
    }
  } else if (action === 'toggleall') {
    const parts = data.split(':');
    const channel = await getChannel(env, project.code, value);
    if (channel) {
      await toggleSubscription(env, project.code, String(callback.from.id), channel.channel_id);
      page = Math.max(0, Number.parseInt(parts[2] || '0', 10) || 0);
      menu = await renderAllChannels(env, project.code, String(callback.from.id), page);
      text = 'Все каналы';
    }
  } else if (action === 'togglesub') {
    const parts = data.split(':');
    const channel = await getChannel(env, project.code, value);
    if (channel) {
      await toggleSubscription(env, project.code, String(callback.from.id), channel.channel_id);
      page = Math.max(0, Number.parseInt(parts[2] || '0', 10) || 0);
      menu = await renderSubscriptions(env, project.code, String(callback.from.id), page);
      text = 'Мои подписки';
    }
  } else if (action === 'clearsubs') {
    const channels = await subscribedChannels(env, project.code, String(callback.from.id));
    await setBulkSubscriptions(
      env,
      project.code,
      String(callback.from.id),
      channels.map((channel) => channel.channel_id),
      false,
    );
    menu = await renderSubscriptions(env, project.code, String(callback.from.id), 0);
    text = 'Мои подписки';
    toast = 'Все подписки отключены';
  } else if (action === 'all' || action === 'none') {
    const channelIds = await descendantChannelIds(env, project.code, value, String(callback.from.id));
    await setBulkSubscriptions(env, project.code, String(callback.from.id), channelIds, action === 'all');
    categoryId = value;
    toast = action === 'all' ? 'Подписка на все здесь включена' : 'Подписка на все здесь отключена';
  } else if (action === 'allall' || action === 'noneall') {
    const channels = await allChannels(env, project.code, String(callback.from.id));
    await setBulkSubscriptions(
      env,
      project.code,
      String(callback.from.id),
      channels.map((channel) => channel.channel_id),
      action === 'allall',
    );
    page = Math.max(0, Number.parseInt(value || '0', 10) || 0);
    menu = await renderAllChannels(env, project.code, String(callback.from.id), page);
    text = 'Все каналы';
    toast = action === 'allall' ? 'Подписка на все каналы включена' : 'Подписка на все каналы отключена';
  }

  await answer(project.bot_token, callback.id, toast);
  if (message) {
    const nextMenu = menu || await renderMenu(env, project.code, String(callback.from.id), categoryId, page);
    const payload: Record<string, unknown> = {
      chat_id: message.chat.id,
      message_id: message.message_id,
      reply_markup: nextMenu,
    };
    if (text) {
      payload.text = text;
      payload.parse_mode = 'HTML';
      payload.disable_web_page_preview = true;
      await telegram(project.bot_token, 'editMessageText', payload);
    } else {
      await telegram(project.bot_token, 'editMessageReplyMarkup', payload);
    }
  }
}

function renderJoinChannelMenu(channelRef: string): object {
  const rows: Array<Array<{ text: string; callback_data?: string; url?: string }>> = [];
  const url = telegramChannelUrl(channelRef);
  if (url) {
    rows.push([{ text: 'Открыть основной канал', url }]);
  }
  rows.push([{ text: 'Проверить подписку на канал', callback_data: 'checkjoin:root' }]);
  return { inline_keyboard: rows };
}

async function renderMainMenu(env: Env, projectCode: string, userId: string): Promise<object> {
  const [categoryCount, channelCount, subscriptionCount, status, admin] = await Promise.all([
    countChildCategories(env, projectCode, 'root'),
    countChannels(env, projectCode, userId),
    countSubscriptions(env, projectCode, userId),
    subscriptionStatus(env, projectCode, userId),
    isAdmin(env, projectCode, userId),
  ]);

  const statusLabel = status === 'booster'
    ? 'Подписка (booster)'
    : status === 'free' || status === 'trial'
      ? 'Подписка (free)'
      : status === 'paid'
        ? 'Подписка (paid)'
        : 'Подписка';
  const rows: Array<Array<TelegramButton>> = [
    [{ text: `📚 Категории (${categoryCount})`, callback_data: 'cats:root' }],
    [{ text: `✅ Мои подписки (${subscriptionCount}/${channelCount})`, callback_data: 'subs:0' }],
    [{ text: `📺 Все каналы (${channelCount})`, callback_data: 'allch:0' }],
    [{ text: statusLabel, callback_data: 'plan:root' }],
    [{ text: '⚙️ Настройки', callback_data: 'settings:root' }],
    [{ text: 'Дополнительно', callback_data: 'extra:root' }],
  ];
  if (admin) {
    rows.push([{ text: 'Статистика', callback_data: 'adminstats:root' }]);
    rows.push([{ text: 'Дать free-доступ', callback_data: 'grantfree:root' }]);
  }

  return { inline_keyboard: rows };
}

function countSelectedVisible(channels: Channel[], selected: Set<string>): number {
  return channels.filter((channel) => selected.has(channel.channel_id)).length;
}

function renderAdminStatsMenu(): object {
  return {
    inline_keyboard: [
      [{ text: '🔄 Меню ботов обновляется из Google Sheets', callback_data: 'adminsyncinfo:root' }],
      [{ text: '🏠 Главное меню', callback_data: 'menu:root' }],
    ],
  };
}

function renderAdminCancelMenu(): object {
  return {
    inline_keyboard: [
      [{ text: '🏠 Главное меню', callback_data: 'menu:root' }],
    ],
  };
}

function renderFreeGrantDurationMenu(): object {
  return {
    inline_keyboard: [
      [{ text: 'Навсегда', callback_data: 'grantfree:forever' }],
      ...chunks(
        Array.from({ length: 12 }, (_, index) => {
          const months = index + 1;
          return { text: String(months), callback_data: `grantfree:${months}` };
        }),
        4,
      ),
      [{ text: '🏠 Главное меню', callback_data: 'menu:root' }],
    ],
  };
}

function renderExtraMenu(): object {
  const rows: Array<Array<TelegramButton>> = [
    [{ text: 'Список научпоп YT-каналов', url: 'https://scitopus.com/youtube-list' }],
    [{ text: 'Наш Telegram-канал', url: 'https://t.me/SciTopus' }],
    [{ text: 'SciTopus в YouTube', url: 'https://www.youtube.com/@SciTopus' }],
    [{ text: 'SciTopus в VK', url: 'https://vk.com/scitopus' }],
    [{ text: 'Наши стикеры', url: 'https://t.me/addstickers/SciTopus_Octavius' }],
    [{ text: 'Подписаться на TG партнёров', url: 'https://t.me/addlist/J9zysqZsgN0wNGYy' }],
    [{ text: 'Наши другие TG-каналы', url: 'https://t.me/addlist/R-OEMFwg18A2ODhi' }],
    [{ text: '🏠 Главное меню', callback_data: 'menu:root' }],
  ];
  return { inline_keyboard: rows };
}

const REMINDER_SETTINGS_TEXT = [
  'Еженедельные напоминания помогают время от времени пересматривать подписки.',
  '',
  'Список каналов SciTopus может обновляться, а ваши интересы тоже могут меняться. Раз в неделю бот может напомнить открыть категории, подписаться на новые каналы или отключить лишнее.',
  '',
  'Напоминание приходит всем, кто его включил, в воскресенье вечером по московскому времени.',
].join('\n');

const WEEKLY_REMINDER_TEXT = [
  'Пора проверить подписки SciTopus.',
  '',
  'В списке могли появиться новые каналы, а ваши интересы могли измениться. Откройте категории, обновите вкусы: подпишитесь на новое или отключите лишнее.',
].join('\n');

async function renderReminderMenu(env: Env, projectCode: string, userId: string): Promise<object> {
  const enabled = await weeklyRemindersEnabled(env, projectCode, userId);
  const rows: Array<Array<TelegramButton>> = [
    [{ text: enabled ? '✅ Напоминания включены' : '➕ Включить напоминания', callback_data: 'remtoggle:root' }],
    [{ text: '📚 Открыть категории', callback_data: 'cats:root' }],
    [{ text: '🏠 Главное меню', callback_data: 'menu:root' }],
  ];
  return { inline_keyboard: rows };
}

function renderWeeklyReminderMessageMenu(): object {
  return {
    inline_keyboard: [
      [{ text: '📚 Обновить подписки', callback_data: 'cats:root' }],
      [{ text: 'Отключить еженедельные напоминания', callback_data: 'remoff:root' }],
    ],
  };
}

async function renderMenu(env: Env, projectCode: string, userId: string, categoryId: string, page: number): Promise<object> {
  const [categories, channels, selected, hiddenInfo] = await Promise.all([
    childCategories(env, projectCode, categoryId),
    childChannels(env, projectCode, categoryId, userId),
    selectedChannels(env, projectCode, userId),
    hiddenNoticeButton(env, projectCode, userId),
  ]);

  const rows: Array<Array<TelegramButton>> = [];

  for (const category of categories) {
    if (category.category_id === 'root') {
      continue;
    }
    const stats = await categoryStats(env, projectCode, userId, category.category_id);
    const prefix = stats.total > 0 && stats.selected === stats.total ? `${SELECTED_MARK} 📁` : '📁';
    rows.push([{ text: `${prefix} ${category.title} (${stats.selected}/${stats.total})`, callback_data: `cat:${category.category_id}` }]);
  }

  if (categoryId === 'root') {
    const totalChannels = await countChannels(env, projectCode, userId);
    const visibleChannels = await allChannels(env, projectCode, userId);
    const selectedVisible = countSelectedVisible(visibleChannels, selected);
    const allPrefix = totalChannels > 0 && selectedVisible === totalChannels ? `${SELECTED_MARK} 📺` : '📺';
    rows.push([{ text: `${allPrefix} Все каналы (${selectedVisible}/${totalChannels})`, callback_data: 'allch:0' }]);
    if (rows.length === 1) {
      rows.unshift([{ text: 'Категории пока не настроены', callback_data: 'noop' }]);
    }
    if (hiddenInfo) {
      rows.push([hiddenInfo]);
    }
    rows.push([{ text: '🏠 Главное меню', callback_data: 'menu:root' }]);
    return { inline_keyboard: rows };
  }

  const totalPages = Math.max(1, Math.ceil(channels.length / CHANNELS_PER_PAGE));
  const currentPage = Math.min(Math.max(0, page), totalPages - 1);
  const visibleChannels = channels.slice(
    currentPage * CHANNELS_PER_PAGE,
    currentPage * CHANNELS_PER_PAGE + CHANNELS_PER_PAGE,
  );

  for (const channel of visibleChannels) {
    const marker = selected.has(channel.channel_id) ? SELECTED_MARK : UNSELECTED_MARK;
    rows.push([{ text: `${marker} ${channel.title}`, callback_data: `toggle:${channel.channel_id}:${currentPage}` }]);
  }

  if (totalPages > 1) {
    const navRow: Array<TelegramButton> = [];
    if (currentPage > 0) {
      navRow.push({ text: '←', callback_data: `page:${categoryId}:${currentPage - 1}` });
    }
    navRow.push({ text: `${currentPage + 1}/${totalPages}`, callback_data: 'noop' });
    if (currentPage < totalPages - 1) {
      navRow.push({ text: '→', callback_data: `page:${categoryId}:${currentPage + 1}` });
    }
    rows.push(navRow);
  }

    rows.push([
    { text: '✅ Подписаться на все', callback_data: `all:${categoryId}` },
    { text: '➖ Отписаться от всех', callback_data: `none:${categoryId}` },
  ]);

  if (categoryId !== 'root') {
    rows.push([{ text: '← Назад', callback_data: `back:${categoryId}` }]);
  }
  if (hiddenInfo) {
    rows.push([hiddenInfo]);
  }
  rows.push([{ text: '🏠 Главное меню', callback_data: 'menu:root' }]);

  if (rows.length === 1) {
    rows.unshift([{ text: 'Пока нет каналов', callback_data: 'noop' }]);
  }

  return { inline_keyboard: rows };
}

async function renderAllChannels(env: Env, projectCode: string, userId: string, page: number): Promise<object> {
  const [channels, selected, hiddenInfo] = await Promise.all([
    allChannels(env, projectCode, userId),
    selectedChannels(env, projectCode, userId),
    hiddenNoticeButton(env, projectCode, userId),
  ]);
  return renderChannelList(channels, selected, page, 'toggleall', 'allch', true, true, false, hiddenInfo);
}

async function renderSubscriptions(env: Env, projectCode: string, userId: string, page: number): Promise<object> {
  const [channels, hiddenInfo] = await Promise.all([
    subscribedChannels(env, projectCode, userId),
    hiddenNoticeButton(env, projectCode, userId),
  ]);
  const selected = new Set(channels.map((channel) => channel.channel_id));
  if (channels.length === 0) {
    const rows: Array<Array<TelegramButton>> = [
      [{ text: 'Пока нет подписок', callback_data: 'noop' }],
      [{ text: '📺 Все каналы', callback_data: 'allch:0' }],
      [{ text: '📚 Выбрать категории', callback_data: 'cats:root' }],
    ];
    if (hiddenInfo) {
      rows.push([hiddenInfo]);
    }
    rows.push([{ text: '🏠 Главное меню', callback_data: 'menu:root' }]);
    return {
      inline_keyboard: rows,
    };
  }
  return renderChannelList(channels, selected, page, 'togglesub', 'subs', false, false, true, hiddenInfo);
}

async function renderPlan(env: Env, projectCode: string, userId: string): Promise<object> {
  const access = await subscriptionAccess(env, projectCode, userId);
  const label = access.status === 'free'
    ? 'У вас полный free-доступ без срока.'
    : access.status === 'trial'
      ? `У вас free-доступ до ${formatShortDate(access.expiresAt || '')}.`
      : access.status === 'booster'
        ? `У вас Booster-доступ: ${access.boostCount || 0}/${boosterMinBoosts(env)} бустов.`
      : access.status === 'paid'
        ? (access.expiresAt ? `У вас paid-доступ до ${formatShortDate(access.expiresAt)}.` : 'У вас paid-доступ без срока.')
        : 'Платная подписка будет подключена позже.';
  const boostUrl = await boostChannelUrl(env, projectCode);
  const rows: Array<Array<TelegramButton>> = [
    [{ text: label, callback_data: 'noop' }],
    [{ text: 'Проверить статус', callback_data: 'boostcheck:root' }],
    [{ text: `Оплатить ${starPriceMonthly(env)} Telegram Stars в месяц`, callback_data: 'starsbuy:1' }],
  ];
  if (boostUrl) {
    rows.push([{ text: `Передать ${boosterMinBoosts(env)} буста`, url: boostUrl }]);
  }
  rows.push([{ text: '⚙️ Настройки', callback_data: 'settings:root' }]);
  rows.push([{ text: '🏠 Главное меню', callback_data: 'menu:root' }]);
  return {
    inline_keyboard: rows,
  };
}

async function renderPlanText(env: Env, projectCode: string, userId: string): Promise<string> {
  const access = await subscriptionAccess(env, projectCode, userId);
  const statusLine = access.status === 'free'
    ? 'Сейчас у вас полный free-доступ без срока.'
    : access.status === 'trial'
      ? `Сейчас у вас временный free-доступ до ${formatShortDate(access.expiresAt || '')}.`
      : access.status === 'booster'
        ? `Сейчас у вас Booster-доступ: ${access.boostCount || 0}/${boosterMinBoosts(env)} бустов.`
        : access.status === 'paid'
          ? (access.expiresAt ? `Сейчас у вас paid-доступ до ${formatShortDate(access.expiresAt)}.` : 'Сейчас у вас paid-доступ без срока.')
          : 'Сейчас платный доступ не активен.';

  return [
    'Статус подписки',
    '',
    statusLine,
    '',
    'Все варианты ниже дают одинаковый полный доступ к личной ленте бота. Отличается только способ поддержки проекта.',
    '',
    `1. Бусты: передайте ${boosterMinBoosts(env)} буста основному каналу и нажмите «Проверить статус». Пока бусты активны, доступ работает как Booster.`,
    `2. Telegram Stars: ${starPriceMonthly(env)} Telegram Stars в месяц. Платёж оформляется внутри Telegram; регулярное списание поддерживается Telegram Stars-подписками.`,
    '3. TON/ручная оплата: можно подключить как ручной paid-доступ через администратора, если нужно принять оплату вне Telegram Stars.',
  ].join('\n');
}

async function renderSettingsMenu(env: Env, projectCode: string, userId: string): Promise<object> {
  const hideOld = await hideInactiveYearEnabled(env, projectCode, userId);
  const visibility = await channelVisibilityStats(env, projectCode);
  const hideLabel = hideOld
    ? `✅ Скрыто ${visibility.hidden}/${visibility.total}, видно ${visibility.visible}`
    : `Скрыть неактивные больше года (${visibility.hidden}/${visibility.total})`;
  return {
    inline_keyboard: [
      [{ text: hideLabel, callback_data: 'hideold:root' }],
      [{ text: '🔔 Напоминания', callback_data: 'reminders:root' }],
      [{ text: 'Проверить статус доступа', callback_data: 'plan:root' }],
      [{ text: '🏠 Главное меню', callback_data: 'menu:root' }],
    ],
  };
}

function renderChannelList(
  channels: Channel[],
  selected: Set<string>,
  page: number,
  toggleAction: string,
  pageAction: string,
  showUnselected: boolean,
  showBulkActions = false,
  showClearSubscriptions = false,
  hiddenInfo: TelegramButton | null = null,
): object {
  const rows: Array<Array<TelegramButton>> = [];
  const totalPages = Math.max(1, Math.ceil(channels.length / CHANNELS_PER_PAGE));
  const currentPage = Math.min(Math.max(0, page), totalPages - 1);
  const visibleChannels = channels.slice(
    currentPage * CHANNELS_PER_PAGE,
    currentPage * CHANNELS_PER_PAGE + CHANNELS_PER_PAGE,
  );

  for (const channel of visibleChannels) {
    const marker = selected.has(channel.channel_id) ? SELECTED_MARK : UNSELECTED_MARK;
    const prefix = showUnselected ? `${marker} ` : `${SELECTED_MARK} `;
    rows.push([{ text: `${prefix}${channel.title}${channelMainFeedLabel(channel)}`, callback_data: `${toggleAction}:${channel.channel_id}:${currentPage}` }]);
  }

  if (totalPages > 1) {
    const navRow: Array<TelegramButton> = [];
    if (currentPage > 0) {
      navRow.push({ text: '←', callback_data: `${pageAction}:${currentPage - 1}` });
    }
    navRow.push({ text: `${currentPage + 1}/${totalPages}`, callback_data: 'noop' });
    if (currentPage < totalPages - 1) {
      navRow.push({ text: '→', callback_data: `${pageAction}:${currentPage + 1}` });
    }
    rows.push(navRow);
  }

  if (showBulkActions) {
    rows.push([
      { text: '✅ Подписаться на все', callback_data: `allall:${currentPage}` },
      { text: '➖ Отписаться от всех', callback_data: `noneall:${currentPage}` },
    ]);
  }
  if (showClearSubscriptions) {
    rows.push([{ text: '➖ Отписаться от всех', callback_data: 'clearsubs:0' }]);
    rows.push([{ text: '📺 Все каналы', callback_data: 'allch:0' }]);
    rows.push([{ text: '📚 Категории', callback_data: 'cats:root' }]);
  }

  if (hiddenInfo) {
    rows.push([hiddenInfo]);
  }
  rows.push([{ text: '🏠 Главное меню', callback_data: 'menu:root' }]);
  return { inline_keyboard: rows };
}

function channelMainFeedLabel(channel: Channel): string {
  return channel.status === 'green' ? ' @' : '';
}

async function upsertUser(env: Env, projectCode: string, user: TelegramUser): Promise<void> {
  await ensureWeeklyReminderColumns(env);
  const now = new Date().toISOString();
  const allowlisted = await env.DB.prepare(
    'SELECT 1 AS found FROM allowlist WHERE project_code = ? AND user_id = ? LIMIT 1',
  ).bind(projectCode, String(user.id)).first<{ found: number }>();

  await env.DB.prepare(
    `INSERT INTO users (project_code, user_id, username, first_name, is_allowlisted, is_admin, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(project_code, user_id) DO UPDATE SET
       username = excluded.username,
       first_name = excluded.first_name,
       is_allowlisted = CASE WHEN users.is_allowlisted = 1 THEN 1 ELSE excluded.is_allowlisted END,
       access_expires_at = users.access_expires_at,
       is_admin = users.is_admin,
       updated_at = excluded.updated_at`,
  ).bind(
    projectCode,
    String(user.id),
    user.username || null,
    user.first_name || null,
    allowlisted ? 1 : 0,
    0,
    now,
    now,
  ).run();
}

async function weeklyRemindersEnabled(env: Env, projectCode: string, userId: string): Promise<boolean> {
  await ensureWeeklyReminderColumns(env);
  const row = await env.DB.prepare(
    'SELECT COALESCE(weekly_reminders, 0) AS weekly_reminders FROM users WHERE project_code = ? AND user_id = ? LIMIT 1',
  ).bind(projectCode, userId).first<{ weekly_reminders: number }>();
  return row?.weekly_reminders === 1;
}

async function setWeeklyReminders(env: Env, projectCode: string, userId: string, enabled: boolean): Promise<void> {
  await ensureWeeklyReminderColumns(env);
  const now = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO users (project_code, user_id, weekly_reminders, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(project_code, user_id) DO UPDATE SET
       weekly_reminders = excluded.weekly_reminders,
       updated_at = excluded.updated_at`,
  ).bind(projectCode, userId, enabled ? 1 : 0, now, now).run();
}

async function handlePendingAdminMessage(env: Env, project: Project, message: TelegramMessage): Promise<boolean> {
  const userId = String(message.from?.id || '');
  if (!userId || !await isAdmin(env, project.code, userId)) {
    return false;
  }
  const key = pendingAdminActionKey(project.code, userId);
  const action = await env.CACHE.get(key);
  if (!action?.startsWith('grantfree')) {
    return false;
  }

  if (action === 'grantfree:user') {
    const input = parseFreeGrantInput(message.text || '');
    if (!input) {
      await env.CACHE.delete(key);
      await telegram(project.bot_token, 'sendMessage', {
        chat_id: message.chat.id,
        text: 'Не вижу ID пользователя. Нажмите «Дать free-доступ» ещё раз и отправьте ID, например: 123456789.',
        reply_markup: renderAdminCancelMenu(),
      });
      return true;
    }
    if (input.details) {
      await env.CACHE.delete(key);
      const grant = parseAdminFreeGrant(message.text || '', 'forever');
      if (!grant) {
        return true;
      }
      const result = await grantFreeAccess(env, project.code, grant.userId, userId, grant.grant);
      await telegram(project.bot_token, 'sendMessage', {
        chat_id: message.chat.id,
        text: `${result.statusLabel} для пользователя ${grant.userId}. Лист «Боты» обновится при следующей синхронизации.`,
        reply_markup: renderAdminStatsMenu(),
      });
      return true;
    }
    await env.CACHE.put(key, `grantfree:duration:${input.userId}`, { expirationTtl: 600 });
    await telegram(project.bot_token, 'sendMessage', {
      chat_id: message.chat.id,
      text: `Выберите срок free-доступа для пользователя ${input.userId}.`,
      reply_markup: renderFreeGrantDurationMenu(),
    });
    return true;
  }

  if (!action.startsWith('grantfree:duration:')) {
    await env.CACHE.delete(key);
    return true;
  }

  await env.CACHE.delete(key);
  const targetUserId = action.slice('grantfree:duration:'.length);
  const grant = parseFreeGrantGrant((message.text || '').trim() || 'forever');
  const result = await grantFreeAccess(env, project.code, targetUserId, userId, grant);
  await telegram(project.bot_token, 'sendMessage', {
    chat_id: message.chat.id,
    text: `${result.statusLabel} для пользователя ${targetUserId}. Лист «Боты» обновится при следующей синхронизации.`,
    reply_markup: renderAdminStatsMenu(),
  });
  return true;
}

async function grantFreeAccess(env: Env, projectCode: string, targetUserId: string, adminUserId: string, grant: AccessGrant): Promise<{ statusLabel: string }> {
  const now = new Date().toISOString();
  const existing = await env.DB.prepare(
    'SELECT COALESCE(is_paid, 0) AS is_paid, access_expires_at FROM users WHERE project_code = ? AND user_id = ? LIMIT 1',
  ).bind(projectCode, targetUserId).first<{ is_paid: number; access_expires_at: string | null }>();
  const existingPaid = existing?.is_paid === 1;
  const expiresAt = resolveGrantedExpiry(grant, existing?.access_expires_at || null, existingPaid);
  const keepPaid = Boolean(expiresAt && existingPaid);
  const statements = [
    env.DB.prepare(
      `INSERT INTO users (project_code, user_id, is_paid, is_allowlisted, is_admin, access_expires_at, created_at, updated_at)
       VALUES (?, ?, ?, ?, 0, ?, ?, ?)
       ON CONFLICT(project_code, user_id) DO UPDATE SET
         is_paid = excluded.is_paid,
         is_allowlisted = excluded.is_allowlisted,
         access_expires_at = excluded.access_expires_at,
         updated_at = excluded.updated_at`,
    ).bind(projectCode, targetUserId, keepPaid ? 1 : 0, keepPaid ? 0 : 1, expiresAt, now, now),
  ];
  if (expiresAt || keepPaid) {
    statements.push(env.DB.prepare(
      'DELETE FROM allowlist WHERE project_code = ? AND user_id = ?',
    ).bind(projectCode, targetUserId));
  } else {
    statements.push(env.DB.prepare(
      `INSERT INTO allowlist (project_code, user_id, note, created_at)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(project_code, user_id) DO UPDATE SET
         note = excluded.note,
         created_at = excluded.created_at`,
    ).bind(projectCode, targetUserId, `granted by admin ${adminUserId}`, now));
  }
  await env.DB.batch(statements);
  if (keepPaid) {
    return { statusLabel: `Paid-доступ продлён до ${formatShortDate(expiresAt || '')}` };
  }
  return { statusLabel: expiresAt ? `Free-доступ выдан до ${formatShortDate(expiresAt)}` : 'Free-доступ выдан навсегда' };
}

function parseAdminFreeGrant(text: string, defaultDuration = 'forever'): FreeGrant | null {
  const input = parseFreeGrantInput(text);
  if (!input) {
    return null;
  }
  const grant = parseFreeGrantGrant(input.details || defaultDuration);
  return {
    userId: input.userId,
    grant,
    label: grant.expiresAt ? `выдан до ${formatShortDate(grant.expiresAt)}` : 'выдан навсегда',
  };
}

function parseFreeGrantInput(text: string): FreeGrantInput | null {
  const trimmed = text.trim();
  const match = trimmed.match(/-?\d+/);
  if (!match) {
    return null;
  }
  const userId = match[0];
  const details = trimmed
    .slice(0, match.index)
    .concat(' ', trimmed.slice((match.index || 0) + match[0].length))
    .replace(/[,;]/g, ' ')
    .trim()
    .toLowerCase();
  return {
    userId,
    details,
  };
}

function parseFreeGrantGrant(details: string): AccessGrant {
  if (!details || /^(free|навсегда|вечн(?:о|ый)?|forever|permanent)$/i.test(details)) {
    return { expiresAt: null };
  }
  if (/\b(год|year)\b/.test(details)) {
    return { expiresAt: addMonths(new Date(), 12).toISOString(), months: 12 };
  }
  if (/\b(полгода|half\s*year)\b/.test(details)) {
    return { expiresAt: addMonths(new Date(), 6).toISOString(), months: 6 };
  }
  const monthMatch = details.match(/\b(1[0-2]|[1-9])\b/);
  if (monthMatch) {
    const months = Number.parseInt(monthMatch[1], 10);
    return { expiresAt: addMonths(new Date(), months).toISOString(), months };
  }
  const isoDate = details.match(/\b(\d{4})-(\d{1,2})-(\d{1,2})\b/);
  if (isoDate) {
    return { expiresAt: new Date(Date.UTC(Number(isoDate[1]), Number(isoDate[2]) - 1, Number(isoDate[3]), 23, 59, 59)).toISOString() };
  }
  const dotDate = details.match(/\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b/);
  if (dotDate) {
    return { expiresAt: new Date(Date.UTC(Number(dotDate[3]), Number(dotDate[2]) - 1, Number(dotDate[1]), 23, 59, 59)).toISOString() };
  }
  return { expiresAt: null };
}

function parseFreeGrantExpiry(details: string): string | null {
  return parseFreeGrantGrant(details).expiresAt;
}

function resolveGrantedExpiry(grant: AccessGrant, existingExpiry: string | null, existingPaid: boolean): string | null {
  if (!grant.expiresAt) {
    return null;
  }
  if (!existingPaid || !grant.months) {
    return grant.expiresAt;
  }
  const now = new Date();
  const existing = existingExpiry ? new Date(existingExpiry) : null;
  const base = existing && !Number.isNaN(existing.getTime()) && existing > now ? existing : now;
  return addMonths(base, grant.months).toISOString();
}

function addMonths(date: Date, months: number): Date {
  const next = new Date(date.getTime());
  const targetMonth = next.getUTCMonth() + months;
  next.setUTCMonth(targetMonth);
  if (next.getUTCMonth() !== ((targetMonth % 12) + 12) % 12) {
    next.setUTCDate(0);
  }
  return next;
}

function formatShortDate(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const day = String(date.getUTCDate()).padStart(2, '0');
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  return `${day}.${month}.${date.getUTCFullYear()}`;
}

async function handlePreCheckoutQuery(project: Project, query: TelegramPreCheckoutQuery): Promise<void> {
  const payload = parseStarsPayload(query.invoice_payload || '');
  const ok = query.currency === 'XTR' && payload !== null && payload.projectCode === project.code;
  await telegram(project.bot_token, 'answerPreCheckoutQuery', {
    pre_checkout_query_id: query.id,
    ok,
    error_message: ok ? undefined : 'Не удалось проверить платёж. Попробуйте открыть оплату заново.',
  });
}

async function handleSuccessfulPayment(env: Env, project: Project, message: TelegramMessage): Promise<void> {
  const payment = message.successful_payment;
  if (!payment || payment.currency !== 'XTR' || !message.from) {
    return;
  }
  const payload = parseStarsPayload(payment.invoice_payload);
  if (!payload || payload.projectCode !== project.code || payload.userId !== String(message.from.id)) {
    await telegram(project.bot_token, 'sendMessage', {
      chat_id: message.chat.id,
      text: 'Платёж получен, но не удалось сопоставить его с подпиской. Напишите администратору.',
    });
    return;
  }
  const expiresAt = payment.subscription_expiration_date
    ? new Date(payment.subscription_expiration_date * 1000).toISOString()
    : addMonths(new Date(), payload.months).toISOString();
  await setPaidAccess(env, project.code, String(message.from.id), expiresAt, 'stars');
  await telegram(project.bot_token, 'sendMessage', {
    chat_id: message.chat.id,
    text: `Спасибо за поддержку звёздами. Paid-доступ активен до ${formatShortDate(expiresAt)}. Если это регулярная подписка, Telegram будет продлевать её автоматически при наличии звёзд на балансе.`,
    reply_markup: await renderMainMenu(env, project.code, String(message.from.id)),
  });
}

async function sendStarsInvoice(env: Env, project: Project, chatId: number, userId: string, months: number): Promise<void> {
  const amount = starPriceMonthly(env) * months;
  await telegram(project.bot_token, 'sendInvoice', {
    chat_id: chatId,
    title: 'Подписка SciTopus Bot',
    description: `Доступ к личной ленте за ${amount} звёзд в месяц.`,
    payload: `stars:${project.code}:${months}:${userId}`,
    provider_token: '',
    currency: 'XTR',
    prices: [{ label: `${months} мес.`, amount }],
    subscription_period: 30 * 24 * 60 * 60,
  });
}

function parseStarsPayload(payload: string): { projectCode: string; months: number; userId: string } | null {
  const match = String(payload || '').match(/^stars:([^:]+):(\d+):(-?\d+)$/);
  if (!match) {
    return null;
  }
  return {
    projectCode: match[1],
    months: Math.max(1, Number.parseInt(match[2], 10) || 1),
    userId: match[3],
  };
}

async function setPaidAccess(env: Env, projectCode: string, userId: string, expiresAt: string | null, paymentMethod: string): Promise<void> {
  const now = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO users (project_code, user_id, is_paid, is_allowlisted, access_expires_at, access_source, payment_method, star_paid_until, boost_count, boost_checked_at, boost_expires_at, created_at, updated_at)
     VALUES (?, ?, 1, 0, ?, 'paid', ?, ?, 0, NULL, NULL, ?, ?)
     ON CONFLICT(project_code, user_id) DO UPDATE SET
       is_paid = 1,
       is_allowlisted = 0,
       access_expires_at = excluded.access_expires_at,
       access_source = 'paid',
       payment_method = excluded.payment_method,
       star_paid_until = excluded.star_paid_until,
       boost_count = 0,
       boost_checked_at = NULL,
       boost_expires_at = NULL,
       updated_at = excluded.updated_at`,
  ).bind(projectCode, userId, expiresAt, paymentMethod, expiresAt, now, now).run();
  await env.DB.prepare(
    'DELETE FROM allowlist WHERE project_code = ? AND user_id = ?',
  ).bind(projectCode, userId).run();
}

async function handleChatBoostUpdate(env: Env, project: Project, update: TelegramChatBoostUpdated): Promise<void> {
  const user = update.boost?.source?.user;
  if (!user) {
    return;
  }
  await upsertUser(env, project.code, user);
  await refreshUserBoostAccess(env, project, String(user.id), false);
}

async function handleRemovedChatBoost(env: Env, project: Project, update: TelegramChatBoostRemoved): Promise<void> {
  const user = update.source?.user;
  if (!user) {
    return;
  }
  await refreshUserBoostAccess(env, project, String(user.id), false);
}

async function refreshUserBoostAccess(env: Env, project: Project, userId: string, notifyFailure: boolean): Promise<{ ok: boolean; count: number }> {
  const requiredChannel = await requiredChannelForProject(env, project.code);
  if (!requiredChannel) {
    return { ok: false, count: 0 };
  }
  const result = await telegram(project.bot_token, 'getUserChatBoosts', {
    chat_id: requiredChannel,
    user_id: userId,
  }) as { ok?: boolean; result?: { boosts?: TelegramChatBoost[] } } | null;
  if (!result?.ok) {
    if (notifyFailure) {
      console.error(JSON.stringify({ level: 'warn', source: 'boost-check', project: project.code, userId, error: 'getUserChatBoosts failed' }));
    }
    return { ok: false, count: 0 };
  }
  const nowSeconds = Math.floor(Date.now() / 1000);
  const activeBoosts = (result.result?.boosts || []).filter((boost) => !boost.expiration_date || boost.expiration_date > nowSeconds);
  const expiresAt = activeBoosts
    .map((boost) => boost.expiration_date || 0)
    .filter(Boolean)
    .sort((a, b) => a - b)[0];
  await setBoosterAccess(env, project.code, userId, activeBoosts.length, expiresAt ? new Date(expiresAt * 1000).toISOString() : null);
  return { ok: activeBoosts.length >= boosterMinBoosts(env), count: activeBoosts.length };
}

async function setBoosterAccess(env: Env, projectCode: string, userId: string, boostCount: number, expiresAt: string | null): Promise<void> {
  const now = new Date().toISOString();
  const hasAccess = boostCount >= boosterMinBoosts(env);
  const current = await env.DB.prepare(
    'SELECT COALESCE(access_source, "") AS access_source FROM users WHERE project_code = ? AND user_id = ? LIMIT 1',
  ).bind(projectCode, userId).first<{ access_source: string }>();
  const shouldRemoveBooster = current?.access_source === 'booster' && !hasAccess;
  await env.DB.prepare(
    `INSERT INTO users (project_code, user_id, is_paid, is_allowlisted, access_expires_at, access_source, payment_method, boost_count, boost_checked_at, boost_expires_at, created_at, updated_at)
     VALUES (?, ?, 0, 0, NULL, ?, 'boost', ?, ?, ?, ?, ?)
     ON CONFLICT(project_code, user_id) DO UPDATE SET
       is_paid = CASE WHEN excluded.access_source = 'booster' THEN 0 ELSE users.is_paid END,
       is_allowlisted = CASE WHEN excluded.access_source = 'booster' THEN 0 ELSE users.is_allowlisted END,
       access_expires_at = CASE WHEN excluded.access_source = 'booster' THEN NULL ELSE users.access_expires_at END,
       access_source = CASE WHEN excluded.access_source = 'booster' OR users.access_source = 'booster' THEN excluded.access_source ELSE users.access_source END,
       payment_method = CASE WHEN excluded.access_source = 'booster' THEN 'boost' ELSE users.payment_method END,
       boost_count = excluded.boost_count,
       boost_checked_at = excluded.boost_checked_at,
       boost_expires_at = excluded.boost_expires_at,
       updated_at = excluded.updated_at`,
  ).bind(projectCode, userId, hasAccess ? 'booster' : (shouldRemoveBooster ? 'none' : 'none'), boostCount, now, expiresAt, now, now).run();
}

async function auditBoosterAccess(env: Env): Promise<void> {
  await ensureAccessSchema(env);
  const rows = await env.DB.prepare(
    `SELECT p.code AS project_code, p.bot_token, u.user_id
     FROM users u
     JOIN projects p ON p.code = u.project_code
     WHERE p.active = 1 AND COALESCE(u.access_source, '') = 'booster'
     ORDER BY p.code, u.user_id`,
  ).all<{ project_code: string; bot_token: string; user_id: string }>();
  for (const row of rows.results || []) {
    const project = await getProject(env, row.project_code);
    if (!project) {
      continue;
    }
    const before = await subscriptionAccess(env, row.project_code, row.user_id);
    const result = await refreshUserBoostAccess(env, project, row.user_id, false);
    if (before.status === 'booster' && !result.ok) {
      await telegram(row.bot_token, 'sendMessage', {
        chat_id: row.user_id,
        text: `Мы больше не видим ${boosterMinBoosts(env)} активных бустов от вас, поэтому Booster-доступ отключён. Его можно вернуть через меню подписки.`,
      });
    }
  }
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

async function childChannels(env: Env, projectCode: string, categoryId: string, userId = ''): Promise<Channel[]> {
  const result = await env.DB.prepare(
    `SELECT channel_id, title, status, last_video_at
     FROM channels
     WHERE project_code = ? AND category_id = ?
     ORDER BY sort_order, title`,
  ).bind(projectCode, categoryId).all<Channel>();
  return filterVisibleChannels(result.results || [], await hideInactiveYearEnabled(env, projectCode, userId));
}

async function allChannels(env: Env, projectCode: string, userId = ''): Promise<Channel[]> {
  const result = await env.DB.prepare(
    `SELECT channel_id, title, status, last_video_at
     FROM channels
     WHERE project_code = ?
     ORDER BY sort_order, title`,
  ).bind(projectCode).all<Channel>();
  return filterVisibleChannels(result.results || [], await hideInactiveYearEnabled(env, projectCode, userId));
}

async function subscribedChannels(env: Env, projectCode: string, userId: string): Promise<Channel[]> {
  const result = await env.DB.prepare(
    `SELECT c.channel_id, c.title, c.status, c.last_video_at
     FROM user_subscriptions s
     JOIN channels c ON c.project_code = s.project_code AND c.channel_id = s.channel_id
     WHERE s.project_code = ? AND s.user_id = ? AND s.active = 1
    ORDER BY c.sort_order, c.title`,
  ).bind(projectCode, userId).all<Channel>();
  return filterVisibleChannels(result.results || [], await hideInactiveYearEnabled(env, projectCode, userId));
}

async function selectedChannels(env: Env, projectCode: string, userId: string): Promise<Set<string>> {
  const result = await env.DB.prepare(
    `SELECT channel_id
     FROM user_subscriptions
     WHERE project_code = ? AND user_id = ? AND active = 1`,
  ).bind(projectCode, userId).all<{ channel_id: string }>();
  return new Set((result.results || []).map((row) => row.channel_id));
}

async function categoryStats(env: Env, projectCode: string, userId: string, categoryId: string): Promise<{ selected: number; total: number }> {
  const channelIds = await descendantChannelIds(env, projectCode, categoryId, userId);
  if (channelIds.length === 0) {
    return { selected: 0, total: 0 };
  }
  const selected = await selectedChannels(env, projectCode, userId);
  let selectedCount = 0;
  for (const channelId of channelIds) {
    if (selected.has(channelId)) {
      selectedCount += 1;
    }
  }
  return { selected: selectedCount, total: channelIds.length };
}

async function countChildCategories(env: Env, projectCode: string, parentId: string): Promise<number> {
  const row = await env.DB.prepare(
    `SELECT COUNT(*) AS count
     FROM categories
     WHERE project_code = ? AND COALESCE(parent_id, 'root') = ?`,
  ).bind(projectCode, parentId).first<{ count: number }>();
  return row?.count || 0;
}

async function countChannels(env: Env, projectCode: string, userId = ''): Promise<number> {
  if (await hideInactiveYearEnabled(env, projectCode, userId)) {
    return (await allChannels(env, projectCode, userId)).length;
  }
  const row = await env.DB.prepare(
    'SELECT COUNT(*) AS count FROM channels WHERE project_code = ?',
  ).bind(projectCode).first<{ count: number }>();
  return row?.count || 0;
}

async function countSubscriptions(env: Env, projectCode: string, userId: string): Promise<number> {
  if (await hideInactiveYearEnabled(env, projectCode, userId)) {
    return (await subscribedChannels(env, projectCode, userId)).length;
  }
  const row = await env.DB.prepare(
    `SELECT COUNT(*) AS count
     FROM user_subscriptions
     WHERE project_code = ? AND user_id = ? AND active = 1`,
  ).bind(projectCode, userId).first<{ count: number }>();
  return row?.count || 0;
}

async function subscriptionStatus(env: Env, projectCode: string, userId: string): Promise<SubscriptionAccess['status']> {
  return (await subscriptionAccess(env, projectCode, userId)).status;
}

async function subscriptionAccess(env: Env, projectCode: string, userId: string): Promise<SubscriptionAccess> {
  const row = await env.DB.prepare(
    `SELECT
       COALESCE(u.is_paid, 0) AS is_paid,
       u.access_expires_at AS access_expires_at,
       COALESCE(u.access_source, '') AS access_source,
       COALESCE(u.boost_count, 0) AS boost_count,
       u.boost_expires_at AS boost_expires_at,
       CASE WHEN a.user_id IS NOT NULL OR (COALESCE(u.is_allowlisted, 0) = 1 AND (u.access_expires_at IS NULL OR u.access_expires_at = '')) THEN 1 ELSE 0 END AS is_free,
       CASE WHEN COALESCE(u.is_allowlisted, 0) = 1 AND u.access_expires_at IS NOT NULL AND u.access_expires_at != '' AND u.access_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now') THEN 1 ELSE 0 END AS is_trial
     FROM users u
     LEFT JOIN allowlist a ON a.project_code = u.project_code AND a.user_id = u.user_id
     WHERE u.project_code = ? AND u.user_id = ?
     LIMIT 1`,
  ).bind(projectCode, userId).first<{ is_paid: number; is_free: number; is_trial: number; access_source: string; boost_count: number; boost_expires_at: string | null; access_expires_at: string | null }>();
  if (
    row?.access_source === 'booster'
    && row.boost_count >= boosterMinBoosts(env)
    && (!row.boost_expires_at || row.boost_expires_at > new Date().toISOString())
  ) {
    return { status: 'booster', expiresAt: row.boost_expires_at || null, boostCount: row.boost_count };
  }
  if (row?.is_free === 1) {
    return { status: 'free', expiresAt: null };
  }
  if (row?.is_trial === 1) {
    return { status: 'trial', expiresAt: row.access_expires_at || null };
  }
  if (row?.is_paid === 1 && (!row.access_expires_at || row.access_expires_at > new Date().toISOString())) {
    return { status: 'paid', expiresAt: row.access_expires_at || null };
  }
  return { status: 'none', expiresAt: null };
}

async function isAdmin(env: Env, projectCode: string, userId: string): Promise<boolean> {
  const row = await env.DB.prepare(
    'SELECT COALESCE(is_admin, 0) AS is_admin FROM users WHERE project_code = ? AND user_id = ? LIMIT 1',
  ).bind(projectCode, userId).first<{ is_admin: number }>();
  return row?.is_admin === 1;
}

async function renderAdminStatsText(env: Env, projectCode: string): Promise<string> {
  const [users, startedUsers, usersWithSubscriptions, access, admins, subscriptions, channels] = await Promise.all([
    env.DB.prepare('SELECT COUNT(*) AS count FROM users WHERE project_code = ?').bind(projectCode).first<{ count: number }>(),
    env.DB.prepare(
      `SELECT COUNT(*) AS count
       FROM users
       WHERE project_code = ?
         AND (NULLIF(TRIM(COALESCE(username, '')), '') IS NOT NULL
          OR NULLIF(TRIM(COALESCE(first_name, '')), '') IS NOT NULL)`,
    ).bind(projectCode).first<{ count: number }>(),
    env.DB.prepare(
      `SELECT COUNT(*) AS count
       FROM (
         SELECT user_id
         FROM user_subscriptions
         WHERE project_code = ? AND active = 1
         GROUP BY user_id
         HAVING COUNT(*) > 0
       )`,
    ).bind(projectCode).first<{ count: number }>(),
    env.DB.prepare(
      `SELECT
         SUM(CASE WHEN COALESCE(u.is_paid, 0) = 1 AND (u.access_expires_at IS NULL OR u.access_expires_at = '' OR u.access_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) THEN 1 ELSE 0 END) AS paid,
         SUM(CASE WHEN a.user_id IS NOT NULL OR (COALESCE(u.is_allowlisted, 0) = 1 AND (u.access_expires_at IS NULL OR u.access_expires_at = '')) THEN 1 ELSE 0 END) AS free,
         SUM(CASE WHEN COALESCE(u.access_source, '') = 'booster' AND COALESCE(u.boost_count, 0) >= ${boosterMinBoosts(env)} AND (u.boost_expires_at IS NULL OR u.boost_expires_at = '' OR u.boost_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) THEN 1 ELSE 0 END) AS booster,
         SUM(CASE WHEN COALESCE(u.is_allowlisted, 0) = 1 AND u.access_expires_at IS NOT NULL AND u.access_expires_at != '' AND u.access_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now') THEN 1 ELSE 0 END) AS trial
       FROM users u
       LEFT JOIN allowlist a ON a.project_code = u.project_code AND a.user_id = u.user_id
       WHERE u.project_code = ?`,
    ).bind(projectCode).first<{ paid: number; free: number; trial: number; booster: number }>(),
    env.DB.prepare('SELECT COUNT(*) AS count FROM users WHERE project_code = ? AND COALESCE(is_admin, 0) = 1').bind(projectCode).first<{ count: number }>(),
    env.DB.prepare('SELECT COUNT(*) AS count FROM user_subscriptions WHERE project_code = ? AND active = 1').bind(projectCode).first<{ count: number }>(),
    env.DB.prepare('SELECT COUNT(*) AS count FROM channels WHERE project_code = ?').bind(projectCode).first<{ count: number }>(),
  ]);
  const userCount = users?.count || 0;
  const freeCount = access?.free || 0;
  const trialCount = access?.trial || 0;
  const paidCount = access?.paid || 0;
  const boosterCount = access?.booster || 0;
  const noneCount = Math.max(0, userCount - freeCount - trialCount - paidCount - boosterCount);
  return [
    'Статистика бота',
    '',
    `Пользователей в базе: ${userCount}`,
    `Запустили бота: ${startedUsers?.count || 0}`,
    `С подписками: ${usersWithSubscriptions?.count || 0}`,
    `Админов: ${admins?.count || 0}`,
    `Free: ${freeCount}`,
    `Free временный: ${trialCount}`,
    `Paid: ${paidCount}`,
    `Booster: ${boosterCount}`,
    `Без доступа: ${noneCount}`,
    `Активных подписок: ${subscriptions?.count || 0}`,
    `Каналов в базе: ${channels?.count || 0}`,
  ].join('\n');
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

async function descendantChannelIds(env: Env, projectCode: string, categoryId: string, userId = ''): Promise<string[]> {
  const channels = await childChannels(env, projectCode, categoryId, userId);
  const categories = await childCategories(env, projectCode, categoryId);
  const nested = await Promise.all(categories.map((category) => descendantChannelIds(env, projectCode, category.category_id, userId)));
  return [...channels.map((channel) => channel.channel_id), ...nested.flat()];
}

async function sendNotifications(
  project: Project,
  recipients: Array<{ user_id: string }>,
  text: string,
  parseMode: string,
  channelId: string,
): Promise<Array<{ userId: string; messageId: number | null }>> {
  const deliveries: Array<{ userId: string; messageId: number | null }> = [];
  for (const recipient of recipients) {
    const result = await telegram(project.bot_token, 'sendMessage', {
      chat_id: recipient.user_id,
      text,
      parse_mode: parseMode,
      disable_web_page_preview: false,
      reply_markup: renderNotificationMenu(channelId, true),
    }) as { ok?: boolean; result?: { message_id?: number } } | null;
    deliveries.push({
      userId: recipient.user_id,
      messageId: result?.result?.message_id ?? null,
    });
  }
  return deliveries;
}

async function sendWeeklySubscriptionReminders(env: Env): Promise<void> {
  await ensureWeeklyReminderColumns(env);
  const now = new Date().toISOString();
  const rows = await env.DB.prepare(
    `SELECT p.code AS project_code, p.bot_token, u.user_id
     FROM users u
     JOIN projects p ON p.code = u.project_code
     WHERE p.active = 1
       AND COALESCE(u.weekly_reminders, 0) = 1
     ORDER BY p.code, u.user_id`,
  ).all<{ project_code: string; bot_token: string; user_id: string }>();

  for (const row of rows.results || []) {
    try {
      await telegram(row.bot_token, 'sendMessage', {
        chat_id: row.user_id,
        text: WEEKLY_REMINDER_TEXT,
        reply_markup: renderWeeklyReminderMessageMenu(),
      });
      await env.DB.prepare(
        'UPDATE users SET last_reminder_at = ?, updated_at = ? WHERE project_code = ? AND user_id = ?',
      ).bind(now, now, row.project_code, row.user_id).run();
    } catch (error) {
      console.error(JSON.stringify({ level: 'warn', source: 'weekly-reminder', project: row.project_code, userId: row.user_id, error: String(error) }));
    }
  }
}

function renderNotificationMenu(channelId: string, subscribed: boolean): object {
  return {
    inline_keyboard: [
      [{ text: subscribed ? '✅ Вы подписаны' : '❌ Вы отписались от канала', callback_data: `unsub:${channelId}` }],
    ],
  };
}

async function requiredChannelForProject(env: Env, projectCode: string): Promise<string> {
  return await env.CACHE.get(requiredChannelKey(projectCode)) || '';
}

async function hasRequiredChannelMembership(project: Project, userId: string, channelRef: string): Promise<boolean> {
  if (!channelRef) {
    return true;
  }
  const result = await telegram(project.bot_token, 'getChatMember', {
    chat_id: channelRef,
    user_id: userId,
  }) as { ok?: boolean; result?: { status?: string } } | null;
  const status = result?.result?.status || '';
  return ['creator', 'administrator', 'member'].includes(status);
}

async function enforceSmallChatLimit(project: Project, chat: TelegramChat): Promise<boolean> {
  if (!chat.type || chat.type === 'private') {
    return true;
  }

  const result = await telegram(project.bot_token, 'getChatMemberCount', {
    chat_id: chat.id,
  }) as { ok?: boolean; result?: number } | null;
  const memberCount = Number(result?.result || 0);
  if (memberCount > 0 && memberCount <= MAX_GROUP_TOTAL_MEMBERS) {
    return true;
  }

  await telegram(project.bot_token, 'sendMessage', {
    chat_id: chat.id,
    text: GROUP_LIMIT_TEXT,
    disable_notification: true,
  });
  await telegram(project.bot_token, 'leaveChat', {
    chat_id: chat.id,
  });
  return false;
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

function requiredChannelKey(projectCode: string): string {
  return `required-channel:${projectCode}`;
}

function botUsernameKey(projectCode: string): string {
  return `bot-username:${projectCode}`;
}

function pendingAdminActionKey(projectCode: string, userId: string): string {
  return `pending-admin:${projectCode}:${userId}`;
}

function normalizeTelegramChannel(value: string): string {
  const text = String(value || '').trim();
  if (!text) {
    return '';
  }
  const match = text.match(/(?:https?:\/\/)?t\.me\/([A-Za-z0-9_]+)/i);
  if (match) {
    return `@${match[1]}`;
  }
  if (text.startsWith('@') || /^-?\d+$/.test(text)) {
    return text;
  }
  if (/^[A-Za-z0-9_]{5,}$/.test(text)) {
    return `@${text}`;
  }
  return text;
}

function telegramChannelUrl(channelRef: string): string {
  const normalized = normalizeTelegramChannel(channelRef);
  if (normalized.startsWith('@')) {
    return `https://t.me/${normalized.slice(1)}`;
  }
  if (/^https?:\/\//i.test(normalized)) {
    return normalized;
  }
  return '';
}

function paymentsRequired(env: Env): boolean {
  return ['1', 'true', 'yes', 'on'].includes(String(env.PAYMENTS_REQUIRED || '').trim().toLowerCase());
}

function usageMonth(date = new Date()): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  return `${year}-${month}`;
}

function usageTableStatement(env: Env): D1PreparedStatement {
  return env.DB.prepare(
    `CREATE TABLE IF NOT EXISTS cloudflare_request_usage (
       month TEXT PRIMARY KEY,
       request_count INTEGER NOT NULL DEFAULT 0,
       updated_at TEXT NOT NULL
     )`,
  );
}

async function ensureWeeklyReminderColumns(env: Env): Promise<void> {
  const key = 'schema:weekly-reminders:v1';
  if (await env.CACHE.get(key)) {
    return;
  }
  for (const statement of [
    'ALTER TABLE users ADD COLUMN weekly_reminders INTEGER NOT NULL DEFAULT 0',
    'ALTER TABLE users ADD COLUMN last_reminder_at TEXT',
  ]) {
    try {
      await env.DB.prepare(statement).run();
    } catch (error) {
      if (!String(error).toLowerCase().includes('duplicate column')) {
        throw error;
      }
    }
  }
  await env.CACHE.put(key, '1');
}

async function recordCloudflareRequest(env: Env): Promise<void> {
  const now = new Date().toISOString();
  await env.DB.batch([
    usageTableStatement(env),
    env.DB.prepare(
      `INSERT INTO cloudflare_request_usage (month, request_count, updated_at)
       VALUES (?, 1, ?)
       ON CONFLICT(month) DO UPDATE SET
         request_count = cloudflare_request_usage.request_count + 1,
         updated_at = excluded.updated_at`,
    ).bind(usageMonth(), now),
  ]);
}

async function ensureAccessSchema(env: Env): Promise<void> {
  const key = 'schema:access-sources:v1';
  if (await env.CACHE.get(key)) {
    return;
  }
  for (const statement of [
    "ALTER TABLE users ADD COLUMN access_source TEXT NOT NULL DEFAULT 'none'",
    'ALTER TABLE users ADD COLUMN payment_method TEXT',
    'ALTER TABLE users ADD COLUMN boost_count INTEGER NOT NULL DEFAULT 0',
    'ALTER TABLE users ADD COLUMN boost_checked_at TEXT',
    'ALTER TABLE users ADD COLUMN boost_expires_at TEXT',
    'ALTER TABLE users ADD COLUMN star_paid_until TEXT',
    'ALTER TABLE users ADD COLUMN hide_inactive_year INTEGER NOT NULL DEFAULT 0',
    'ALTER TABLE channels ADD COLUMN last_video_at TEXT',
  ]) {
    try {
      await env.DB.prepare(statement).run();
    } catch (error) {
      if (!String(error).toLowerCase().includes('duplicate column')) {
        throw error;
      }
    }
  }
  await env.CACHE.put(key, '1');
}

function boosterMinBoosts(env: Env): number {
  const value = Number.parseInt(String(env.BOOSTER_MIN_BOOSTS || ''), 10);
  return Number.isFinite(value) && value > 0 ? value : BOOSTER_MIN_BOOSTS_DEFAULT;
}

function starPriceMonthly(env: Env): number {
  const value = Number.parseInt(String(env.STAR_PRICE_MONTHLY || ''), 10);
  return Number.isFinite(value) && value > 0 ? value : DEFAULT_STAR_PRICE_MONTHLY;
}

function normalizeAccessSource(value: string): string {
  const text = String(value || '').trim().toLowerCase();
  return ['free', 'paid', 'booster', 'none'].includes(text) ? text : 'none';
}

async function hideInactiveYearEnabled(env: Env, projectCode: string, userId: string): Promise<boolean> {
  if (!userId) {
    return false;
  }
  const row = await env.DB.prepare(
    'SELECT COALESCE(hide_inactive_year, 0) AS hide_inactive_year FROM users WHERE project_code = ? AND user_id = ? LIMIT 1',
  ).bind(projectCode, userId).first<{ hide_inactive_year: number }>();
  return row?.hide_inactive_year === 1;
}

async function setHideInactiveYear(env: Env, projectCode: string, userId: string, enabled: boolean): Promise<void> {
  const now = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO users (project_code, user_id, hide_inactive_year, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(project_code, user_id) DO UPDATE SET
       hide_inactive_year = excluded.hide_inactive_year,
       updated_at = excluded.updated_at`,
  ).bind(projectCode, userId, enabled ? 1 : 0, now, now).run();
}

function filterVisibleChannels(channels: Channel[], hideInactiveYear: boolean): Channel[] {
  if (!hideInactiveYear) {
    return channels;
  }
  return channels.filter((channel) => !isInactiveMoreThanYear(channel));
}

function isInactiveMoreThanYear(channel: Channel): boolean {
  const cutoff = Date.now() - 365 * 24 * 60 * 60 * 1000;
  const value = String(channel.last_video_at || '').trim();
  if (!value) {
    return false;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return false;
  }
  return parsed.getTime() < cutoff;
}

async function channelVisibilityStats(env: Env, projectCode: string): Promise<{ total: number; hidden: number; visible: number }> {
  const channels = await allChannels(env, projectCode);
  const hidden = channels.filter(isInactiveMoreThanYear).length;
  return {
    total: channels.length,
    hidden,
    visible: channels.length - hidden,
  };
}

async function hiddenNoticeButton(env: Env, projectCode: string, userId: string): Promise<TelegramButton | null> {
  if (!await hideInactiveYearEnabled(env, projectCode, userId)) {
    return null;
  }
  const visibility = await channelVisibilityStats(env, projectCode);
  if (visibility.hidden <= 0) {
    return null;
  }
  return {
    text: `Скрыты неактивные каналы: ${visibility.hidden}`,
    callback_data: 'settings:root',
  };
}

async function boostChannelUrl(env: Env, projectCode: string): Promise<string> {
  const channel = await requiredChannelForProject(env, projectCode);
  const normalized = normalizeTelegramChannel(channel);
  if (normalized.startsWith('@')) {
    return `https://t.me/boost/${normalized.slice(1)}`;
  }
  return telegramChannelUrl(normalized);
}

async function getCloudflareUsage(env: Env): Promise<{ month: string; limit: number; used: number; remaining: number; updated_at: string }> {
  await usageTableStatement(env).run();
  const month = usageMonth();
  const row = await env.DB.prepare(
    `SELECT request_count, updated_at
     FROM cloudflare_request_usage
     WHERE month = ?`,
  ).bind(month).first<{ request_count: number; updated_at: string }>();
  const used = row?.request_count || 0;
  return {
    month,
    limit: CLOUDFLARE_MONTHLY_REQUEST_LIMIT,
    used,
    remaining: Math.max(0, CLOUDFLARE_MONTHLY_REQUEST_LIMIT - used),
    updated_at: row?.updated_at || '',
  };
}

function json(body: object, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: jsonHeaders,
  });
}
