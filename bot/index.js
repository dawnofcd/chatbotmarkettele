require('dotenv').config();

const crypto = require('crypto');
const http = require('http');
const { Telegraf, Markup } = require('telegraf');
const { createClient } = require('./pg_client');

const botToken = process.env.TELEGRAM_TOKEN;
const databaseUrl = process.env.DATABASE_URL;
const adminSecretKey = String(process.env.ADMIN_SECRET_KEY || '').trim();
const mmobankSecretKey = process.env.MMOBANK_SECRET_KEY || process.env.SEPAY_API_KEY || '';
const mmobankAccountNo = process.env.MMOBANK_ACCOUNT_NO || process.env.SEPAY_ACCOUNT_NO || '';
const mmobankBankCode = process.env.MMOBANK_BANK_CODE || process.env.SEPAY_BANK_CODE || '';
const mmobankAccountName = process.env.MMOBANK_ACCOUNT_NAME || process.env.SEPAY_ACCOUNT_NAME || '';
const mmobankWebhookPath = process.env.MMOBANK_WEBHOOK_PATH || process.env.SEPAY_WEBHOOK_PATH || '/mmobank/webhook';
const webhookPort = Number(process.env.PORT || process.env.WEBHOOK_PORT || 3000);
const adminDashboardPath = (() => {
  const raw = String(process.env.ADMIN_DASHBOARD_PATH || '/admin').trim();
  if (!raw) {
    return '/admin';
  }
  const withLeadingSlash = raw.startsWith('/') ? raw : `/${raw}`;
  if (withLeadingSlash.length > 1 && withLeadingSlash.endsWith('/')) {
    return withLeadingSlash.slice(0, -1);
  }
  return withLeadingSlash;
})();
const adminDashboardKey = String(process.env.ADMIN_DASHBOARD_KEY || adminSecretKey || '').trim();
const adminDashboardSessionTtlSecondsRaw = Number(process.env.ADMIN_DASHBOARD_SESSION_TTL_SECONDS || 43200);
const adminDashboardSessionTtlSeconds = Number.isFinite(adminDashboardSessionTtlSecondsRaw) && adminDashboardSessionTtlSecondsRaw > 0
  ? Math.round(adminDashboardSessionTtlSecondsRaw)
  : 43200;
const adminDashboardSessionCookieName = 'admin_dash_session';
const supportZaloNumber = process.env.SUPPORT_ZALO || '0563228054';
const supportShopName = process.env.SUPPORT_SHOP_NAME || 'Tài Nguyên Hero';
const supportZaloGroup = process.env.SUPPORT_ZALO_GROUP || '';
const supportTelegramContact = process.env.SUPPORT_TELEGRAM || '';
const paymentTimeoutSecondsRaw = Number(process.env.PAYMENT_TIMEOUT_SECONDS || 60);
const paymentTimeoutSeconds = Number.isFinite(paymentTimeoutSecondsRaw) && paymentTimeoutSecondsRaw > 0
  ? Math.round(paymentTimeoutSecondsRaw)
  : 60;
const paymentTimeoutMs = paymentTimeoutSeconds * 1000;
const orderExpirySweepIntervalMsRaw = Number(process.env.ORDER_EXPIRY_SWEEP_INTERVAL_MS || 15000);
const orderExpirySweepIntervalMs = Number.isFinite(orderExpirySweepIntervalMsRaw) && orderExpirySweepIntervalMsRaw >= 5000
  ? Math.round(orderExpirySweepIntervalMsRaw)
  : 15000;
const notifyAllDelayMsRaw = Number(process.env.NOTIFY_ALL_DELAY_MS || 35);
const notifyAllDelayMs = Number.isFinite(notifyAllDelayMsRaw) && notifyAllDelayMsRaw >= 0
  ? Math.round(notifyAllDelayMsRaw)
  : 35;
const adminTelegramIds = new Set(
  (process.env.ADMIN_TELEGRAM_IDS || '')
    .split(',')
    .map((id) => id.trim())
    .filter(Boolean),
);

if (!botToken || !databaseUrl) {
  throw new Error('TELEGRAM_TOKEN and DATABASE_URL must be defined.');
}

const bot = new Telegraf(botToken);
const db = createClient({
  connectionString: databaseUrl,
  ssl: process.env.PGSSL === 'false' ? false : { rejectUnauthorized: false },
});
bot.use(async (ctx, next) => {
  const chatType = ctx.chat?.type;
  if (chatType === 'private') {
    return next();
  }

  const text = String(ctx.message?.text || '').trim();
  if (text.startsWith('/')) {
    await safeReply(ctx, 'Vui lòng dùng bot ở chat riêng (private).');
    return;
  }

  if (ctx.callbackQuery) {
    try {
      await ctx.answerCbQuery('Vui lòng dùng bot ở chat riêng (private).', { show_alert: true });
    } catch (error) {
      // no-op
    }
  }
});

const runtimeAdminIds = new Set(adminTelegramIds);
const pendingAdminInputs = new Map();
const pendingUserInputs = new Map();
const orderPaymentMessageRefs = new Map();
const orderExpiryTimers = new Map();
const adminDashboardSessions = new Map();
let orderExpirySweepTimer = null;
let orderExpirySweepInFlight = false;
const inFlightCallbackUsers = new Set();
const ORDER_MESSAGE_REF_PREFIX = '__MSGREF__';

function saveOrderPaymentMessageRef(orderId, chatId, messageId) {
  const oid = String(orderId || '').trim();
  const cid = Number(chatId);
  const mid = Number(messageId);
  if (!oid || !Number.isInteger(cid) || !Number.isInteger(mid)) {
    return;
  }

  const current = orderPaymentMessageRefs.get(oid) || [];
  const exists = current.some((item) => item.chatId === cid && item.messageId === mid);
  if (!exists) {
    current.push({ chatId: cid, messageId: mid });
    orderPaymentMessageRefs.set(oid, current);
  }
}

function buildOrderMessageRefComment(kind, chatId, messageId) {
  return `${ORDER_MESSAGE_REF_PREFIX}:${String(kind || 'payment')}:${Number(chatId)}:${Number(messageId)}`;
}

function parseOrderMessageRefComment(comment) {
  const raw = String(comment || '').trim();
  const pattern = new RegExp(`^${ORDER_MESSAGE_REF_PREFIX}:([a-zA-Z0-9_-]+):(-?\\d+):(\\d+)$`);
  const match = raw.match(pattern);
  if (!match) {
    return null;
  }

  const chatId = Number(match[2]);
  const messageId = Number(match[3]);
  if (!Number.isInteger(chatId) || !Number.isInteger(messageId)) {
    return null;
  }
  return { kind: match[1], chatId, messageId };
}

async function persistOrderMessageRef(orderId, chatId, messageId, kind = 'payment') {
  const oid = String(orderId || '').trim();
  const cid = Number(chatId);
  const mid = Number(messageId);
  if (!oid || !Number.isInteger(cid) || !Number.isInteger(mid)) {
    return;
  }

  try {
    await db.from('order_history').insert({
      order_id: oid,
      changed_by: null,
      status: 'internal',
      comment: buildOrderMessageRefComment(kind, cid, mid),
    });
  } catch (error) {
    // Do not block purchase flow when telemetry insert fails.
  }
}

async function loadPersistedOrderMessageRefs(orderId) {
  const oid = String(orderId || '').trim();
  if (!oid) {
    return [];
  }

  const { data, error } = await db
    .from('order_history')
    .select('comment')
    .eq('order_id', oid)
    .order('created_at', { ascending: false })
    .limit(200);
  if (error) {
    throw error;
  }

  const refs = [];
  for (const row of (data || [])) {
    const parsed = parseOrderMessageRefComment(row.comment);
    if (parsed) {
      refs.push({ chatId: parsed.chatId, messageId: parsed.messageId });
    }
  }
  return refs;
}

async function rememberOrderPaymentMessageRef(orderId, chatId, messageId, kind = 'payment') {
  saveOrderPaymentMessageRef(orderId, chatId, messageId);
  await persistOrderMessageRef(orderId, chatId, messageId, kind);
}

async function clearOrderPaymentMessages(orderId) {
  const oid = String(orderId || '').trim();
  if (!oid) {
    return;
  }

  const refs = orderPaymentMessageRefs.get(oid) || [];
  let persistedRefs = [];
  try {
    persistedRefs = await loadPersistedOrderMessageRefs(oid);
  } catch (error) {
    // no-op: fallback to in-memory refs only
  }

  const dedup = new Map();
  for (const ref of [...refs, ...persistedRefs]) {
    const chatId = Number(ref?.chatId);
    const messageId = Number(ref?.messageId);
    if (!Number.isInteger(chatId) || !Number.isInteger(messageId)) {
      continue;
    }
    dedup.set(`${chatId}:${messageId}`, { chatId, messageId });
  }

  const finalRefs = [...dedup.values()];
  if (!finalRefs.length) {
    orderPaymentMessageRefs.delete(oid);
    return;
  }
  for (const ref of finalRefs) {
    try {
      await bot.telegram.deleteMessage(ref.chatId, ref.messageId);
    } catch (error) {
      // ignore: message can be already deleted / too old / not found
    }
  }
  orderPaymentMessageRefs.delete(oid);
}

function clearOrderExpiryTimer(orderId) {
  const oid = String(orderId || '').trim();
  if (!oid) {
    return;
  }

  const current = orderExpiryTimers.get(oid);
  if (current) {
    clearTimeout(current);
    orderExpiryTimers.delete(oid);
  }
}

async function expireUnpaidOrder(orderId) {
  const oid = String(orderId || '').trim();
  if (!oid) {
    return;
  }

  const { data: order, error: orderError } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency')
    .eq('id', oid)
    .maybeSingle();
  if (orderError) {
    throw orderError;
  }
  if (!order || !['draft', 'confirmed'].includes(String(order.status || '').toLowerCase())) {
    return;
  }

  const { data: updated, error: updateError } = await db
    .from('orders')
    .update({ status: 'cancelled' })
    .eq('id', oid)
    .in('status', ['draft', 'confirmed'])
    .select('id,user_id,status,total_amount,currency')
    .maybeSingle();
  if (updateError) {
    throw updateError;
  }
  if (!updated) {
    return;
  }

  let releasedAccountProducts = [];
  try {
    const released = await releaseReservedAccountsForOrder(updated.id);
    releasedAccountProducts = released.productIds || [];
  } catch (error) {
    console.error('releaseReservedAccountsForOrder failed (timeout):', error);
  }

  try {
    await restoreStockFromOrderItems(updated.id, { skipProductIds: releasedAccountProducts });
  } catch (error) {
    console.error('restoreStockFromOrderItems failed (timeout):', error);
  }

  try {
    await db.from('order_history').insert({
      order_id: updated.id,
      changed_by: null,
      status: 'cancelled',
      comment: `Auto-cancelled after ${paymentTimeoutSeconds}s without payment`,
    });
  } catch (error) {
    console.error('order_history insert (timeout cancel) failed:', error);
  }

  await clearOrderPaymentMessages(updated.id);

  try {
    const { data: owner } = await db
      .from('users')
      .select('telegram_id')
      .eq('id', updated.user_id)
      .maybeSingle();
    if (owner?.telegram_id) {
      await bot.telegram.sendMessage(
        Number(owner.telegram_id),
        `Đơn #${updated.id} đã tự hủy do quá ${paymentTimeoutSeconds} giây chưa thanh toán.`,
      );
    }
  } catch (error) {
    // no-op
  }
}

function scheduleOrderExpiry(orderId, delayMs = paymentTimeoutMs) {
  const oid = String(orderId || '').trim();
  if (!oid) {
    return;
  }

  const timeoutMs = Number.isFinite(Number(delayMs)) ? Math.max(0, Math.round(Number(delayMs))) : paymentTimeoutMs;
  clearOrderExpiryTimer(oid);
  const timer = setTimeout(async () => {
    orderExpiryTimers.delete(oid);
    try {
      await expireUnpaidOrder(oid);
    } catch (error) {
      console.error('expireUnpaidOrder failed:', error);
    }
  }, timeoutMs);

  if (typeof timer.unref === 'function') {
    timer.unref();
  }
  orderExpiryTimers.set(oid, timer);
}

async function restorePendingOrderExpirySchedules() {
  const { data: pendingOrders, error } = await db
    .from('orders')
    .select('id,status,created_at')
    .in('status', ['draft', 'confirmed'])
    .order('created_at', { ascending: false })
    .limit(5000);
  if (error) {
    throw error;
  }

  const now = Date.now();
  let scheduledCount = 0;
  let expiredNowCount = 0;
  const rows = pendingOrders || [];

  for (const order of rows) {
    const createdAtMs = Date.parse(String(order.created_at || ''));
    if (!Number.isFinite(createdAtMs)) {
      scheduleOrderExpiry(order.id);
      scheduledCount += 1;
      continue;
    }

    const elapsedMs = now - createdAtMs;
    const remainingMs = paymentTimeoutMs - elapsedMs;
    if (remainingMs <= 0) {
      await expireUnpaidOrder(order.id);
      expiredNowCount += 1;
      continue;
    }

    scheduleOrderExpiry(order.id, remainingMs);
    scheduledCount += 1;
  }

  console.log(`Order expiry restored: scheduled=${scheduledCount}, expired_now=${expiredNowCount}`);
}

async function expireOverdueUnpaidOrders(limit = 5000) {
  const maxRows = Number.isFinite(Number(limit)) ? Math.max(1, Math.round(Number(limit))) : 5000;
  const { data: pendingOrders, error } = await db
    .from('orders')
    .select('id,created_at')
    .in('status', ['draft', 'confirmed'])
    .order('created_at', { ascending: true })
    .limit(maxRows);
  if (error) {
    throw error;
  }

  const now = Date.now();
  let expiredCount = 0;
  for (const order of (pendingOrders || [])) {
    const createdAtMs = Date.parse(String(order.created_at || ''));
    if (!Number.isFinite(createdAtMs)) {
      continue;
    }
    if ((now - createdAtMs) < paymentTimeoutMs) {
      break;
    }

    await expireUnpaidOrder(order.id);
    expiredCount += 1;
  }
  return expiredCount;
}

async function runOrderExpirySweep() {
  if (orderExpirySweepInFlight) {
    return;
  }

  orderExpirySweepInFlight = true;
  try {
    const expiredCount = await expireOverdueUnpaidOrders();
    if (expiredCount > 0) {
      console.log(`Order expiry sweep auto-cancelled: ${expiredCount}`);
    }
  } catch (error) {
    console.error('Order expiry sweep failed:', error);
  } finally {
    orderExpirySweepInFlight = false;
  }
}

function startOrderExpirySweeper() {
  if (orderExpirySweepTimer) {
    return;
  }

  orderExpirySweepTimer = setInterval(() => {
    runOrderExpirySweep();
  }, orderExpirySweepIntervalMs);
  if (typeof orderExpirySweepTimer.unref === 'function') {
    orderExpirySweepTimer.unref();
  }
  runOrderExpirySweep();
}

function stopOrderExpirySweeper() {
  if (!orderExpirySweepTimer) {
    return;
  }

  clearInterval(orderExpirySweepTimer);
  orderExpirySweepTimer = null;
}

function parseAccountData(rawAccountData) {
  const raw = String(rawAccountData || '').trim();
  if (!raw) {
    return { account: '(trong)', password: '(trong)', twofa: '(khong co)' };
  }

  const parts = raw.split('|').map((part) => String(part || '').trim());
  return {
    account: parts[0] || '(trong)',
    password: parts[1] || '(trong)',
    twofa: parts[2] || '(khong co)',
  };
}

function buildPaidDeliveryMessage(order, sections, shortageCount) {
  const orderCode = String(order?.id || '').toUpperCase();
  const supportCode = buildSupportOrderCode(order?.id);
  const productName = sections[0]?.productName || '(khong ro)';

  const lines = [
    '━━━━━━━━━━━━━━━━━━━━━━',
    '🧾 THÔNG TIN ĐƠN HÀNG',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
    `🔐 Mã đơn: #${orderCode}`,
    `🧷 Mã hỗ trợ: ${supportCode || '(N/A)'}`,
    `📦 Sản phẩm: ${productName}`,
    '',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '📌 TÀI KHOẢN (định dạng: TK | MK | 2FA)',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
  ];

  for (const section of sections) {
    if (sections.length > 1) {
      lines.push(`• ${section.productName}`);
      lines.push('');
    }

    section.accounts.forEach((accountData, index) => {
      const parsed = parseAccountData(accountData);
      lines.push(`${index + 1}️⃣ ${parsed.account}`);
      lines.push(`🔑 Mật khẩu: ${parsed.password}`);
      lines.push(`🛡 2FA: ${parsed.twofa}`);
      lines.push('');
    });
  }

  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  lines.push('⚠️ LƯU Ý QUAN TRỌNG');
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  lines.push('');
  lines.push('• Đổi mật khẩu ngay sau khi đăng nhập');
  lines.push('• Bật lại bảo mật theo thông tin cá nhân của bạn');
  lines.push('• Không chia sẻ tài khoản cho bên thứ ba');
  if (shortageCount > 0) {
    lines.push('• Còn thiếu một số tài khoản, vui lòng nhắn admin để được cấp bổ sung');
  }
  lines.push('');
  lines.push('Chúc bạn sử dụng dịch vụ thuận lợi ✅');
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  return lines.join('\n');
}

const TEXTS = {
  vi: {
    welcome: 'Xin chào! Tôi là bot bán tài khoản.',
    menuHint: 'Chọn một mục bên dưới:',
    emptyCatalogue: 'Danh mục hiện đang trống.',
    emptyHistory: 'Bạn chưa có đơn hàng nào.',
    supportEmpty: 'Chưa có kênh hỗ trợ.',
    noAdmin: 'Bạn không có quyền admin.',
    invalidKey: 'Secret key không đúng.',
    adminGranted: 'Đã cấp quyền admin.',
    adminPanel: 'Admin panel',
    langCurrent: 'Ngôn ngữ hiện tại: Tiếng Việt',
    orderCreated: 'Đã tạo đơn. Mã: #{id}\nTổng tiền: {total} {currency}',
    outOfStock: 'Sản phẩm đã hết hàng.',
    productMissing: 'Không tìm thấy sản phẩm.',
    orderStatusUpdated: 'Đơn #{id} đã cập nhật -> {status}',
    reportTitle: 'Báo cáo nhanh',
  },
  en: {
    welcome: 'Welcome! I am your account shop bot.',
    menuHint: 'Choose an option below:',
    emptyCatalogue: 'Catalogue is empty right now.',
    emptyHistory: 'You do not have any orders yet.',
    supportEmpty: 'No support channels configured yet.',
    noAdmin: 'You are not an admin.',
    invalidKey: 'Invalid secret key.',
    adminGranted: 'Admin access granted.',
    adminPanel: 'Admin panel',
    langCurrent: 'Current language: English',
    orderCreated: 'Order created. ID: #{id}\nTotal: {total} {currency}',
    outOfStock: 'Product is out of stock.',
    productMissing: 'Product not found.',
    orderStatusUpdated: 'Order #{id} updated -> {status}',
    reportTitle: 'Quick report',
  },
};

const STATUS_LABEL = {
  draft: 'draft',
  confirmed: 'confirmed',
  paid: 'paid',
  cancelled: 'cancelled',
};

function t(locale, key, params = {}) {
  const safeLocale = locale === 'en' ? 'en' : 'vi';
  let text = TEXTS[safeLocale][key] || TEXTS.vi[key] || key;
  for (const [k, v] of Object.entries(params)) {
    text = text.replace(`{${k}}`, String(v));
  }
  return text;
}

function mainMenu(locale, hasAdminAccess = false) {
  const iconCart = '\uD83D\uDED2';
  const iconHistory = '\uD83D\uDCCB';
  const iconSupport = '\uD83D\uDCAC';
  const iconLang = '\uD83C\uDF10';

  if (locale === 'en') {
    const rows = [
      [
        Markup.button.callback(`${iconCart} Shop`, 'menu_catalogue'),
        Markup.button.callback(`${iconHistory} Orders`, 'menu_history'),
      ],
      [
        Markup.button.callback(`${iconSupport} Support`, 'menu_support'),
        Markup.button.callback(`${iconLang} Lang`, 'menu_language'),
      ],
    ];

    if (hasAdminAccess) {
      rows.push([Markup.button.callback('🛠 Admin', 'menu_admin')]);
    }

    return Markup.inlineKeyboard(rows);
  }

  const rows = [
    [
      Markup.button.callback(`${iconCart} Mua hàng`, 'menu_catalogue'),
      Markup.button.callback(`${iconHistory} Đơn của tôi`, 'menu_history'),
    ],
    [
      Markup.button.callback(`${iconSupport} H\u1ed7 tr\u1ee3`, 'menu_support'),
      Markup.button.callback(`${iconLang} Ngôn ngữ`, 'menu_language'),
    ],
  ];

  if (hasAdminAccess) {
    rows.push([Markup.button.callback('🛠 Admin', 'menu_admin')]);
  }

  return Markup.inlineKeyboard(rows);
}

function buildHomeMessage(locale, firstName) {
  const iconAnnounce = '\uD83C\uDF89';
  const envAnnouncement = locale === 'en'
    ? process.env.HOME_ANNOUNCEMENT_EN
    : process.env.HOME_ANNOUNCEMENT_VI;

  if (envAnnouncement) {
    return envAnnouncement;
  }

  if (locale === 'en') {
    return [
      '━━━━━━━━━━━━━━━━━━━━━━',
      `${iconAnnounce} WELCOME TO THE SHOP`,
      '━━━━━━━━━━━━━━━━━━━━━━',
      '',
      `Hi ${firstName || 'there'},`,
      'Pick one option below to continue:',
      '• Browse products',
      '• Track your orders',
      '• Contact support quickly',
    ].join('\n');
  }

  return [
    '━━━━━━━━━━━━━━━━━━━━━━',
    `${iconAnnounce} CHÀO MỪNG BẠN`,
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
    `Xin chào ${firstName || 'bạn'},`,
    'Chọn thao tác bên dưới để bắt đầu:',
    '• Xem sản phẩm đang bán',
    '• Theo dõi đơn đã tạo',
    '• Liên hệ hỗ trợ nhanh',
  ].join('\n');
}

async function sendHomePanel(ctx, userRecord, locale) {
  const firstName = ctx.from.first_name || (locale === 'en' ? 'there' : 'b\u1ea1n');
  await ctx.reply(buildHomeMessage(locale, firstName), mainMenu(locale, isAdmin(ctx, userRecord)));
}
function buildAdminMainKeyboard() {
  return Markup.inlineKeyboard([
    [
      Markup.button.callback('📦 Đơn chờ xử lý', 'admin_orders_new'),
      Markup.button.callback('🛍 Sản phẩm', 'admin_products_v2'),
    ],
    [
      Markup.button.callback('📊 Thống kê', 'admin_reports'),
      Markup.button.callback('🔄 Làm mới', 'admin_home_refresh'),
    ],
    [Markup.button.callback('🗑 Đóng', 'admin_home_close')],
  ]);
}

async function ensureUser(ctx) {
  const telegramId = String(ctx.from.id);
  const roleFromEnv = adminTelegramIds.has(telegramId) ? 'admin' : 'customer';

  const { data: existingUser, error: selectError } = await db
    .from('users')
    .select('*')
    .eq('telegram_id', Number(telegramId))
    .maybeSingle();

  if (selectError) {
    throw selectError;
  }

  if (existingUser) {
    if (roleFromEnv === 'admin' && existingUser.role !== 'admin') {
      const { data: updatedUser, error: updateError } = await db
        .from('users')
        .update({ role: 'admin' })
        .eq('id', existingUser.id)
        .select('*')
        .single();

      if (updateError) {
        throw updateError;
      }

      return updatedUser || existingUser;
    }

    return existingUser;
  }

  const { data: newUser, error: insertError } = await db
    .from('users')
    .insert({
      telegram_id: Number(telegramId),
      username: ctx.from.username || null,
      display_name: `${ctx.from.first_name || ''} ${ctx.from.last_name || ''}`.trim(),
      role: roleFromEnv,
      language_code: 'vi',
    })
    .select('*')
    .single();

  if (insertError) {
    throw insertError;
  }

  return newUser;
}

async function setUserLanguage(userId, languageCode) {
  const { data, error } = await db
    .from('users')
    .update({ language_code: languageCode })
    .eq('id', userId)
    .select('*')
    .single();

  if (error) {
    throw error;
  }

  return data;
}

function getLocale(userRecord) {
  return userRecord?.language_code === 'en' ? 'en' : 'vi';
}

function hasAdminIdentity(ctx, userRecord) {
  const telegramId = String(ctx.from.id);
  return runtimeAdminIds.has(telegramId) || userRecord?.role === 'admin';
}

function canUseNotifyAll(ctx, userRecord) {
  return hasAdminIdentity(ctx, userRecord);
}

function isAdmin(ctx, userRecord) {
  // Chat-admin features are disabled. Only /notifyall keeps separate permission.
  return false;
}

function isSecretKeyValid(input) {
  const normalizedInput = String(input || '').trim();
  if (!adminSecretKey || !normalizedInput) {
    return false;
  }

  const expected = Buffer.from(adminSecretKey);
  const actual = Buffer.from(normalizedInput);
  if (expected.length !== actual.length) {
    return false;
  }

  return crypto.timingSafeEqual(expected, actual);
}

function getCommandPayload(text, command) {
  const pattern = new RegExp(`^/${command}(?:@\\w+)?\\s*`, 'i');
  return (text || '').replace(pattern, '').trim();
}

function sleep(ms) {
  const delay = Math.max(0, Number(ms) || 0);
  return new Promise((resolve) => setTimeout(resolve, delay));
}

function slugifyName(name) {
  return String(name || '')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 50);
}

function normalizeAddProductType(rawType) {
  const normalized = String(rawType || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[\s_-]+/g, '');

  if (!normalized) return null;
  if (normalized === 'key' || normalized === 'code') return 'code';
  if (normalized === 'account' || normalized === 'acc' || normalized === 'auto') return 'account';
  if (normalized === 'support' || normalized === 'hotro' || normalized === 'manual') return 'support';
  return null;
}

function parseAddProductPayload(payload) {
  const parts = payload.split('|').map((p) => p.trim());
  const [name, priceRaw, currencyRaw, typeRaw, ...descriptionParts] = parts;
  const price = Number(priceRaw);
  const currency = (currencyRaw || 'VND').toUpperCase();
  const normalizedType = normalizeAddProductType(typeRaw);
  const description = descriptionParts.join('|').trim();

  if (!name || !Number.isFinite(price) || price < 0 || !normalizedType) {
    return { ok: false };
  }

  return {
    ok: true,
    data: {
      name,
      price,
      currency,
      deliveryType: normalizedType === 'support' ? 'manual' : 'auto',
      productKind: normalizedType,
      description,
    },
  };
}

function parseBulkAccountLines(input) {
  const lines = String(input || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const unique = [];
  const seen = new Set();
  for (const line of lines) {
    if (!seen.has(line)) {
      seen.add(line);
      unique.push(line);
    }
  }
  return unique;
}

async function loadActiveProducts(limit = 50) {
  const { data, error } = await db
    .from('products')
    .select('id,name,description,price,currency,stock_quantity,delivery_type')
    .eq('is_active', true)
    .order('updated_at', { ascending: false })
    .limit(limit);

  if (error) {
    throw error;
  }

  return attachBuyerToOrders(data || []);
}

async function loadProduct(productId) {
  const { data, error } = await db
    .from('products')
    .select('id,name,description,delivery_type,manual_contact_note,price,currency,stock_quantity,is_active')
    .eq('id', productId)
    .eq('is_active', true)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data;
}

async function loadProductAny(productId) {
  const { data, error } = await db
    .from('products')
    .select('id,name,description,delivery_type,manual_contact_note,price,currency,stock_quantity,is_active')
    .eq('id', productId)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data;
}

async function findProductForAdminKeyword(keyword) {
  const normalized = String(keyword || '').trim().toLowerCase();
  if (!normalized) {
    return null;
  }

  const { data, error } = await db
    .from('products')
    .select('id,name,description,price,currency,stock_quantity,is_active,delivery_type')
    .order('updated_at', { ascending: false })
    .limit(200);
  if (error) {
    throw error;
  }

  const rows = data || [];
  const byId = rows.find((p) => String(p.id || '').toLowerCase().startsWith(normalized));
  if (byId) {
    return byId;
  }
  return rows.find((p) => String(p.name || '').toLowerCase().includes(normalized)) || null;
}

async function createProduct(input) {
  const slugBase = slugifyName(input.name) || `product-${Date.now()}`;
  const slug = `${slugBase}-${Math.random().toString(36).slice(2, 7)}`;
  const kindTag = `[type:${String(input.productKind || 'account').toLowerCase()}]`;
  const description = [kindTag, String(input.description || '').trim()].filter(Boolean).join('\n');
  const { data, error } = await db
    .from('products')
    .insert({
      category_id: null,
      name: input.name,
      slug,
      description: description || null,
      delivery_type: input.deliveryType || 'auto',
      manual_contact_note: `Sau khi chuyển khoản thành công, vui lòng liên hệ Zalo ${supportZaloNumber} để được cấp tài khoản.`,
      price: input.price,
      currency: input.currency || 'VND',
      stock_quantity: 0,
      is_active: true,
    })
    .select('id,name,price,currency,stock_quantity')
    .single();

  if (error) {
    throw error;
  }

  return data;
}

async function addProductAccountsBulk(productId, accountLines) {
  const lines = parseBulkAccountLines(accountLines);
  if (lines.length === 0) {
    return { added: 0, skipped: 0, total: 0 };
  }

  const { data: existingRows, error: existingError } = await db
    .from('product_accounts')
    .select('account_data')
    .eq('product_id', productId)
    .limit(5000);

  if (existingError) {
    throw existingError;
  }

  const existingSet = new Set((existingRows || []).map((r) => String(r.account_data || '').trim()));
  const toInsert = lines
    .filter((line) => !existingSet.has(line))
    .map((line) => ({ product_id: productId, account_data: line, is_used: false }));

  if (toInsert.length > 0) {
    const { error: insertError } = await db
      .from('product_accounts')
      .insert(toInsert);
    if (insertError) {
      throw insertError;
    }
  }

  return {
    added: toInsert.length,
    skipped: lines.length - toInsert.length,
    total: lines.length,
  };
}

async function adjustProductStockWithRetry(productId, delta, maxRetries = 6) {
  const targetProductId = String(productId || '').trim();
  const targetDelta = Number(delta || 0);
  if (!targetProductId) {
    throw new Error('product_id_missing');
  }
  if (!Number.isFinite(targetDelta) || targetDelta === 0) {
    return { ok: true, stockQuantity: null };
  }

  const retries = Number.isFinite(Number(maxRetries))
    ? Math.max(1, Math.round(Number(maxRetries)))
    : 6;

  for (let attempt = 0; attempt < retries; attempt += 1) {
    const { data: currentProduct, error: productError } = await db
      .from('products')
      .select('id,stock_quantity')
      .eq('id', targetProductId)
      .maybeSingle();
    if (productError) {
      throw productError;
    }
    if (!currentProduct) {
      return { ok: false, reason: 'product_not_found' };
    }

    const currentStock = Number(currentProduct.stock_quantity || 0);
    const nextStock = currentStock + targetDelta;
    if (nextStock < 0) {
      return { ok: false, reason: 'insufficient_stock', currentStock };
    }

    const { data: updatedProduct, error: updateError } = await db
      .from('products')
      .update({ stock_quantity: nextStock })
      .eq('id', targetProductId)
      .eq('stock_quantity', currentStock)
      .select('id,stock_quantity')
      .maybeSingle();
    if (updateError) {
      throw updateError;
    }
    if (updatedProduct) {
      return {
        ok: true,
        stockQuantity: Number(updatedProduct.stock_quantity || 0),
      };
    }
  }

  return { ok: false, reason: 'stock_update_conflict' };
}

async function syncProductStockFromAutoAccounts(productId) {
  const { count, error: countError } = await db
    .from('product_accounts')
    .select('id', { count: 'exact', head: true })
    .eq('product_id', productId)
    .eq('is_used', false);

  if (countError) {
    throw countError;
  }

  const stock = Number.isInteger(count) ? count : 0;
  const { data, error: updateError } = await db
    .from('products')
    .update({ stock_quantity: stock })
    .eq('id', productId)
    .select('id,name,stock_quantity')
    .maybeSingle();

  if (updateError) {
    throw updateError;
  }

  return data || { id: productId, stock_quantity: stock };
}

async function releaseReservedAccountsForOrder(orderId) {
  const oid = String(orderId || '').trim();
  if (!oid) {
    return { released: 0, productIds: [] };
  }

  const { data: rows, error: rowsError } = await db
    .from('product_accounts')
    .select('id,product_id')
    .eq('used_order_id', oid)
    .limit(5000);
  if (rowsError) {
    throw rowsError;
  }

  const targetRows = rows || [];
  if (!targetRows.length) {
    return { released: 0, productIds: [] };
  }

  const { error: releaseError } = await db
    .from('product_accounts')
    .update({ is_used: false, used_order_id: null, used_at: null })
    .eq('used_order_id', oid);
  if (releaseError) {
    throw releaseError;
  }

  const productIds = [...new Set(targetRows.map((row) => String(row.product_id || '').trim()).filter(Boolean))];
  for (const productId of productIds) {
    try {
      await syncProductStockFromAutoAccounts(productId);
    } catch (error) {
      console.error('syncProductStockFromAutoAccounts failed (release):', error);
    }
  }

  return { released: targetRows.length, productIds };
}

async function claimAutoAccount(productId, orderId) {
  const { data: accountRow, error: selectError } = await db
    .from('product_accounts')
    .select('id,account_data')
    .eq('product_id', productId)
    .eq('is_used', false)
    .order('created_at', { ascending: true })
    .limit(1)
    .maybeSingle();

  if (selectError) {
    throw selectError;
  }

  if (!accountRow) {
    return null;
  }

  const { data: lockedRow, error: lockError } = await db
    .from('product_accounts')
    .update({
      is_used: true,
      used_order_id: orderId,
      used_at: new Date().toISOString(),
    })
    .eq('id', accountRow.id)
    .eq('is_used', false)
    .select('id,account_data')
    .maybeSingle();

  if (lockError) {
    throw lockError;
  }

  return lockedRow || null;
}

async function createSingleItemOrder(userId, product, quantity = 1) {
  const qty = Number(quantity);
  if (!Number.isInteger(qty) || qty <= 0) {
    throw new Error('Số lượng không hợp lệ');
  }
  const unitPrice = calcUnitPriceByQuantity(product.price, qty);
  const total = unitPrice * qty;

  const { data: order, error: orderError } = await db
    .from('orders')
    .insert({
      user_id: userId,
      status: 'confirmed',
      payment_method: 'mmobank',
      total_amount: total,
      currency: product.currency || 'VND',
    })
    .select('id,status,total_amount,currency,payment_method')
    .single();

  if (orderError) {
    throw orderError;
  }

  const accountInventory = usesAccountInventory(product);
  let stockReserved = false;
  try {
    if (accountInventory) {
      const reservedRows = [];
      for (let i = 0; i < qty; i += 1) {
        const claimed = await claimAutoAccount(product.id, order.id);
        if (!claimed) {
          break;
        }
        reservedRows.push(claimed);
      }

      if (reservedRows.length < qty) {
        await releaseReservedAccountsForOrder(order.id);
        throw new Error('out_of_stock');
      }
      await syncProductStockFromAutoAccounts(product.id);
    } else {
      const reserveResult = await adjustProductStockWithRetry(product.id, -qty);
      if (!reserveResult.ok) {
        throw new Error('out_of_stock');
      }
      stockReserved = true;
    }

    const { error: itemError } = await db
      .from('order_items')
      .insert({
        order_id: order.id,
        product_id: product.id,
        unit_price: unitPrice,
        quantity: qty,
        total_price: total,
      });

    if (itemError) {
      throw itemError;
    }

    await db.from('order_history').insert({
      order_id: order.id,
      changed_by: userId,
      status: 'confirmed',
      comment: 'Order created from Telegram bot',
    });

    return order;
  } catch (error) {
    try {
      if (accountInventory) {
        await releaseReservedAccountsForOrder(order.id);
      } else if (stockReserved) {
        await adjustProductStockWithRetry(product.id, qty);
      }
    } catch (rollbackError) {
      console.error('createSingleItemOrder rollback inventory failed:', rollbackError);
    }

    try {
      await db.from('order_items').delete().eq('order_id', order.id);
    } catch (cleanupItemsError) {
      console.error('createSingleItemOrder cleanup order_items failed:', cleanupItemsError);
    }

    try {
      await db.from('orders').delete().eq('id', order.id);
    } catch (cleanupOrderError) {
      console.error('createSingleItemOrder cleanup order failed:', cleanupOrderError);
    }

    throw error;
  }
}

async function notifyAdminsNewOrder(orderId, total, currency) {
  const adminIds = [...runtimeAdminIds].map((id) => Number(id)).filter(Number.isInteger);
  if (adminIds.length === 0) {
    return;
  }

  const message = `Đơn mới #${orderId}\nTổng tiền: ${total} ${currency}\nPhương thức: MMOBank`;
  for (const telegramId of adminIds) {
    try {
      await bot.telegram.sendMessage(telegramId, message);
    } catch (error) {
      // Ignore failed admin notifications to keep order flow responsive.
    }
  }
}

async function restoreStockFromOrderItems(orderId, options = {}) {
  const skipProductIds = new Set(
    [...(options?.skipProductIds || [])]
      .map((id) => String(id || '').trim())
      .filter(Boolean),
  );

  const { data: items, error: itemsError } = await db
    .from('order_items')
    .select('product_id,quantity')
    .eq('order_id', orderId);
  if (itemsError) {
    throw itemsError;
  }

  const grouped = new Map();
  for (const item of items || []) {
    const productId = item.product_id;
    const quantity = Number(item.quantity || 0);
    if (!productId || !Number.isFinite(quantity) || quantity <= 0) {
      continue;
    }
    grouped.set(productId, (grouped.get(productId) || 0) + quantity);
  }

  for (const [productId, quantity] of grouped.entries()) {
    if (skipProductIds.has(String(productId || '').trim())) {
      continue;
    }

    const { data: product, error: productError } = await db
      .from('products')
      .select('id,stock_quantity')
      .eq('id', productId)
      .maybeSingle();
    if (productError) {
      throw productError;
    }
    if (!product) {
      continue;
    }

    const restoreResult = await adjustProductStockWithRetry(productId, quantity);
    if (!restoreResult.ok && restoreResult.reason !== 'product_not_found') {
      throw new Error(`stock_restore_failed:${restoreResult.reason}`);
    }
  }
}

async function cancelOrderByUser(orderId, userId) {
  const { data: order, error: orderError } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency')
    .eq('id', orderId)
    .eq('user_id', userId)
    .maybeSingle();
  if (orderError) {
    throw orderError;
  }
  if (!order) {
    return { ok: false, reason: 'not_found' };
  }
  if (order.status === 'paid') {
    clearOrderExpiryTimer(orderId);
    return { ok: false, reason: 'paid', order };
  }
  if (order.status === 'cancelled') {
    clearOrderExpiryTimer(orderId);
    await clearOrderPaymentMessages(orderId);
    return { ok: true, alreadyCancelled: true, order };
  }

  const { data: updatedOrder, error: updateError } = await db
    .from('orders')
    .update({ status: 'cancelled' })
    .eq('id', orderId)
    .eq('user_id', userId)
    .in('status', ['draft', 'confirmed'])
    .select('id,user_id,status,total_amount,currency')
    .maybeSingle();
  if (updateError) {
    throw updateError;
  }
  if (!updatedOrder) {
    return { ok: false, reason: 'status_changed' };
  }

  let releasedAccountProducts = [];
  try {
    const released = await releaseReservedAccountsForOrder(orderId);
    releasedAccountProducts = released.productIds || [];
  } catch (error) {
    console.error('releaseReservedAccountsForOrder failed:', error);
  }

  try {
    await restoreStockFromOrderItems(orderId, { skipProductIds: releasedAccountProducts });
  } catch (error) {
    console.error('restoreStockFromOrderItems failed:', error);
  }

  try {
    await db.from('order_history').insert({
      order_id: updatedOrder.id,
      changed_by: userId,
      status: 'cancelled',
      comment: 'Cancelled by user from Telegram payment panel',
    });
  } catch (error) {
    console.error('order_history insert (cancelled) failed:', error);
  }

  clearOrderExpiryTimer(orderId);
  await clearOrderPaymentMessages(orderId);

  return { ok: true, alreadyCancelled: false, order: updatedOrder };
}

async function updateOrderStatusFromAdmin(orderId, status, changedByUserId = null, comment = 'Updated from admin panel') {
  const targetStatus = String(status || '').trim().toLowerCase();
  if (!['draft', 'confirmed', 'paid', 'cancelled'].includes(targetStatus)) {
    return { ok: false, reason: 'invalid_status' };
  }

  const { data: currentOrder, error: currentOrderError } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency')
    .eq('id', orderId)
    .maybeSingle();
  if (currentOrderError) {
    throw currentOrderError;
  }
  if (!currentOrder) {
    return { ok: false, reason: 'not_found' };
  }

  const previousStatus = String(currentOrder.status || '').toLowerCase();
  const { data: updated, error: updateError } = await db
    .from('orders')
    .update({ status: targetStatus })
    .eq('id', orderId)
    .select('id,user_id,status,total_amount,currency')
    .single();
  if (updateError) {
    throw updateError;
  }

  await db.from('order_history').insert({
    order_id: updated.id,
    changed_by: changedByUserId,
    status: targetStatus,
    comment,
  });

  if (targetStatus === 'paid') {
    clearOrderExpiryTimer(updated.id);
    await deliverAutoAccountsAfterPaid(updated);
    await clearOrderPaymentMessages(updated.id);
  } else if (targetStatus === 'cancelled') {
    clearOrderExpiryTimer(updated.id);
    if (['draft', 'confirmed'].includes(previousStatus)) {
      let releasedAccountProducts = [];
      try {
        const released = await releaseReservedAccountsForOrder(updated.id);
        releasedAccountProducts = released.productIds || [];
      } catch (releaseError) {
        console.error('releaseReservedAccountsForOrder failed (admin cancel):', releaseError);
      }

      try {
        await restoreStockFromOrderItems(updated.id, { skipProductIds: releasedAccountProducts });
      } catch (restoreError) {
        console.error('restoreStockFromOrderItems failed (admin cancel):', restoreError);
      }
    }
    await clearOrderPaymentMessages(updated.id);
  } else {
    scheduleOrderExpiry(updated.id);
  }

  return { ok: true, order: updated, previousStatus };
}

async function notifyOrderOwnerStatusChanged(order) {
  if (!order?.id || !order?.user_id) {
    return;
  }

  const { data: owner } = await db
    .from('users')
    .select('telegram_id')
    .eq('id', order.user_id)
    .maybeSingle();

  if (owner?.telegram_id) {
    try {
      await bot.telegram.sendMessage(
        Number(owner.telegram_id),
        `Đơn #${order.id} của bạn đã được cập nhật: ${order.status}`,
      );
    } catch (errorNotify) {
      // no-op
    }
  }
}

async function deleteProductWithFallback(productId) {
  const linkedOrderItems = await countOrderItemsByProductId(productId);
  if (linkedOrderItems > 0) {
    await updateAdminProduct(productId, { is_active: false });
    return { mode: 'soft_hidden', linkedOrderItems };
  }

  await hardDeleteProduct(productId);
  return { mode: 'hard_deleted', linkedOrderItems: 0 };
}

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(body);
}

function sendHtml(res, statusCode, html, extraHeaders = {}) {
  res.writeHead(statusCode, {
    'Content-Type': 'text/html; charset=utf-8',
    ...extraHeaders,
  });
  res.end(html);
}

function redirectResponse(res, location, extraHeaders = {}) {
  res.writeHead(302, {
    Location: location,
    ...extraHeaders,
  });
  res.end();
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatDashboardDateTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '-';
  }
  return date.toLocaleString('vi-VN', { hour12: false, timeZone: 'Asia/Ho_Chi_Minh' });
}

function parseCookieHeader(cookieHeader) {
  const cookies = new Map();
  const raw = String(cookieHeader || '').trim();
  if (!raw) {
    return cookies;
  }

  for (const part of raw.split(';')) {
    const [nameRaw, ...valueParts] = part.split('=');
    const name = String(nameRaw || '').trim();
    if (!name) {
      continue;
    }
    cookies.set(name, valueParts.join('=').trim());
  }
  return cookies;
}

function buildAdminDashboardSessionCookie(token) {
  return `${adminDashboardSessionCookieName}=${token}; Path=${adminDashboardPath}; HttpOnly; SameSite=Lax; Max-Age=${adminDashboardSessionTtlSeconds}`;
}

function buildClearAdminDashboardSessionCookie() {
  return `${adminDashboardSessionCookieName}=; Path=${adminDashboardPath}; HttpOnly; SameSite=Lax; Max-Age=0`;
}

function clearExpiredAdminDashboardSessions() {
  const now = Date.now();
  for (const [token, session] of adminDashboardSessions.entries()) {
    if (!session?.expiresAt || session.expiresAt <= now) {
      adminDashboardSessions.delete(token);
    }
  }
}

function getAdminDashboardSession(req) {
  clearExpiredAdminDashboardSessions();
  const cookies = parseCookieHeader(req.headers?.cookie);
  const token = String(cookies.get(adminDashboardSessionCookieName) || '').trim();
  if (!token) {
    return null;
  }

  const session = adminDashboardSessions.get(token);
  if (!session) {
    return null;
  }

  if (session.expiresAt <= Date.now()) {
    adminDashboardSessions.delete(token);
    return null;
  }

  return { token, ...session };
}

function createAdminDashboardSession() {
  clearExpiredAdminDashboardSessions();
  const token = crypto.randomBytes(24).toString('hex');
  const expiresAt = Date.now() + (adminDashboardSessionTtlSeconds * 1000);
  adminDashboardSessions.set(token, { expiresAt });
  return token;
}

function revokeAdminDashboardSession(req) {
  const session = getAdminDashboardSession(req);
  if (session?.token) {
    adminDashboardSessions.delete(session.token);
  }
}

async function readRawRequestBody(req, maxBytes = 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let rawBody = '';
    req.on('data', (chunk) => {
      rawBody += chunk;
      if (rawBody.length > maxBytes) {
        reject(new Error('payload_too_large'));
        req.destroy();
      }
    });

    req.on('end', () => {
      resolve(rawBody);
    });

    req.on('error', (error) => {
      reject(error);
    });
  });
}

function parseFormUrlEncoded(rawBody) {
  const params = new URLSearchParams(String(rawBody || ''));
  const data = {};
  for (const [key, value] of params.entries()) {
    data[key] = value;
  }
  return data;
}

function normalizeDashboardTab(rawTab) {
  const tab = String(rawTab || '').trim().toLowerCase();
  if (tab === 'products' || tab === 'accounts' || tab === 'reports') {
    return tab;
  }
  return 'orders';
}

function normalizeDashboardOrderStatus(rawStatus) {
  const status = String(rawStatus || '').trim().toLowerCase();
  if (['draft', 'confirmed', 'paid', 'cancelled'].includes(status)) {
    return status;
  }
  return 'all';
}

function normalizeDashboardProductScope(rawScope) {
  const scope = String(rawScope || '').trim().toLowerCase();
  if (scope === 'all') {
    return 'all';
  }
  return 'active';
}

function normalizeDashboardAccountScope(rawScope) {
  const scope = String(rawScope || '').trim().toLowerCase();
  if (scope === 'all') {
    return 'all';
  }
  if (scope === 'used') {
    return 'used';
  }
  return 'use';
}

function normalizeDashboardDateInput(rawDate) {
  const value = String(rawDate || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return '';
  }

  const date = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) {
    return '';
  }

  return value;
}

function normalizeDashboardReportFilters(rawFilters = {}) {
  const keywordRaw = String(rawFilters.productKeyword || '').trim();
  const productKeyword = keywordRaw.slice(0, 120);
  let dateFrom = normalizeDashboardDateInput(rawFilters.dateFrom);
  let dateTo = normalizeDashboardDateInput(rawFilters.dateTo);

  if (dateFrom && dateTo && dateFrom > dateTo) {
    const tmp = dateFrom;
    dateFrom = dateTo;
    dateTo = tmp;
  }

  return {
    productKeyword,
    dateFrom,
    dateTo,
  };
}

function buildAdminDashboardUrl(params = {}) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value == null) {
      continue;
    }
    const stringValue = String(value).trim();
    if (!stringValue) {
      continue;
    }
    search.set(key, stringValue);
  }
  const query = search.toString();
  return query ? `${adminDashboardPath}?${query}` : adminDashboardPath;
}

async function loadDashboardOrders(statusFilter = 'all', limit = 80) {
  const safeStatus = normalizeDashboardOrderStatus(statusFilter);
  const maxRows = Number.isFinite(Number(limit)) ? Math.max(1, Math.round(Number(limit))) : 80;
  const query = db
    .from('orders')
    .select('id,user_id,status,total_amount,currency,payment_method,created_at')
    .order('created_at', { ascending: false })
    .limit(maxRows);
  if (safeStatus !== 'all') {
    query.eq('status', safeStatus);
  }

  const { data, error } = await query;
  if (error) {
    throw error;
  }

  const rows = await attachBuyerToOrders(data || []);
  if (!rows.length) {
    return rows;
  }

  const previewByOrderId = await loadOrderItemPreviewByOrderIds(rows.map((row) => row.id));
  return rows.map((row) => ({
    ...row,
    item_preview: previewByOrderId.get(row.id) || null,
  }));
}

async function loadDashboardProducts(limit = 120, scope = 'active') {
  const maxRows = Number.isFinite(Number(limit)) ? Math.max(1, Math.round(Number(limit))) : 120;
  const safeScope = normalizeDashboardProductScope(scope);
  const query = db
    .from('products')
    .select('id,name,price,currency,stock_quantity,is_active,delivery_type,updated_at')
    .order('updated_at', { ascending: false })
    .limit(maxRows);
  if (safeScope === 'active') {
    query.eq('is_active', true);
  }

  const { data, error } = await query;

  if (error) {
    throw error;
  }

  return data || [];
}

function parseDashboardNumber(value) {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeDashboardCurrency(value) {
  const currency = String(value || 'VND').trim().toUpperCase();
  return currency || 'VND';
}

function createEmptyDashboardReports() {
  return {
    snapshot: {
      today: [],
      month: [],
      year: [],
    },
    byProduct: [],
    byProductDay: [],
    byProductMonth: [],
    byProductYear: [],
    byDay: [],
    byMonth: [],
    byYear: [],
  };
}

function normalizeDashboardReports(reports) {
  const empty = createEmptyDashboardReports();
  if (!reports || typeof reports !== 'object') {
    return empty;
  }

  const snapshot = reports.snapshot && typeof reports.snapshot === 'object'
    ? reports.snapshot
    : {};

  return {
    snapshot: {
      today: Array.isArray(snapshot.today) ? snapshot.today : [],
      month: Array.isArray(snapshot.month) ? snapshot.month : [],
      year: Array.isArray(snapshot.year) ? snapshot.year : [],
    },
    byProduct: Array.isArray(reports.byProduct) ? reports.byProduct : [],
    byProductDay: Array.isArray(reports.byProductDay) ? reports.byProductDay : [],
    byProductMonth: Array.isArray(reports.byProductMonth) ? reports.byProductMonth : [],
    byProductYear: Array.isArray(reports.byProductYear) ? reports.byProductYear : [],
    byDay: Array.isArray(reports.byDay) ? reports.byDay : [],
    byMonth: Array.isArray(reports.byMonth) ? reports.byMonth : [],
    byYear: Array.isArray(reports.byYear) ? reports.byYear : [],
  };
}

function formatDashboardRevenueByCurrency(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) {
    return '0 VND';
  }

  return list
    .map((row) => `${formatPriceVnd(row.totalRevenue)} ${normalizeDashboardCurrency(row.currency)}`)
    .join(' | ');
}

function formatDashboardPeriodLabel(periodKey, periodType) {
  const raw = String(periodKey || '').trim();
  if (!raw) {
    return '-';
  }

  if (periodType === 'day') {
    const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (match) {
      return `${match[3]}/${match[2]}/${match[1]}`;
    }
  }

  if (periodType === 'month') {
    const match = raw.match(/^(\d{4})-(\d{2})$/);
    if (match) {
      return `${match[2]}/${match[1]}`;
    }
  }

  return raw;
}

function buildDashboardOrderDateFilter(filters, orderAlias = 'o') {
  const clauses = [];
  const params = [];
  const safeFilters = normalizeDashboardReportFilters(filters);

  if (safeFilters.dateFrom) {
    params.push(safeFilters.dateFrom);
    clauses.push(`timezone('Asia/Ho_Chi_Minh', ${orderAlias}.created_at)::date >= $${params.length}::date`);
  }
  if (safeFilters.dateTo) {
    params.push(safeFilters.dateTo);
    clauses.push(`timezone('Asia/Ho_Chi_Minh', ${orderAlias}.created_at)::date <= $${params.length}::date`);
  }

  return {
    whereSql: clauses.length ? ` where ${clauses.join(' and ')}` : '',
    params,
  };
}

function buildDashboardSoldProductFilter(filters, orderAlias = 'o', productAlias = 'p') {
  const clauses = [`${orderAlias}.status = 'paid'`];
  const params = [];
  const safeFilters = normalizeDashboardReportFilters(filters);

  if (safeFilters.dateFrom) {
    params.push(safeFilters.dateFrom);
    clauses.push(`timezone('Asia/Ho_Chi_Minh', ${orderAlias}.created_at)::date >= $${params.length}::date`);
  }
  if (safeFilters.dateTo) {
    params.push(safeFilters.dateTo);
    clauses.push(`timezone('Asia/Ho_Chi_Minh', ${orderAlias}.created_at)::date <= $${params.length}::date`);
  }
  if (safeFilters.productKeyword) {
    params.push(`%${safeFilters.productKeyword}%`);
    clauses.push(`(${productAlias}.name ilike $${params.length} or coalesce(${productAlias}.description, '') ilike $${params.length})`);
  }

  return {
    whereSql: clauses.length ? ` where ${clauses.join(' and ')}` : '',
    params,
  };
}

async function loadDashboardReports(filters = {}) {
  const safeFilters = normalizeDashboardReportFilters(filters);
  const orderDateFilter = buildDashboardOrderDateFilter(safeFilters, 'o');
  const soldFilter = buildDashboardSoldProductFilter(safeFilters, 'o', 'p');

  const snapshotSql = `
    select
      coalesce(o.currency, 'VND') as currency,
      coalesce(sum(case
        when timezone('Asia/Ho_Chi_Minh', o.created_at)::date = timezone('Asia/Ho_Chi_Minh', now())::date
          then o.total_amount
        else 0
      end), 0) as revenue_today,
      coalesce(sum(case
        when date_trunc('month', timezone('Asia/Ho_Chi_Minh', o.created_at)) = date_trunc('month', timezone('Asia/Ho_Chi_Minh', now()))
          then o.total_amount
        else 0
      end), 0) as revenue_month,
      coalesce(sum(case
        when date_trunc('year', timezone('Asia/Ho_Chi_Minh', o.created_at)) = date_trunc('year', timezone('Asia/Ho_Chi_Minh', now()))
          then o.total_amount
        else 0
      end), 0) as revenue_year
    from orders o
    ${orderDateFilter.whereSql}
    group by coalesce(o.currency, 'VND')
    order by coalesce(o.currency, 'VND') asc
  `;
  const byProductSql = `
    select
      oi.product_id,
      coalesce(p.name, oi.product_id::text) as product_name,
      coalesce(o.currency, 'VND') as currency,
      coalesce(sum(oi.quantity), 0)::bigint as total_quantity,
      coalesce(sum(oi.total_price), 0) as total_revenue,
      count(distinct oi.order_id)::bigint as total_orders
    from order_items oi
    join orders o on o.id = oi.order_id
    left join products p on p.id = oi.product_id
    ${soldFilter.whereSql}
    group by oi.product_id, p.name, coalesce(o.currency, 'VND')
    order by coalesce(sum(oi.total_price), 0) desc, coalesce(sum(oi.quantity), 0) desc, coalesce(p.name, oi.product_id::text) asc
  `;
  const byDaySql = `
    select
      to_char(timezone('Asia/Ho_Chi_Minh', o.created_at), 'YYYY-MM-DD') as period_key,
      coalesce(o.currency, 'VND') as currency,
      count(*)::bigint as order_count,
      coalesce(sum(o.total_amount), 0) as total_revenue
    from orders o
    ${orderDateFilter.whereSql}
    group by period_key, coalesce(o.currency, 'VND')
    order by period_key desc, coalesce(o.currency, 'VND') asc
  `;
  const byMonthSql = `
    select
      to_char(timezone('Asia/Ho_Chi_Minh', o.created_at), 'YYYY-MM') as period_key,
      coalesce(o.currency, 'VND') as currency,
      count(*)::bigint as order_count,
      coalesce(sum(o.total_amount), 0) as total_revenue
    from orders o
    ${orderDateFilter.whereSql}
    group by period_key, coalesce(o.currency, 'VND')
    order by period_key desc, coalesce(o.currency, 'VND') asc
  `;
  const byYearSql = `
    select
      to_char(timezone('Asia/Ho_Chi_Minh', o.created_at), 'YYYY') as period_key,
      coalesce(o.currency, 'VND') as currency,
      count(*)::bigint as order_count,
      coalesce(sum(o.total_amount), 0) as total_revenue
    from orders o
    ${orderDateFilter.whereSql}
    group by period_key, coalesce(o.currency, 'VND')
    order by period_key desc, coalesce(o.currency, 'VND') asc
  `;
  const buildByProductQuantitySql = (periodFormat) => `
    select
      to_char(timezone('Asia/Ho_Chi_Minh', o.created_at), '${periodFormat}') as period_key,
      oi.product_id,
      coalesce(p.name, oi.product_id::text) as product_name,
      coalesce(sum(oi.quantity), 0)::bigint as total_quantity,
      count(distinct oi.order_id)::bigint as total_orders
    from order_items oi
    join orders o on o.id = oi.order_id
    left join products p on p.id = oi.product_id
    ${soldFilter.whereSql}
    group by period_key, oi.product_id, p.name
    order by period_key desc, coalesce(sum(oi.quantity), 0) desc, coalesce(p.name, oi.product_id::text) asc
  `;
  const byProductDaySql = buildByProductQuantitySql('YYYY-MM-DD');
  const byProductMonthSql = buildByProductQuantitySql('YYYY-MM');
  const byProductYearSql = buildByProductQuantitySql('YYYY');

  const [
    snapshotResp,
    byProductResp,
    byDayResp,
    byMonthResp,
    byYearResp,
    byProductDayResp,
    byProductMonthResp,
    byProductYearResp,
  ] = await Promise.all([
    db.query(snapshotSql, orderDateFilter.params),
    db.query(byProductSql, soldFilter.params),
    db.query(byDaySql, orderDateFilter.params),
    db.query(byMonthSql, orderDateFilter.params),
    db.query(byYearSql, orderDateFilter.params),
    db.query(byProductDaySql, soldFilter.params),
    db.query(byProductMonthSql, soldFilter.params),
    db.query(byProductYearSql, soldFilter.params),
  ]);

  if (snapshotResp.error) throw snapshotResp.error;
  if (byProductResp.error) throw byProductResp.error;
  if (byDayResp.error) throw byDayResp.error;
  if (byMonthResp.error) throw byMonthResp.error;
  if (byYearResp.error) throw byYearResp.error;
  if (byProductDayResp.error) throw byProductDayResp.error;
  if (byProductMonthResp.error) throw byProductMonthResp.error;
  if (byProductYearResp.error) throw byProductYearResp.error;

  const snapshot = {
    today: [],
    month: [],
    year: [],
  };
  for (const row of (snapshotResp.data || [])) {
    const currency = normalizeDashboardCurrency(row.currency);
    const revenueToday = parseDashboardNumber(row.revenue_today);
    const revenueMonth = parseDashboardNumber(row.revenue_month);
    const revenueYear = parseDashboardNumber(row.revenue_year);

    if (revenueToday !== 0) {
      snapshot.today.push({ currency, totalRevenue: revenueToday });
    }
    if (revenueMonth !== 0) {
      snapshot.month.push({ currency, totalRevenue: revenueMonth });
    }
    if (revenueYear !== 0) {
      snapshot.year.push({ currency, totalRevenue: revenueYear });
    }
  }

  const byProduct = (byProductResp.data || []).map((row) => ({
    productId: String(row.product_id || '').trim(),
    productName: String(row.product_name || row.product_id || '').trim(),
    currency: normalizeDashboardCurrency(row.currency),
    totalQuantity: Math.max(0, Math.round(parseDashboardNumber(row.total_quantity))),
    totalRevenue: parseDashboardNumber(row.total_revenue),
    totalOrders: Math.max(0, Math.round(parseDashboardNumber(row.total_orders))),
  }));

  const mapPeriodRows = (rows) => (rows || []).map((row) => ({
    periodKey: String(row.period_key || '').trim(),
    currency: normalizeDashboardCurrency(row.currency),
    orderCount: Math.max(0, Math.round(parseDashboardNumber(row.order_count))),
    totalRevenue: parseDashboardNumber(row.total_revenue),
  }));
  const mapProductPeriodRows = (rows) => (rows || []).map((row) => ({
    periodKey: String(row.period_key || '').trim(),
    productId: String(row.product_id || '').trim(),
    productName: String(row.product_name || row.product_id || '').trim(),
    totalQuantity: Math.max(0, Math.round(parseDashboardNumber(row.total_quantity))),
    totalOrders: Math.max(0, Math.round(parseDashboardNumber(row.total_orders))),
  }));

  return {
    snapshot,
    byProduct,
    byProductDay: mapProductPeriodRows(byProductDayResp.data),
    byProductMonth: mapProductPeriodRows(byProductMonthResp.data),
    byProductYear: mapProductPeriodRows(byProductYearResp.data),
    byDay: mapPeriodRows(byDayResp.data),
    byMonth: mapPeriodRows(byMonthResp.data),
    byYear: mapPeriodRows(byYearResp.data),
  };
}

function renderAdminDashboardLoginPage(errorText = '') {
  const errorBlock = errorText
    ? `<div class="alert alert-error">${escapeHtml(errorText)}</div>`
    : '';

  return [
    '<!doctype html>',
    '<html lang="vi">',
    '<head>',
    '  <meta charset="utf-8" />',
    '  <meta name="viewport" content="width=device-width, initial-scale=1" />',
    '  <title>Admin Dashboard Login</title>',
    '  <link rel="preconnect" href="https://fonts.googleapis.com" />',
    '  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />',
    '  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Space+Grotesk:wght@500;700&display=swap" rel="stylesheet" />',
    '  <style>',
    '    :root { --bg: #f3f7ff; --ink: #12263a; --muted: #5f7389; --line: #d4deea; --panel: #ffffff; --brand: #0f8a7f; --danger-bg: #ffe3e0; --danger-ink: #b42318; }',
    '    * { box-sizing: border-box; }',
    '    body { margin: 0; min-height: 100vh; display: grid; place-items: center; padding: 24px; font-family: "IBM Plex Sans", sans-serif; color: var(--ink); background: radial-gradient(90rem 40rem at -15% -15%, #c5e6ff 0%, transparent 55%), radial-gradient(80rem 35rem at 115% -20%, #d8fff3 0%, transparent 52%), var(--bg); }',
    '    .card { width: min(460px, 100%); border: 1px solid var(--line); border-radius: 22px; padding: 28px; background: linear-gradient(180deg, #ffffff 0%, #fbfdff 100%); box-shadow: 0 20px 45px rgba(17, 33, 51, 0.12); }',
    '    .eyebrow { display: inline-flex; align-items: center; gap: 8px; padding: 6px 10px; border-radius: 999px; background: #eaf8f6; color: #0b6f67; font-size: 12px; font-weight: 600; letter-spacing: 0.03em; text-transform: uppercase; margin-bottom: 12px; }',
    '    h1 { margin: 0 0 8px; font-family: "Space Grotesk", sans-serif; font-size: 32px; line-height: 1.1; }',
    '    p { margin: 0 0 18px; color: var(--muted); font-size: 15px; }',
    '    input { width: 100%; border: 1px solid var(--line); border-radius: 12px; padding: 12px 14px; font-size: 15px; color: var(--ink); margin-bottom: 12px; background: #fff; }',
    '    input:focus { outline: 2px solid #9dd7d2; outline-offset: 1px; border-color: #87cfc9; }',
    '    button { width: 100%; border: none; border-radius: 12px; background: var(--brand); color: #fff; font-weight: 600; padding: 12px; font-size: 15px; cursor: pointer; transition: transform .08s ease, filter .15s ease; }',
    '    button:hover { filter: brightness(0.95); }',
    '    button:active { transform: translateY(1px); }',
    '    .alert { border-radius: 12px; padding: 10px 12px; margin-bottom: 12px; font-size: 14px; }',
    '    .alert-error { background: var(--danger-bg); color: var(--danger-ink); border: 1px solid #f8c8c2; }',
  '  </style>',
    '</head>',
    '<body>',
    '  <main class="card">',
    '    <div class="eyebrow">Secure Access</div>',
    '    <h1>Admin Dashboard</h1>',
    '    <p>Nhập khóa quản trị để truy cập khu vực điều hành.</p>',
    `    ${errorBlock}`,
    `    <form method="post" action="${escapeHtml(`${adminDashboardPath}/login`)}">`,
    '      <input name="key" type="password" placeholder="ADMIN_DASHBOARD_KEY" autocomplete="off" required />',
    '      <button type="submit">Đăng nhập</button>',
    '    </form>',
    '  </main>',
    '</body>',
    '</html>',
  ].join('\n');
}

function renderAdminDashboardPage({
  tab = 'orders',
  statusFilter = 'all',
  productScope = 'active',
  accountScope = 'use',
  reportFilters = {},
  infoMessage = '',
  errorMessage = '',
  orders = [],
  products = [],
  selectedProductId = '',
  selectedProduct = null,
  accountSummary = null,
  reports = createEmptyDashboardReports(),
}) {
  const safeTab = normalizeDashboardTab(tab);
  const safeStatusFilter = normalizeDashboardOrderStatus(statusFilter);
  const safeProductScope = normalizeDashboardProductScope(productScope);
  const safeAccountScope = normalizeDashboardAccountScope(accountScope);
  const safeReportFilters = normalizeDashboardReportFilters(reportFilters);
  const normalizedReports = normalizeDashboardReports(reports);
  const orderStatusOptions = ['all', 'draft', 'confirmed', 'paid', 'cancelled'];
  const statusMeta = {
    all: { label: 'Tất cả', className: 'neutral' },
    draft: { label: 'Nháp', className: 'draft' },
    confirmed: { label: 'Chờ thanh toán', className: 'confirmed' },
    paid: { label: 'Đã thanh toán', className: 'paid' },
    cancelled: { label: 'Đã hủy', className: 'cancelled' },
  };
  const orderActionMeta = {
    confirmed: { label: 'Xác nhận', className: 'btn-confirm' },
    paid: { label: 'Đã thanh toán', className: 'btn-paid' },
    cancelled: { label: 'Hủy', className: 'btn-cancel' },
  };

  const infoBlock = infoMessage ? `<div class="alert alert-info">${escapeHtml(infoMessage)}</div>` : '';
  const errorBlock = errorMessage ? `<div class="alert alert-error">${escapeHtml(errorMessage)}</div>` : '';

  const navOrdersClass = safeTab === 'orders' ? 'tab active' : 'tab';
  const navProductsClass = safeTab === 'products' ? 'tab active' : 'tab';
  const navAccountsClass = safeTab === 'accounts' ? 'tab active' : 'tab';
  const navReportsClass = safeTab === 'reports' ? 'tab active' : 'tab';
  const accountProducts = products.filter((product) => usesAccountInventory(product));
  const effectiveSelectedProductId = String(
    selectedProductId
    || selectedProduct?.id
    || accountProducts[0]?.id
    || '',
  ).trim();

  const nav = [
    `<a class="${navOrdersClass}" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'orders', status_filter: safeStatusFilter, products_scope: safeProductScope }))}">Đơn hàng</a>`,
    `<a class="${navProductsClass}" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'products', products_scope: safeProductScope }))}">Sản phẩm</a>`,
    `<a class="${navAccountsClass}" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'accounts', product_id: effectiveSelectedProductId, products_scope: safeProductScope, account_scope: safeAccountScope }))}">Kho account</a>`,
    `<a class="${navReportsClass}" href="${escapeHtml(buildAdminDashboardUrl({
      tab: 'reports',
      products_scope: safeProductScope,
      product_keyword: safeReportFilters.productKeyword,
      date_from: safeReportFilters.dateFrom,
      date_to: safeReportFilters.dateTo,
    }))}">Thống kê</a>`,
  ].join('');

  const orderStats = {
    total: orders.length,
    draft: 0,
    confirmed: 0,
    paid: 0,
    cancelled: 0,
  };
  for (const order of orders) {
    const currentStatus = String(order.status || '').toLowerCase();
    if (Object.prototype.hasOwnProperty.call(orderStats, currentStatus)) {
      orderStats[currentStatus] += 1;
    }
  }
  const activeProducts = products.filter((product) => Boolean(product.is_active)).length;
  const lowStockProducts = products.filter((product) => Number(product.stock_quantity || 0) <= 3).length;
  const statCards = [
    { title: 'Đơn hiển thị', value: orderStats.total, tone: 'blue' },
    { title: 'Đơn chờ thanh toán', value: orderStats.draft + orderStats.confirmed, tone: 'amber' },
    { title: 'Đơn đã thanh toán', value: orderStats.paid, tone: 'green' },
    { title: 'SP đang bật', value: activeProducts, tone: 'teal' },
    { title: 'SP sắp hết', value: lowStockProducts, tone: 'red' },
  ];
  if (selectedProduct && accountSummary) {
    statCards.push(
      { title: 'Account available', value: Number(accountSummary.available || 0), tone: 'purple' },
      { title: 'Account used', value: Number(accountSummary.used || 0), tone: 'gray' },
    );
  }
  const statCardsHtml = statCards.map((card) => [
    `<article class="stat-card tone-${escapeHtml(card.tone)}">`,
    `  <h3>${escapeHtml(card.title)}</h3>`,
    `  <p>${escapeHtml(String(card.value))}</p>`,
    '</article>',
  ].join('\n')).join('\n');

  const statusFilterOptions = orderStatusOptions.map((status) => {
    const selectedAttr = safeStatusFilter === status ? 'selected' : '';
    const label = statusMeta[status]?.label || status.toUpperCase();
    return `<option value="${status}" ${selectedAttr}>${escapeHtml(label)}</option>`;
  }).join('');

  const quickFilterLinks = orderStatusOptions.map((status) => {
    const active = safeStatusFilter === status ? 'chip active' : 'chip';
    return `<a class="${active}" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'orders', status_filter: status, products_scope: safeProductScope }))}">${escapeHtml(statusMeta[status]?.label || status)}</a>`;
  }).join('');
  const productScopeLinks = [
    `<a class="${safeProductScope === 'active' ? 'chip active' : 'chip'}" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'products', products_scope: 'active' }))}">Đang bật</a>`,
    `<a class="${safeProductScope === 'all' ? 'chip active' : 'chip'}" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'products', products_scope: 'all' }))}">Tất cả</a>`,
  ].join('');

  const orderRows = orders.map((order) => {
    const buyer = formatBuyerLabel(order);
    const currentStatus = String(order.status || '').toLowerCase();
    const status = statusMeta[currentStatus] || { label: currentStatus || 'N/A', className: 'neutral' };
    const itemPreview = order.item_preview
      ? `${order.item_preview.firstProductName || '(N/A)'} x${order.item_preview.totalQuantity || 0}`
      : '(chưa có dòng sản phẩm)';
    const actionStatuses = ['confirmed', 'paid', 'cancelled'].filter((statusKey) => statusKey !== currentStatus);
    const actionForms = actionStatuses.map((actionStatus) => {
      const meta = orderActionMeta[actionStatus] || { label: actionStatus, className: 'btn-neutral' };
      return [
        `<form method="post" action="${escapeHtml(`${adminDashboardPath}/order-status`)}">`,
        `  <input type="hidden" name="order_id" value="${escapeHtml(order.id)}" />`,
        `  <input type="hidden" name="status" value="${escapeHtml(actionStatus)}" />`,
        `  <input type="hidden" name="status_filter" value="${escapeHtml(safeStatusFilter)}" />`,
        `  <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
        `  <button class="btn-small btn-action ${escapeHtml(meta.className)}" type="submit">${escapeHtml(meta.label)}</button>`,
        '</form>',
      ].join('\n');
    }).join('\n');

    const shortId = String(order.id || '').slice(0, 8);
    return [
      '<tr>',
      '  <td>',
      `    <div class="id-stack"><strong>#${escapeHtml(shortId)}</strong><code>${escapeHtml(order.id)}</code></div>`,
      '  </td>',
      `  <td>${escapeHtml(buyer)}</td>`,
      `  <td>${escapeHtml(itemPreview)}</td>`,
      `  <td><span class="status-badge ${escapeHtml(status.className)}">${escapeHtml(status.label)}</span></td>`,
      `  <td>${escapeHtml(formatPriceVnd(order.total_amount))} ${escapeHtml(order.currency || 'VND')}</td>`,
      `  <td>${escapeHtml(formatDashboardDateTime(order.created_at))}</td>`,
      `  <td class="actions">${actionForms || '-'}</td>`,
      '</tr>',
    ].join('\n');
  }).join('\n');

  const orderPanel = [
    '<section class="panel">',
    '  <div class="panel-head">',
    '    <div>',
    '      <h2>Đơn hàng</h2>',
    `      <p class="panel-subtitle">Theo dõi trạng thái thanh toán và xử lý nhanh theo từng đơn.</p>`,
    '    </div>',
    `    <form method="get" action="${escapeHtml(adminDashboardPath)}" class="inline-form">`,
    '      <input type="hidden" name="tab" value="orders" />',
    `      <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
    `      <label>Lọc trạng thái <select name="status_filter">${statusFilterOptions}</select></label>`,
    '      <button type="submit">Lọc</button>',
    '    </form>',
    '  </div>',
    `  <div class="chips">${quickFilterLinks}</div>`,
    '  <div class="table-wrap">',
    '    <table>',
      '      <thead><tr><th>ID</th><th>Người mua</th><th>Sản phẩm</th><th>Trạng thái</th><th>Tổng</th><th>Tạo lúc</th><th>Hành động</th></tr></thead>',
    `      <tbody>${orderRows || '<tr><td colspan="7">Không có dữ liệu.</td></tr>'}</tbody>`,
    '    </table>',
    '  </div>',
    '</section>',
  ].join('\n');

  const productRows = products.map((product) => {
    const category = inferProductCategoryKey(product);
    const usingAccounts = usesAccountInventory(product);
    const checkedAttr = product.is_active ? 'checked' : '';
    const stockValue = Number(product.stock_quantity || 0);
    const stockBadgeClass = stockValue <= 3 ? 'stock-badge low' : 'stock-badge';
    const syncForm = usingAccounts
      ? [
        `<form method="post" action="${escapeHtml(`${adminDashboardPath}/product-sync`)}">`,
        `  <input type="hidden" name="product_id" value="${escapeHtml(product.id)}" />`,
        `  <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
        '  <button class="btn-small btn-action btn-neutral" type="submit">Sync kho</button>',
        '</form>',
      ].join('\n')
      : '';
    const accountLink = usingAccounts
      ? `<a class="btn-small link-like" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'accounts', product_id: product.id, products_scope: safeProductScope, account_scope: safeAccountScope }))}">Mở kho</a>`
      : '';
    const deleteForm = [
      `<form method="post" action="${escapeHtml(`${adminDashboardPath}/product-delete`)}">`,
      `  <input type="hidden" name="product_id" value="${escapeHtml(product.id)}" />`,
      `  <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
      '  <button class="btn-small btn-action btn-cancel" type="submit">Xóa / Ẩn</button>',
      '</form>',
    ].join('\n');

    return [
      '<tr>',
      `  <td><div class="id-stack"><strong>${escapeHtml(String(product.id || '').slice(0, 8))}</strong><code>${escapeHtml(product.id)}</code></div></td>`,
      `  <td>${escapeHtml(product.name || '')}</td>`,
      `  <td><span class="tag">${escapeHtml(category)}</span></td>`,
      `  <td><span class="tag">${escapeHtml(product.delivery_type || '')}</span></td>`,
      '  <td>',
      `    <form class="inline-form product-form" method="post" action="${escapeHtml(`${adminDashboardPath}/product-update`)}">`,
      `      <input type="hidden" name="product_id" value="${escapeHtml(product.id)}" />`,
      `      <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
      `      <label>Giá <input type="number" name="price" min="0" step="1" value="${escapeHtml(Math.round(Number(product.price || 0)))}" /></label>`,
      `      <label>Tồn <input type="number" name="stock_quantity" min="0" step="1" value="${escapeHtml(stockValue)}" /></label>`,
      `      <span class="${stockBadgeClass}">${escapeHtml(`Stock ${stockValue}`)}</span>`,
      `      <label class="checkbox"><input type="checkbox" name="is_active" value="1" ${checkedAttr} />Active</label>`,
      '      <button class="btn-small btn-action btn-neutral" type="submit">Lưu</button>',
      '    </form>',
      '  </td>',
      `  <td>${escapeHtml(formatDashboardDateTime(product.updated_at))}</td>`,
      `  <td class="actions">${syncForm}${accountLink}${deleteForm}</td>`,
      '</tr>',
    ].join('\n');
  }).join('\n');

  const createProductForm = [
    `<form class="create-form" method="post" action="${escapeHtml(`${adminDashboardPath}/product-create`)}">`,
    `  <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
    '  <label>Tên sản phẩm <input name="name" type="text" placeholder="Ví dụ: Netflix Premium 1 tháng" required /></label>',
    '  <label>Giá <input name="price" type="number" min="0" step="1" value="0" required /></label>',
    '  <label>Tiền tệ <input name="currency" type="text" value="VND" maxlength="8" /></label>',
    '  <label>Loại',
    '    <select name="product_kind" required>',
    '      <option value="code">Code</option>',
    '      <option value="account" selected>Account</option>',
    '      <option value="support">Support</option>',
    '    </select>',
    '  </label>',
    '  <label>Mô tả <input name="description" type="text" placeholder="Mô tả ngắn cho sản phẩm" /></label>',
    '  <button type="submit">Tạo sản phẩm</button>',
    '</form>',
  ].join('\n');

  const productPanel = [
    '<section class="panel">',
    '  <div class="panel-head">',
    '    <div>',
      '      <h2>Sản phẩm</h2>',
      '      <p class="panel-subtitle">Sửa nhanh giá, tồn kho, trạng thái bán và đồng bộ kho key/account.</p>',
    '    </div>',
    '  </div>',
    `  <div class="chips">${productScopeLinks}</div>`,
    `  ${createProductForm}`,
    '  <div class="table-wrap">',
    '    <table>',
      '      <thead><tr><th>ID</th><th>Tên</th><th>Loại</th><th>Delivery</th><th>Giá / Tồn / Active</th><th>Cập nhật</th><th>Kho account</th></tr></thead>',
    `      <tbody>${productRows || '<tr><td colspan="7">Không có dữ liệu.</td></tr>'}</tbody>`,
    '    </table>',
    '  </div>',
    '</section>',
  ].join('\n');

  const accountOptions = accountProducts.map((product) => {
    const selected = String(product.id) === String(effectiveSelectedProductId) ? 'selected' : '';
    return `<option value="${escapeHtml(product.id)}" ${selected}>${escapeHtml(product.name || product.id)}</option>`;
  }).join('');
  const accountScopeLinks = [
    `<a class="${safeAccountScope === 'use' ? 'chip active' : 'chip'}" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'accounts', product_id: effectiveSelectedProductId, products_scope: safeProductScope, account_scope: 'use' }))}">Use</a>`,
    `<a class="${safeAccountScope === 'used' ? 'chip active' : 'chip'}" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'accounts', product_id: effectiveSelectedProductId, products_scope: safeProductScope, account_scope: 'used' }))}">Used</a>`,
    `<a class="${safeAccountScope === 'all' ? 'chip active' : 'chip'}" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'accounts', product_id: effectiveSelectedProductId, products_scope: safeProductScope, account_scope: 'all' }))}">All</a>`,
  ].join('');
  const accountScopeLabel = safeAccountScope === 'used'
    ? 'Used'
    : safeAccountScope === 'all'
      ? 'All'
      : 'Use';

  let accountPanel = [
    '<section class="panel">',
    '  <div class="panel-head">',
    '    <div>',
    '      <h2>Kho account</h2>',
    '      <p class="panel-subtitle">Chọn sản phẩm key/account, thêm dữ liệu kho và theo dõi trạng thái used.</p>',
    '    </div>',
    `    <form class="inline-form" method="get" action="${escapeHtml(adminDashboardPath)}">`,
    '      <input type="hidden" name="tab" value="accounts" />',
    `      <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
    `      <input type="hidden" name="account_scope" value="${escapeHtml(safeAccountScope)}" />`,
    `      <select name="product_id">${accountOptions || '<option value="">(không có sản phẩm KEY/ACCOUNT)</option>'}</select>`,
    '      <button type="submit">Xem</button>',
    '    </form>',
    '  </div>',
    `  <div class="chips">${accountScopeLinks}</div>`,
  ].join('\n');

  if (selectedProduct && accountSummary) {
    const accountRows = (accountSummary.preview || []).map((row, index) => {
      const parsed = parseAccountData(row.account_data);
      const statusBadge = row.is_used
        ? '<span class="status-badge cancelled">used</span>'
        : '<span class="status-badge paid">available</span>';
      const orderCell = row.used_order_id ? `<code>${escapeHtml(row.used_order_id)}</code>` : '-';
      const stateForm = [
        `<form method="post" action="${escapeHtml(`${adminDashboardPath}/account-state`)}">`,
        `  <input type="hidden" name="product_id" value="${escapeHtml(selectedProduct.id)}" />`,
        `  <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
        `  <input type="hidden" name="account_scope" value="${escapeHtml(safeAccountScope)}" />`,
        `  <input type="hidden" name="account_id" value="${escapeHtml(row.id)}" />`,
        `  <input type="hidden" name="target" value="${row.is_used ? '0' : '1'}" />`,
        `  <button class="btn-small btn-action ${row.is_used ? 'btn-neutral' : 'btn-paid'}" type="submit">${row.is_used ? 'Unuse' : 'Use'}</button>`,
        '</form>',
      ].join('\n');
      const editForm = [
        `<form method="post" action="${escapeHtml(`${adminDashboardPath}/account-update`)}">`,
        `  <input type="hidden" name="product_id" value="${escapeHtml(selectedProduct.id)}" />`,
        `  <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
        `  <input type="hidden" name="account_scope" value="${escapeHtml(safeAccountScope)}" />`,
        `  <input type="hidden" name="account_id" value="${escapeHtml(row.id)}" />`,
        `  <input class="edit-account-input" type="text" name="account_data" value="${escapeHtml(String(row.account_data || ''))}" />`,
        '  <button class="btn-small btn-action btn-neutral" type="submit">Sửa</button>',
        '</form>',
      ].join('\n');
      const deleteForm = row.is_used
        ? ''
        : [
          `<form method="post" action="${escapeHtml(`${adminDashboardPath}/account-delete`)}">`,
          `  <input type="hidden" name="product_id" value="${escapeHtml(selectedProduct.id)}" />`,
          `  <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
          `  <input type="hidden" name="account_scope" value="${escapeHtml(safeAccountScope)}" />`,
          `  <input type="hidden" name="account_id" value="${escapeHtml(row.id)}" />`,
          '  <button class="btn-small btn-action btn-cancel" type="submit">Xóa</button>',
          '</form>',
        ].join('\n');
      return [
        '<tr>',
        `  <td>${index + 1}</td>`,
        `  <td>${escapeHtml(parsed.account)}</td>`,
        `  <td>${escapeHtml(parsed.password)}</td>`,
        `  <td>${escapeHtml(parsed.twofa)}</td>`,
        `  <td>${statusBadge}</td>`,
        `  <td>${orderCell}</td>`,
        `  <td class="actions">${stateForm}${editForm}${deleteForm}</td>`,
      '</tr>',
    ].join('\n');
  }).join('\n');

    accountPanel += [
      '  <div class="account-summary">',
      `    <div class="tagline"><strong>${escapeHtml(selectedProduct.name)}</strong><span class="tag">Stock products: ${escapeHtml(String(Number(selectedProduct.stock_quantity || 0)))}</span><span class="tag">Tab: ${escapeHtml(accountScopeLabel)}</span></div>`,
      `    <span class="status-badge paid">Available: ${escapeHtml(String(accountSummary.available))}</span>`,
      `    <span class="status-badge cancelled">Used: ${escapeHtml(String(accountSummary.used))}</span>`,
      '  </div>',
      `  <form class="account-form" method="post" action="${escapeHtml(`${adminDashboardPath}/account-add`)}">`,
      `    <input type="hidden" name="product_id" value="${escapeHtml(selectedProduct.id)}" />`,
      `    <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
      `    <input type="hidden" name="account_scope" value="${escapeHtml(safeAccountScope)}" />`,
      '    <textarea name="account_lines" rows="8" placeholder="Mỗi dòng 1 account. Ví dụ: email@gmail.com|MatKhau|2FA"></textarea>',
      '    <button type="submit">Thêm vào kho</button>',
      '  </form>',
      `  <form class="inline-form" method="post" action="${escapeHtml(`${adminDashboardPath}/product-sync`)}">`,
      `    <input type="hidden" name="product_id" value="${escapeHtml(selectedProduct.id)}" />`,
      '    <input type="hidden" name="tab" value="accounts" />',
      `    <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
      `    <input type="hidden" name="account_scope" value="${escapeHtml(safeAccountScope)}" />`,
      '    <button class="btn-small btn-action btn-neutral" type="submit">Sync stock từ kho account</button>',
      '  </form>',
      '  <div class="table-wrap">',
      '    <table>',
      '      <thead><tr><th>#</th><th>Account</th><th>Password</th><th>2FA</th><th>State</th><th>Order used</th><th>Hành động</th></tr></thead>',
      `      <tbody>${accountRows || '<tr><td colspan="7">Kho trống.</td></tr>'}</tbody>`,
      '    </table>',
      '  </div>',
    ].join('\n');
  } else {
    accountPanel += '<p>Chọn sản phẩm KEY/ACCOUNT để xem kho.</p>';
  }
  accountPanel += '</section>';

  const reportSnapshotCards = [
    {
      title: 'Doanh thu hôm nay',
      value: formatDashboardRevenueByCurrency(normalizedReports.snapshot.today),
      tone: 'green',
    },
    {
      title: 'Doanh thu tháng này',
      value: formatDashboardRevenueByCurrency(normalizedReports.snapshot.month),
      tone: 'teal',
    },
    {
      title: 'Doanh thu năm nay',
      value: formatDashboardRevenueByCurrency(normalizedReports.snapshot.year),
      tone: 'blue',
    },
  ];
  const reportSnapshotCardsHtml = reportSnapshotCards.map((card) => [
    `<article class="stat-card tone-${escapeHtml(card.tone)}">`,
    `  <h3>${escapeHtml(card.title)}</h3>`,
    `  <p class="stat-multi">${escapeHtml(card.value)}</p>`,
    '</article>',
  ].join('\n')).join('\n');

  const reportProductRows = normalizedReports.byProduct.map((row, index) => [
    '<tr>',
    `  <td>${index + 1}</td>`,
    `  <td>${escapeHtml(row.productName || '(không rõ)')}</td>`,
    `  <td><code>${escapeHtml(row.productId || '-')}</code></td>`,
    `  <td>${escapeHtml(String(row.totalQuantity))}</td>`,
    `  <td>${escapeHtml(formatPriceVnd(row.totalRevenue))}</td>`,
    `  <td>${escapeHtml(String(row.totalOrders))}</td>`,
    `  <td>${escapeHtml(row.currency || 'VND')}</td>`,
    '</tr>',
  ].join('\n')).join('\n');

  const buildReportPeriodRows = (rows, periodType) => rows.map((row) => [
    '<tr>',
    `  <td>${escapeHtml(formatDashboardPeriodLabel(row.periodKey, periodType))}</td>`,
    `  <td>${escapeHtml(String(row.orderCount))}</td>`,
    `  <td>${escapeHtml(formatPriceVnd(row.totalRevenue))}</td>`,
    `  <td>${escapeHtml(row.currency || 'VND')}</td>`,
    '</tr>',
  ].join('\n')).join('\n');

  const reportByDayRows = buildReportPeriodRows(normalizedReports.byDay, 'day');
  const reportByMonthRows = buildReportPeriodRows(normalizedReports.byMonth, 'month');
  const reportByYearRows = buildReportPeriodRows(normalizedReports.byYear, 'year');
  const buildReportProductPeriodRows = (rows, periodType) => rows.map((row) => [
    '<tr>',
    `  <td>${escapeHtml(formatDashboardPeriodLabel(row.periodKey, periodType))}</td>`,
    `  <td>${escapeHtml(row.productName || '(không rõ)')}</td>`,
    `  <td><code>${escapeHtml(row.productId || '-')}</code></td>`,
    `  <td>${escapeHtml(String(row.totalQuantity))}</td>`,
    `  <td>${escapeHtml(String(row.totalOrders))}</td>`,
    '</tr>',
  ].join('\n')).join('\n');
  const reportProductByDayRows = buildReportProductPeriodRows(normalizedReports.byProductDay, 'day');
  const reportProductByMonthRows = buildReportProductPeriodRows(normalizedReports.byProductMonth, 'month');
  const reportProductByYearRows = buildReportProductPeriodRows(normalizedReports.byProductYear, 'year');
  const reportFilterForm = [
    `<form class="inline-form" method="get" action="${escapeHtml(adminDashboardPath)}">`,
    '  <input type="hidden" name="tab" value="reports" />',
    `  <input type="hidden" name="products_scope" value="${escapeHtml(safeProductScope)}" />`,
    `  <label>Tên sản phẩm <input type="text" name="product_keyword" value="${escapeHtml(safeReportFilters.productKeyword)}" placeholder="Ví dụ: Netflix" /></label>`,
    `  <label>Từ ngày <input type="date" name="date_from" value="${escapeHtml(safeReportFilters.dateFrom)}" /></label>`,
    `  <label>Đến ngày <input type="date" name="date_to" value="${escapeHtml(safeReportFilters.dateTo)}" /></label>`,
    '  <button type="submit">Lọc</button>',
    `  <a class="link-like" href="${escapeHtml(buildAdminDashboardUrl({ tab: 'reports', products_scope: safeProductScope }))}">Xóa lọc</a>`,
    '</form>',
  ].join('\n');

  const reportPanel = [
    '<section class="panel">',
    '  <div class="panel-head">',
    '    <div>',
    '      <h2>Thống kê</h2>',
    '      <p class="panel-subtitle">Báo cáo theo sản phẩm đã bán (ngày/tháng/năm) và doanh thu theo kỳ. Có thể lọc theo tên sản phẩm và khoảng ngày.</p>',
    '    </div>',
    `    ${reportFilterForm}`,
    '  </div>',
    `  <section class="stat-grid">${reportSnapshotCardsHtml}</section>`,
    '  <section class="report-section">',
    '    <h3>Tổng theo sản phẩm đã bán</h3>',
    '    <div class="table-wrap">',
    '      <table>',
    '        <thead><tr><th>#</th><th>Sản phẩm</th><th>Product ID</th><th>Số lượng bán</th><th>Doanh thu</th><th>Số đơn</th><th>Tiền tệ</th></tr></thead>',
    `        <tbody>${reportProductRows || '<tr><td colspan="7">Không có dữ liệu.</td></tr>'}</tbody>`,
    '      </table>',
    '    </div>',
  '  </section>',
    '  <section class="report-section">',
    '    <h3>Số lượng theo sản phẩm đã bán (ngày)</h3>',
    '    <div class="table-wrap">',
    '      <table>',
    '        <thead><tr><th>Ngày</th><th>Sản phẩm</th><th>Product ID</th><th>Số lượng đã bán</th><th>Số đơn</th></tr></thead>',
    `        <tbody>${reportProductByDayRows || '<tr><td colspan="5">Không có dữ liệu.</td></tr>'}</tbody>`,
    '      </table>',
    '    </div>',
    '  </section>',
    '  <section class="report-section">',
    '    <h3>Số lượng theo sản phẩm đã bán (tháng)</h3>',
    '    <div class="table-wrap">',
    '      <table>',
    '        <thead><tr><th>Tháng</th><th>Sản phẩm</th><th>Product ID</th><th>Số lượng đã bán</th><th>Số đơn</th></tr></thead>',
    `        <tbody>${reportProductByMonthRows || '<tr><td colspan="5">Không có dữ liệu.</td></tr>'}</tbody>`,
    '      </table>',
    '    </div>',
    '  </section>',
    '  <section class="report-section">',
    '    <h3>Số lượng theo sản phẩm đã bán (năm)</h3>',
    '    <div class="table-wrap">',
    '      <table>',
    '        <thead><tr><th>Năm</th><th>Sản phẩm</th><th>Product ID</th><th>Số lượng đã bán</th><th>Số đơn</th></tr></thead>',
    `        <tbody>${reportProductByYearRows || '<tr><td colspan="5">Không có dữ liệu.</td></tr>'}</tbody>`,
    '      </table>',
    '    </div>',
    '  </section>',
    '  <section class="report-section">',
    '    <h3>Doanh thu theo ngày</h3>',
    '    <div class="table-wrap">',
    '      <table>',
    '        <thead><tr><th>Ngày</th><th>Số đơn</th><th>Doanh thu</th><th>Tiền tệ</th></tr></thead>',
    `        <tbody>${reportByDayRows || '<tr><td colspan="4">Không có dữ liệu.</td></tr>'}</tbody>`,
    '      </table>',
    '    </div>',
    '  </section>',
    '  <section class="report-section">',
    '    <h3>Doanh thu theo tháng</h3>',
    '    <div class="table-wrap">',
    '      <table>',
    '        <thead><tr><th>Tháng</th><th>Số đơn</th><th>Doanh thu</th><th>Tiền tệ</th></tr></thead>',
    `        <tbody>${reportByMonthRows || '<tr><td colspan="4">Không có dữ liệu.</td></tr>'}</tbody>`,
    '      </table>',
    '    </div>',
    '  </section>',
    '  <section class="report-section">',
    '    <h3>Doanh thu theo năm</h3>',
    '    <div class="table-wrap">',
    '      <table>',
    '        <thead><tr><th>Năm</th><th>Số đơn</th><th>Doanh thu</th><th>Tiền tệ</th></tr></thead>',
    `        <tbody>${reportByYearRows || '<tr><td colspan="4">Không có dữ liệu.</td></tr>'}</tbody>`,
    '      </table>',
    '    </div>',
    '  </section>',
    '</section>',
  ].join('\n');

  const mainStatGridHtml = safeTab === 'reports'
    ? ''
    : `<section class="stat-grid">${statCardsHtml}</section>`;

  const panelContent = safeTab === 'products'
    ? productPanel
    : safeTab === 'accounts'
      ? accountPanel
      : safeTab === 'reports'
        ? reportPanel
        : orderPanel;

  return [
    '<!doctype html>',
    '<html lang="vi">',
    '<head>',
    '  <meta charset="utf-8" />',
    '  <meta name="viewport" content="width=device-width, initial-scale=1" />',
    '  <title>Admin Dashboard</title>',
    '  <link rel="preconnect" href="https://fonts.googleapis.com" />',
    '  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />',
    '  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Space+Grotesk:wght@500;700&display=swap" rel="stylesheet" />',
    '  <style>',
    '    :root { --bg: #edf4ff; --text: #10233a; --muted: #61758d; --panel: #ffffff; --line: #d6e2ee; --brand: #0f8a7f; --brand-dark: #0a6f66; --accent: #ff8a24; --danger: #b42318; --danger-soft: #ffe5e1; --ok: #067647; --ok-soft: #dcfae6; }',
    '    * { box-sizing: border-box; }',
    '    body { margin: 0; font-family: "IBM Plex Sans", sans-serif; color: var(--text); background: radial-gradient(75rem 28rem at 0% -5%, #c7e5ff, transparent 58%), radial-gradient(70rem 24rem at 100% -15%, #d8fff4, transparent 58%), var(--bg); }',
    '    .wrap { max-width: 1440px; margin: 16px auto 32px; padding: 0 16px; }',
    '    .hero { display: grid; gap: 10px; margin-bottom: 14px; padding: 16px; border: 1px solid var(--line); border-radius: 18px; background: linear-gradient(115deg, rgba(15,138,127,0.1), rgba(255,138,36,0.12)); }',
    '    .hero h1 { margin: 0; font-family: "Space Grotesk", sans-serif; font-size: 30px; line-height: 1.05; }',
    '    .hero p { margin: 0; color: #274765; font-size: 14px; }',
    '    .top-actions { display: flex; justify-content: space-between; align-items: center; gap: 10px; flex-wrap: wrap; }',
    '    .tabs { display: inline-flex; background: rgba(255,255,255,0.65); border: 1px solid rgba(16,35,58,0.12); border-radius: 999px; padding: 4px; gap: 4px; backdrop-filter: blur(6px); }',
    '    .tab { text-decoration: none; border-radius: 999px; padding: 8px 14px; color: #1b3550; font-weight: 600; font-size: 14px; }',
    '    .tab.active { background: #fff; color: #0a4e48; box-shadow: 0 4px 12px rgba(0,0,0,0.09); }',
    '    .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin: 12px 0; }',
    '    .stat-card { border: 1px solid var(--line); border-radius: 14px; padding: 10px 12px; background: #fff; min-height: 84px; display: grid; align-content: center; }',
    '    .stat-card h3 { margin: 0; font-size: 12px; text-transform: uppercase; letter-spacing: 0.03em; color: var(--muted); }',
    '    .stat-card p { margin: 4px 0 0; font-family: "Space Grotesk", sans-serif; font-size: 27px; line-height: 1; }',
    '    .stat-card p.stat-multi { font-family: "IBM Plex Sans", sans-serif; font-size: 14px; line-height: 1.35; }',
    '    .tone-blue { background: linear-gradient(160deg, #ffffff, #edf6ff); }',
    '    .tone-amber { background: linear-gradient(160deg, #ffffff, #fff5e8); }',
    '    .tone-green { background: linear-gradient(160deg, #ffffff, #ebfff5); }',
    '    .tone-teal { background: linear-gradient(160deg, #ffffff, #eafffb); }',
    '    .tone-red { background: linear-gradient(160deg, #ffffff, #fff0ef); }',
    '    .tone-purple { background: linear-gradient(160deg, #ffffff, #f5f2ff); }',
    '    .tone-gray { background: linear-gradient(160deg, #ffffff, #f4f6f9); }',
    '    .alert { border-radius: 12px; padding: 10px 12px; margin: 8px 0; font-size: 14px; }',
    '    .alert-info { background: #dff4ff; color: #0c4a6e; border: 1px solid #c1e8ff; }',
    '    .alert-error { background: #ffe7e4; color: var(--danger); border: 1px solid #fbcfca; }',
    '    .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 14px; box-shadow: 0 10px 24px rgba(18,35,58,0.08); }',
    '    .panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; flex-wrap: wrap; margin-bottom: 10px; }',
    '    .panel h2 { margin: 0; font-family: "Space Grotesk", sans-serif; font-size: 24px; }',
    '    .panel-subtitle { margin: 5px 0 0; color: var(--muted); font-size: 14px; }',
    '    .report-section { display: grid; gap: 8px; margin-top: 12px; }',
    '    .report-section h3 { margin: 0; font-family: "Space Grotesk", sans-serif; font-size: 18px; }',
    '    .chips { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }',
    '    .chip { display: inline-flex; align-items: center; padding: 6px 11px; border-radius: 999px; border: 1px solid var(--line); color: #274765; text-decoration: none; font-size: 12px; font-weight: 600; background: #f9fcff; }',
    '    .chip.active { background: #e1fffa; border-color: #8cd5cb; color: #0a6159; }',
    '    .table-wrap { overflow: auto; border: 1px solid #e5edf4; border-radius: 14px; }',
    '    table { width: 100%; border-collapse: collapse; min-width: 940px; }',
    '    thead th { position: sticky; top: 0; z-index: 2; background: #f8fbff; }',
    '    th, td { border-bottom: 1px solid #edf2f7; text-align: left; vertical-align: top; padding: 9px 10px; font-size: 13px; }',
    '    th { font-size: 11px; letter-spacing: 0.03em; text-transform: uppercase; color: #64748b; }',
    '    tbody tr:nth-child(even) { background: #fcfeff; }',
    '    .id-stack { display: grid; gap: 2px; }',
    '    .id-stack strong { font-size: 13px; }',
    '    code { font-size: 11px; color: #47627c; }',
    '    .status-badge, .tag, .stock-badge { display: inline-flex; align-items: center; padding: 4px 8px; border-radius: 999px; font-size: 11px; font-weight: 600; border: 1px solid transparent; }',
    '    .status-badge.neutral { background: #eef2f6; border-color: #d7dee6; color: #425466; }',
    '    .status-badge.draft { background: #f2f4f7; border-color: #d0d6dd; color: #344054; }',
    '    .status-badge.confirmed { background: #fff4df; border-color: #ffd7a0; color: #8a4800; }',
    '    .status-badge.paid { background: var(--ok-soft); border-color: #99e9be; color: var(--ok); }',
    '    .status-badge.cancelled { background: var(--danger-soft); border-color: #f4b8b3; color: var(--danger); }',
    '    .tag { background: #f1f6fd; border-color: #d4e1ef; color: #274765; }',
    '    .stock-badge { background: #ecfdf3; border-color: #a9e6c0; color: #067647; }',
    '    .stock-badge.low { background: #fff1ee; border-color: #f7bbb3; color: #b42318; }',
    '    .actions { display: flex; flex-wrap: wrap; gap: 6px; }',
    '    .inline-form { display: inline-flex; gap: 8px; align-items: center; flex-wrap: wrap; }',
    '    .create-form { display: grid; grid-template-columns: minmax(210px, 2fr) repeat(4, minmax(120px, 1fr)) auto; gap: 8px; align-items: end; padding: 10px; margin-bottom: 10px; border: 1px solid #dbe6f2; border-radius: 12px; background: #f9fcff; }',
    '    .create-form label { display: grid; gap: 4px; font-size: 12px; color: #475467; }',
    '    .create-form button { height: 38px; white-space: nowrap; }',
    '    .product-form label { display: inline-flex; align-items: center; gap: 6px; color: #475467; font-size: 12px; }',
    '    .edit-account-input { min-width: 260px; max-width: 420px; width: min(44vw, 420px); }',
    '    input[type="number"], input[type="text"], input[type="date"], select, textarea { border: 1px solid #cdd9e5; border-radius: 10px; padding: 7px 9px; font-size: 13px; color: var(--text); background: #fff; }',
    '    textarea { width: 100%; min-height: 170px; resize: vertical; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }',
    '    input:focus, select:focus, textarea:focus { outline: 2px solid #b5ded8; outline-offset: 1px; border-color: #8bcfc6; }',
    '    button, .link-like { border: none; border-radius: 10px; background: var(--brand); color: #fff; padding: 8px 10px; font-weight: 600; font-size: 13px; cursor: pointer; text-decoration: none; display: inline-flex; align-items: center; justify-content: center; transition: transform .08s ease, filter .15s ease; }',
    '    button:hover, .link-like:hover { filter: brightness(0.96); }',
    '    button:active, .link-like:active { transform: translateY(1px); }',
    '    .btn-small { font-size: 12px; padding: 6px 8px; }',
    '    .btn-action.btn-confirm { background: #b45309; }',
    '    .btn-action.btn-paid { background: #067647; }',
    '    .btn-action.btn-cancel { background: #b42318; }',
    '    .btn-action.btn-neutral { background: #475467; }',
    '    .checkbox { display: inline-flex; align-items: center; gap: 5px; font-size: 12px; color: #475467; }',
    '    .account-summary { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 10px; }',
    '    .tagline { display: inline-flex; gap: 8px; align-items: center; margin-right: 8px; }',
    '    .account-form { display: grid; gap: 8px; margin-bottom: 10px; }',
    '    .actions-row { display: flex; gap: 8px; flex-wrap: wrap; }',
    '    @media (max-width: 1180px) { .create-form { grid-template-columns: 1fr 1fr; } .create-form button { grid-column: span 2; } }',
    '    @media (max-width: 930px) { .hero h1 { font-size: 25px; } .tabs { width: 100%; justify-content: space-between; } .tab { flex: 1; text-align: center; } .panel { padding: 10px; } .create-form { grid-template-columns: 1fr; } .create-form button { grid-column: span 1; } }',
  '  </style>',
    '</head>',
    '<body>',
    '  <div class="wrap">',
    '    <section class="hero">',
    '      <div class="top-actions">',
    '        <div>',
    '          <h1>Admin Control Center</h1>',
    '          <p>Ưu tiên thao tác nhanh: cập nhật trạng thái đơn, quản lý giá/tồn, và đồng bộ kho account.</p>',
    '        </div>',
    `        <form method="post" action="${escapeHtml(`${adminDashboardPath}/logout`)}"><button type="submit">Đăng xuất</button></form>`,
    '      </div>',
    `      <nav class="tabs">${nav}</nav>`,
    '    </section>',
    `    ${mainStatGridHtml}`,
    `    ${infoBlock}`,
    `    ${errorBlock}`,
    `    ${panelContent}`,
    '  </div>',
    '</body>',
    '</html>',
  ].join('\n');
}

async function handleAdminDashboardRequest(req, res, requestUrl) {
  const pathnameRaw = String(requestUrl.pathname || '').trim();
  const pathname = pathnameRaw.length > 1 && pathnameRaw.endsWith('/')
    ? pathnameRaw.slice(0, -1)
    : pathnameRaw;
  const loginPath = `${adminDashboardPath}/login`;
  const logoutPath = `${adminDashboardPath}/logout`;
  const orderStatusPath = `${adminDashboardPath}/order-status`;
  const productCreatePath = `${adminDashboardPath}/product-create`;
  const productUpdatePath = `${adminDashboardPath}/product-update`;
  const productDeletePath = `${adminDashboardPath}/product-delete`;
  const productSyncPath = `${adminDashboardPath}/product-sync`;
  const accountAddPath = `${adminDashboardPath}/account-add`;
  const accountUpdatePath = `${adminDashboardPath}/account-update`;
  const accountDeletePath = `${adminDashboardPath}/account-delete`;
  const accountStatePath = `${adminDashboardPath}/account-state`;

  if (
    pathname !== adminDashboardPath
    && pathname !== loginPath
    && pathname !== logoutPath
    && pathname !== orderStatusPath
    && pathname !== productCreatePath
    && pathname !== productUpdatePath
    && pathname !== productDeletePath
    && pathname !== productSyncPath
    && pathname !== accountAddPath
    && pathname !== accountUpdatePath
    && pathname !== accountDeletePath
    && pathname !== accountStatePath
  ) {
    return false;
  }

  if (pathname === loginPath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const key = String(form.key || '').trim();

    if (!adminDashboardKey) {
      sendHtml(res, 503, renderAdminDashboardLoginPage('Chưa cấu hình ADMIN_DASHBOARD_KEY trong .env'));
      return true;
    }

    if (key !== adminDashboardKey) {
      sendHtml(res, 401, renderAdminDashboardLoginPage('Key đăng nhập không đúng.'));
      return true;
    }

    const sessionToken = createAdminDashboardSession();
    redirectResponse(res, buildAdminDashboardUrl({ tab: 'orders' }), {
      'Set-Cookie': buildAdminDashboardSessionCookie(sessionToken),
    });
    return true;
  }

  const session = getAdminDashboardSession(req);
  if (!session) {
    if (req.method === 'GET' && pathname === adminDashboardPath) {
      sendHtml(res, 200, renderAdminDashboardLoginPage(!adminDashboardKey ? 'Cần cấu hình ADMIN_DASHBOARD_KEY để bật dashboard.' : ''));
      return true;
    }
    redirectResponse(res, adminDashboardPath, {
      'Set-Cookie': buildClearAdminDashboardSessionCookie(),
    });
    return true;
  }

  if (pathname === logoutPath && (req.method === 'POST' || req.method === 'GET')) {
    revokeAdminDashboardSession(req);
    redirectResponse(res, adminDashboardPath, {
      'Set-Cookie': buildClearAdminDashboardSessionCookie(),
    });
    return true;
  }

  if (pathname === orderStatusPath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const orderId = String(form.order_id || '').trim();
    const status = normalizeDashboardOrderStatus(form.status);
    const statusFilter = normalizeDashboardOrderStatus(form.status_filter);
    const productScope = normalizeDashboardProductScope(form.products_scope);
    if (!orderId || status === 'all') {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'orders',
        status_filter: statusFilter,
        products_scope: productScope,
        err: 'Thiếu dữ liệu cập nhật trạng thái đơn.',
      }));
      return true;
    }

    try {
      const result = await updateOrderStatusFromAdmin(orderId, status, null, 'Updated from admin dashboard');
      if (!result.ok) {
        const message = result.reason === 'not_found' ? 'Không tìm thấy đơn.' : 'Không thể cập nhật trạng thái.';
        redirectResponse(res, buildAdminDashboardUrl({
          tab: 'orders',
          status_filter: statusFilter,
          products_scope: productScope,
          err: message,
        }));
        return true;
      }

      await notifyOrderOwnerStatusChanged(result.order);
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'orders',
        status_filter: statusFilter,
        products_scope: productScope,
        msg: `Đã cập nhật đơn #${orderId} -> ${status}`,
      }));
      return true;
    } catch (error) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'orders',
        status_filter: statusFilter,
        products_scope: productScope,
        err: `Cập nhật thất bại: ${String(error.message || 'unknown')}`,
      }));
      return true;
    }
  }

  if (pathname === productCreatePath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const productScope = normalizeDashboardProductScope(form.products_scope);
    const name = String(form.name || '').trim();
    const price = parsePositiveMoney(String(form.price || '').trim());
    const currency = String(form.currency || 'VND').trim().toUpperCase() || 'VND';
    const productKind = normalizeAddProductType(form.product_kind);
    const description = String(form.description || '').trim();
    if (!name || price === null || !productKind) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'products',
        products_scope: productScope,
        err: 'Dữ liệu tạo sản phẩm chưa hợp lệ.',
      }));
      return true;
    }

    try {
      const created = await createProduct({
        name,
        price,
        currency,
        productKind,
        description,
        deliveryType: productKind === 'support' ? 'manual' : 'auto',
      });
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'products',
        products_scope: productScope,
        msg: `Đã tạo sản phẩm: ${created.name}`,
      }));
      return true;
    } catch (error) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'products',
        products_scope: productScope,
        err: `Tạo sản phẩm thất bại: ${String(error.message || 'unknown')}`,
      }));
      return true;
    }
  }

  if (pathname === productUpdatePath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const productScope = normalizeDashboardProductScope(form.products_scope);
    const productId = String(form.product_id || '').trim();
    const price = parsePositiveMoney(String(form.price || '').trim());
    const stockQuantity = parseNonNegativeInt(String(form.stock_quantity || '').trim());
    const isActive = String(form.is_active || '').trim() === '1';

    if (!productId || price === null || stockQuantity === null) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'products',
        products_scope: productScope,
        err: 'Giá hoặc tồn kho không hợp lệ.',
      }));
      return true;
    }

    try {
      await updateAdminProduct(productId, {
        price,
        stock_quantity: stockQuantity,
        is_active: isActive,
      });
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'products',
        products_scope: productScope,
        msg: 'Đã cập nhật sản phẩm.',
      }));
      return true;
    } catch (error) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'products',
        products_scope: productScope,
        err: `Cập nhật sản phẩm thất bại: ${String(error.message || 'unknown')}`,
      }));
      return true;
    }
  }

  if (pathname === productDeletePath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const productScope = normalizeDashboardProductScope(form.products_scope);
    const productId = String(form.product_id || '').trim();
    if (!productId) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'products',
        products_scope: productScope,
        err: 'Thiếu product_id để xóa sản phẩm.',
      }));
      return true;
    }

    try {
      const result = await deleteProductWithFallback(productId);
      if (result.mode === 'soft_hidden') {
        redirectResponse(res, buildAdminDashboardUrl({
          tab: 'products',
          products_scope: productScope,
          msg: `SP có ${result.linkedOrderItems} dòng đơn, đã chuyển tạm ẩn thay vì xóa cứng.`,
        }));
      } else {
        redirectResponse(res, buildAdminDashboardUrl({
          tab: 'products',
          products_scope: productScope,
          msg: 'Đã xóa sản phẩm.',
        }));
      }
      return true;
    } catch (error) {
      try {
        await updateAdminProduct(productId, { is_active: false });
        redirectResponse(res, buildAdminDashboardUrl({
          tab: 'products',
          products_scope: productScope,
          msg: 'Không thể xóa cứng, đã chuyển tạm ẩn.',
        }));
      } catch (nestedError) {
        redirectResponse(res, buildAdminDashboardUrl({
          tab: 'products',
          products_scope: productScope,
          err: `Xóa sản phẩm thất bại: ${String(error.message || 'unknown')}`,
        }));
      }
      return true;
    }
  }

  if (pathname === productSyncPath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const productScope = normalizeDashboardProductScope(form.products_scope);
    const accountScope = normalizeDashboardAccountScope(form.account_scope);
    const productId = String(form.product_id || '').trim();
    if (!productId) {
      redirectResponse(res, buildAdminDashboardUrl({ tab: 'products', products_scope: productScope, account_scope: accountScope, err: 'Thiếu product_id để sync kho.' }));
      return true;
    }

    try {
      const synced = await syncProductStockFromAutoAccounts(productId);
      const targetTab = String(form.tab || '').trim() === 'accounts' ? 'accounts' : 'products';
      redirectResponse(res, buildAdminDashboardUrl({
        tab: targetTab,
        product_id: targetTab === 'accounts' ? productId : '',
        products_scope: productScope,
        account_scope: accountScope,
        msg: `Đã sync stock: ${Number(synced.stock_quantity || 0)}`,
      }));
      return true;
    } catch (error) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'products',
        products_scope: productScope,
        account_scope: accountScope,
        err: `Sync stock thất bại: ${String(error.message || 'unknown')}`,
      }));
      return true;
    }
  }

  if (pathname === accountAddPath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const productScope = normalizeDashboardProductScope(form.products_scope);
    const accountScope = normalizeDashboardAccountScope(form.account_scope);
    const productId = String(form.product_id || '').trim();
    const accountLines = String(form.account_lines || '').trim();

    if (!productId || !accountLines) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: productId,
        products_scope: productScope,
        account_scope: accountScope,
        err: 'Vui lòng nhập dữ liệu account.',
      }));
      return true;
    }

    try {
      const result = await addProductAccountsBulk(productId, accountLines);
      const synced = await syncProductStockFromAutoAccounts(productId);
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: productId,
        products_scope: productScope,
        account_scope: accountScope,
        msg: `Đã thêm ${result.added}/${result.total} dòng, stock=${Number(synced.stock_quantity || 0)}`,
      }));
      return true;
    } catch (error) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: productId,
        products_scope: productScope,
        account_scope: accountScope,
        err: `Thêm account thất bại: ${String(error.message || 'unknown')}`,
      }));
      return true;
    }
  }

  if (pathname === accountUpdatePath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const productScope = normalizeDashboardProductScope(form.products_scope);
    const accountScope = normalizeDashboardAccountScope(form.account_scope);
    const accountId = String(form.account_id || '').trim();
    const productId = String(form.product_id || '').trim();
    const accountData = String(form.account_data || '').trim();
    if (!accountId || !accountData) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: productId,
        products_scope: productScope,
        account_scope: accountScope,
        err: 'Thiếu dữ liệu để sửa account.',
      }));
      return true;
    }

    try {
      const account = await loadProductAccountById(accountId);
      if (!account) {
        redirectResponse(res, buildAdminDashboardUrl({
          tab: 'accounts',
          product_id: productId,
          products_scope: productScope,
          account_scope: accountScope,
          err: 'Không tìm thấy account.',
        }));
        return true;
      }

      await updateProductAccountDataById(accountId, accountData);
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: account.product_id,
        products_scope: productScope,
        account_scope: accountScope,
        msg: 'Đã cập nhật dữ liệu account.',
      }));
      return true;
    } catch (error) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: productId,
        products_scope: productScope,
        account_scope: accountScope,
        err: `Sửa account thất bại: ${String(error.message || 'unknown')}`,
      }));
      return true;
    }
  }

  if (pathname === accountDeletePath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const productScope = normalizeDashboardProductScope(form.products_scope);
    const accountScope = normalizeDashboardAccountScope(form.account_scope);
    const accountId = String(form.account_id || '').trim();
    const productId = String(form.product_id || '').trim();
    if (!accountId) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: productId,
        products_scope: productScope,
        account_scope: accountScope,
        err: 'Thiếu account_id để xóa.',
      }));
      return true;
    }

    try {
      const account = await loadProductAccountById(accountId);
      if (!account) {
        redirectResponse(res, buildAdminDashboardUrl({
          tab: 'accounts',
          product_id: productId,
          products_scope: productScope,
          account_scope: accountScope,
          err: 'Không tìm thấy account.',
        }));
        return true;
      }
      if (account.is_used) {
        redirectResponse(res, buildAdminDashboardUrl({
          tab: 'accounts',
          product_id: account.product_id || productId,
          products_scope: productScope,
          account_scope: accountScope,
          err: 'Account đã used, không xóa trực tiếp.',
        }));
        return true;
      }

      await deleteProductAccountById(accountId);
      await syncProductStockFromAutoAccounts(account.product_id);
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: account.product_id,
        products_scope: productScope,
        account_scope: accountScope,
        msg: 'Đã xóa account khỏi kho.',
      }));
      return true;
    } catch (error) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: productId,
        products_scope: productScope,
        account_scope: accountScope,
        err: `Xóa account thất bại: ${String(error.message || 'unknown')}`,
      }));
      return true;
    }
  }

  if (pathname === accountStatePath && req.method === 'POST') {
    const rawBody = await readRawRequestBody(req);
    const form = parseFormUrlEncoded(rawBody);
    const productScope = normalizeDashboardProductScope(form.products_scope);
    const accountScope = normalizeDashboardAccountScope(form.account_scope);
    const accountId = String(form.account_id || '').trim();
    const productId = String(form.product_id || '').trim();
    const target = String(form.target || '').trim() === '1';
    if (!accountId) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: productId,
        products_scope: productScope,
        account_scope: accountScope,
        err: 'Thiếu account_id để chuyển trạng thái.',
      }));
      return true;
    }

    try {
      const account = await loadProductAccountById(accountId);
      if (!account) {
        redirectResponse(res, buildAdminDashboardUrl({
          tab: 'accounts',
          product_id: productId,
          products_scope: productScope,
          account_scope: accountScope,
          err: 'Không tìm thấy account.',
        }));
        return true;
      }

      await setProductAccountUsedState(accountId, target, null);
      await syncProductStockFromAutoAccounts(account.product_id);
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: account.product_id,
        products_scope: productScope,
        account_scope: accountScope,
        msg: target ? 'Đã chuyển account sang used.' : 'Đã chuyển account về available.',
      }));
      return true;
    } catch (error) {
      redirectResponse(res, buildAdminDashboardUrl({
        tab: 'accounts',
        product_id: productId,
        products_scope: productScope,
        account_scope: accountScope,
        err: `Cập nhật account thất bại: ${String(error.message || 'unknown')}`,
      }));
      return true;
    }
  }

  if (pathname !== adminDashboardPath || req.method !== 'GET') {
    sendJson(res, 404, { ok: false, error: 'Not found' });
    return true;
  }

  const tab = normalizeDashboardTab(requestUrl.searchParams.get('tab'));
  const statusFilter = normalizeDashboardOrderStatus(requestUrl.searchParams.get('status_filter'));
  const productScope = normalizeDashboardProductScope(requestUrl.searchParams.get('products_scope'));
  const accountScope = normalizeDashboardAccountScope(requestUrl.searchParams.get('account_scope'));
  const reportFilters = normalizeDashboardReportFilters({
    productKeyword: requestUrl.searchParams.get('product_keyword'),
    dateFrom: requestUrl.searchParams.get('date_from'),
    dateTo: requestUrl.searchParams.get('date_to'),
  });
  const infoMessage = String(requestUrl.searchParams.get('msg') || '').trim();
  const errorMessage = String(requestUrl.searchParams.get('err') || '').trim();
  const selectedProductIdRaw = String(requestUrl.searchParams.get('product_id') || '').trim();

  try {
    const products = await loadDashboardProducts(120, tab === 'products' ? productScope : 'all');
    const accountProductIds = products
      .filter((product) => usesAccountInventory(product))
      .map((product) => String(product.id || '').trim())
      .filter(Boolean);
    const selectedProductId = selectedProductIdRaw || accountProductIds[0] || '';
    let orders = [];
    if (tab === 'orders') {
      orders = await loadDashboardOrders(statusFilter, 120);
    }
    const reports = tab === 'reports'
      ? await loadDashboardReports(reportFilters)
      : createEmptyDashboardReports();

    let selectedProduct = null;
    let accountSummary = null;
    if (tab === 'accounts' && selectedProductId) {
      selectedProduct = await loadProductAny(selectedProductId);
      if (selectedProduct && usesAccountInventory(selectedProduct)) {
        accountSummary = await loadAdminProductAccountsSummary(selectedProductId, 60, accountScope);
      }
    }

    const html = renderAdminDashboardPage({
      tab,
      statusFilter,
      productScope,
      accountScope,
      reportFilters,
      infoMessage,
      errorMessage,
      orders,
      products,
      selectedProductId,
      selectedProduct,
      accountSummary,
      reports,
    });
    sendHtml(res, 200, html);
    return true;
  } catch (error) {
    sendHtml(res, 500, renderAdminDashboardPage({
      tab,
      statusFilter,
      productScope,
      accountScope,
      reportFilters,
      infoMessage: '',
      errorMessage: `Lỗi tải dashboard: ${String(error.message || 'unknown')}`,
      orders: [],
      products: [],
      selectedProductId: selectedProductIdRaw,
      selectedProduct: null,
      accountSummary: null,
      reports: createEmptyDashboardReports(),
    }));
    return true;
  }
}

async function findOrderByTransferCode(transferCode) {
  const normalizedCode = String(transferCode || '').trim().toUpperCase();
  if (!normalizedCode) {
    return null;
  }

  const { data, error } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency,payment_method,created_at')
    .in('status', ['draft', 'confirmed', 'paid'])
    .order('created_at', { ascending: false })
    .limit(5000);

  if (error) {
    throw error;
  }

  const rows = data || [];
  for (const row of rows) {
    const currentCode = buildMmobankTransferCode(row.id).toUpperCase();
    const legacyCode = `DH${String(row.id || '').replace(/-/g, '').slice(0, 10).toUpperCase()}`;
    if (currentCode === normalizedCode || legacyCode === normalizedCode) {
      return row;
    }
  }

  return null;
}

async function notifyOrderPaid(orderId, userId, amount, currency) {
  const { data: owner, error: ownerError } = await db
    .from('users')
    .select('telegram_id')
    .eq('id', userId)
    .maybeSingle();

  if (ownerError) {
    throw ownerError;
  }

  if (owner?.telegram_id) {
    try {
      await bot.telegram.sendMessage(
        Number(owner.telegram_id),
        `Don #${orderId} da duoc xac nhan thanh toan.\nSo tien: ${amount} ${currency || 'VND'}`,
      );
    } catch (error) {
      // no-op
    }
  }

  const adminIds = [...runtimeAdminIds].map((id) => Number(id)).filter(Number.isInteger);
  for (const telegramId of adminIds) {
    try {
      await bot.telegram.sendMessage(
        telegramId,
        `MMOBank webhook: don #${orderId} da thanh toan.\nSo tien: ${amount} ${currency || 'VND'}`,
      );
    } catch (error) {
      // no-op
    }
  }
}

async function deliverAutoAccountsAfterPaid(order) {
  if (!order?.id || !order?.user_id) {
    return { deliveredCount: 0, shortageCount: 0 };
  }

  const { data: items, error: itemsError } = await db
    .from('order_items')
    .select('product_id,quantity')
    .eq('order_id', order.id);
  if (itemsError) {
    throw itemsError;
  }

  const orderItems = items || [];
  if (orderItems.length === 0) {
    return { deliveredCount: 0, shortageCount: 0 };
  }

  const productIds = [...new Set(orderItems.map((item) => item.product_id).filter(Boolean))];
  if (productIds.length === 0) {
    return { deliveredCount: 0, shortageCount: 0 };
  }

  const { data: products, error: productsError } = await db
    .from('products')
    .select('id,name,delivery_type')
    .in('id', productIds);
  if (productsError) {
    throw productsError;
  }

  const productMap = new Map((products || []).map((p) => [p.id, p]));
  const sections = [];
  let deliveredCount = 0;
  let shortageCount = 0;

  for (const item of orderItems) {
    const product = productMap.get(item.product_id);
    if (!product || product.delivery_type !== 'auto') {
      continue;
    }

    const quantity = Math.max(1, Number(item.quantity || 1));
    const { data: existingAccounts, error: existingError } = await db
      .from('product_accounts')
      .select('account_data,created_at')
      .eq('product_id', item.product_id)
      .eq('used_order_id', order.id)
      .order('created_at', { ascending: true });
    if (existingError) {
      throw existingError;
    }

    const accounts = (existingAccounts || []).map((row) => row.account_data).filter(Boolean);
    let missing = quantity - accounts.length;

    while (missing > 0) {
      const claimed = await claimAutoAccount(item.product_id, order.id);
      if (!claimed?.account_data) {
        break;
      }
      accounts.push(claimed.account_data);
      missing -= 1;
    }

    if (accounts.length > 0) {
      deliveredCount += accounts.length;
      sections.push({
        productName: product.name || item.product_id,
        accounts,
      });
    }

    if (accounts.length < quantity) {
      shortageCount += quantity - accounts.length;
    }
  }

  if (sections.length === 0) {
    return { deliveredCount: 0, shortageCount };
  }

  await db.from('order_history').insert({
    order_id: order.id,
    changed_by: null,
    status: 'paid',
    comment: `Auto accounts prepared (${deliveredCount} account(s))`,
  });

  const { data: owner, error: ownerError } = await db
    .from('users')
    .select('telegram_id')
    .eq('id', order.user_id)
    .maybeSingle();
  if (ownerError) {
    throw ownerError;
  }

  if (owner?.telegram_id) {
    const text = buildPaidDeliveryMessage(order, sections, shortageCount);
    try {
      await bot.telegram.sendMessage(
        Number(owner.telegram_id),
        text,
        Markup.inlineKeyboard([
          [Markup.button.callback('🧹 Clear trò chuyện', `ordclr:${order.id}`)],
        ]),
      );
      await db.from('order_history').insert({
        order_id: order.id,
        changed_by: null,
        status: 'paid',
        comment: `Auto accounts delivered to user (${deliveredCount} account(s))`,
      });
    } catch (error) {
      const reason = String(error?.message || 'unknown_error').slice(0, 220);
      await db.from('order_history').insert({
        order_id: order.id,
        changed_by: null,
        status: 'paid',
        comment: `Auto account delivery failed (${reason})`,
      });
      const adminIds = [...runtimeAdminIds].map((id) => Number(id)).filter(Number.isInteger);
      for (const telegramId of adminIds) {
        try {
          await bot.telegram.sendMessage(
            telegramId,
            `Canh bao giao tai khoan tu dong that bai\nDon: #${order.id}\nUser: ${order.user_id}\nLy do: ${reason}`,
          );
        } catch (notifyError) {
          // no-op
        }
      }
    }
  } else {
    await db.from('order_history').insert({
      order_id: order.id,
      changed_by: null,
      status: 'paid',
      comment: 'Auto account delivery skipped (missing user telegram_id)',
    });
  }

  return { deliveredCount, shortageCount };
}

async function markOrderPaidFromMmobank(order, event) {
  if (!order) {
    return { ok: false, reason: 'order_not_found' };
  }

  if (order.status === 'paid') {
    clearOrderExpiryTimer(order.id);
    return { ok: true, alreadyPaid: true, order };
  }

  const updatePayload = { status: 'paid' };
  const { data: updated, error: updateError } = await db
    .from('orders')
    .update(updatePayload)
    .eq('id', order.id)
    .select('id,user_id,status,total_amount,currency')
    .single();

  if (updateError) {
    throw updateError;
  }
  clearOrderExpiryTimer(updated.id);

  const txPart = event.transactionId ? `tx:${event.transactionId}` : 'tx:n/a';
  const amountPart = Number.isFinite(Number(event.amount)) ? `amount:${Math.round(Number(event.amount))}` : 'amount:n/a';
  const contentPart = event.content ? `content:${event.content.slice(0, 140)}` : 'content:n/a';

  await db.from('order_history').insert({
    order_id: updated.id,
    changed_by: null,
    status: 'paid',
    comment: `Paid via MMOBank webhook (${txPart}; ${amountPart}; ${contentPart})`,
  });

  await notifyOrderPaid(updated.id, updated.user_id, updated.total_amount, updated.currency || 'VND');
  await deliverAutoAccountsAfterPaid(updated);
  await clearOrderPaymentMessages(updated.id);
  return { ok: true, alreadyPaid: false, order: updated };
}

async function handleMmobankWebhook(req, res, rawBody) {
  const secretHeader = String(req.headers['secret-key'] || '').trim();
  if (mmobankSecretKey && secretHeader !== mmobankSecretKey) {
    sendJson(res, 401, { ok: false, error: 'Unauthorized' });
    return;
  }

  let payload = {};
  try {
    payload = rawBody ? JSON.parse(rawBody) : {};
  } catch (error) {
    sendJson(res, 400, { ok: false, error: 'Invalid JSON' });
    return;
  }

  const events = extractMmobankEvents(payload);
  if (events.length === 0) {
    sendJson(res, 202, { ok: true, ignored: 'missing_payload' });
    return;
  }

  let paidCount = 0;
  let alreadyPaidCount = 0;
  let ignoredCount = 0;
  const paidOrderIds = [];

  for (const event of events) {
    if (mmobankAccountNo && event.accountNo) {
      const configured = String(mmobankAccountNo).trim();
      const receivedAccountNo = String(event.accountNo).trim();
      if (configured && receivedAccountNo && configured !== receivedAccountNo) {
        ignoredCount += 1;
        continue;
      }
    }

    if (event.transferType && event.transferType.includes('out')) {
      ignoredCount += 1;
      continue;
    }

    if (!event.transferCode) {
      ignoredCount += 1;
      continue;
    }

    const order = await findOrderByTransferCode(event.transferCode);
    if (!order) {
      ignoredCount += 1;
      continue;
    }

    if (Number.isFinite(Number(event.amount))) {
      const expected = Math.round(Number(order.total_amount || 0));
      const received = Math.round(Number(event.amount));
      if (received < expected) {
        ignoredCount += 1;
        continue;
      }
    }

    const result = await markOrderPaidFromMmobank(order, event);
    if (result.alreadyPaid) {
      await clearOrderPaymentMessages(order.id);
      alreadyPaidCount += 1;
      continue;
    }

    paidCount += 1;
    paidOrderIds.push(result.order.id);
  }

  if (paidCount === 0 && alreadyPaidCount === 0) {
    sendJson(res, 202, { ok: true, ignored: 'no_matching_transaction', ignoredCount });
    return;
  }

  sendJson(res, 200, {
    ok: true,
    status: paidCount > 0 ? 'paid' : 'already_paid',
    paidCount,
    alreadyPaidCount,
    ignoredCount,
    orderIds: paidOrderIds,
  });
}

function startMmobankWebhookServer() {
  const server = http.createServer(async (req, res) => {
    const rawUrl = String(req.url || '/');
    const requestUrl = new URL(rawUrl, 'http://localhost');
    if (req.method === 'GET' && requestUrl.pathname === '/health') {
      sendJson(res, 200, { ok: true });
      return;
    }

    try {
      const handledByDashboard = await handleAdminDashboardRequest(req, res, requestUrl);
      if (handledByDashboard) {
        return;
      }
    } catch (error) {
      console.error('Admin dashboard error:', error);
      if (!res.headersSent) {
        sendJson(res, 500, { ok: false, error: 'admin_dashboard_error' });
      }
      return;
    }

    if (req.method !== 'POST' || requestUrl.pathname !== mmobankWebhookPath) {
      sendJson(res, 404, { ok: false, error: 'Not found' });
      return;
    }

    try {
      const rawBody = await readRawRequestBody(req);
      await handleMmobankWebhook(req, res, rawBody);
    } catch (error) {
      if (String(error?.message || '').includes('payload_too_large')) {
        sendJson(res, 413, { ok: false, error: 'payload_too_large' });
        return;
      }
      console.error('MMOBank webhook error:', error);
      if (!res.headersSent) {
        sendJson(res, 500, { ok: false, error: 'internal_error' });
      }
    }
  });

  server.listen(webhookPort, () => {
    console.log(`Webhook server listening on :${webhookPort}${mmobankWebhookPath} | admin: ${adminDashboardPath}`);
  });

  return server;
}

async function loadRecentUserOrders(userId) {
  const { data, error } = await db
    .from('orders')
    .select('id,status,total_amount,currency,created_at')
    .eq('user_id', userId)
    .order('created_at', { ascending: false })
    .limit(10);

  if (error) {
    throw error;
  }

  const rows = data || [];
  if (!rows.length) {
    return rows;
  }

  const previewByOrderId = await loadOrderItemPreviewByOrderIds(rows.map((row) => row.id));
  return rows.map((row) => ({
    ...row,
    item_preview: previewByOrderId.get(row.id) || null,
  }));
}

async function loadRecentUserPaidOrders(userId) {
  const { data, error } = await db
    .from('orders')
    .select('id,status,total_amount,currency,created_at')
    .eq('user_id', userId)
    .eq('status', 'paid')
    .order('created_at', { ascending: false })
    .limit(10);

  if (error) {
    throw error;
  }

  const rows = data || [];
  if (!rows.length) {
    return rows;
  }

  const previewByOrderId = await loadOrderItemPreviewByOrderIds(rows.map((row) => row.id));
  return rows.map((row) => ({
    ...row,
    item_preview: previewByOrderId.get(row.id) || null,
  }));
}

async function loadOrderItemPreviewByOrderIds(orderIds) {
  const normalizedIds = [...new Set((orderIds || []).map((id) => String(id || '').trim()).filter(Boolean))];
  if (!normalizedIds.length) {
    return new Map();
  }

  const { data, error } = await db
    .from('order_items')
    .select('order_id,product_id,quantity')
    .in('order_id', normalizedIds)
    .order('created_at', { ascending: true });
  if (error) {
    throw error;
  }

  const rows = data || [];
  const productIds = [...new Set(rows.map((row) => String(row.product_id || '').trim()).filter(Boolean))];
  let productNameById = new Map();
  if (productIds.length) {
    const { data: productRows, error: productError } = await db
      .from('products')
      .select('id,name')
      .in('id', productIds);
    if (productError) {
      throw productError;
    }
    productNameById = new Map((productRows || []).map((row) => [row.id, row.name]));
  }

  const byOrder = new Map();
  for (const row of rows) {
    const orderId = String(row.order_id || '').trim();
    if (!orderId) continue;

    const current = byOrder.get(orderId) || {
      firstProductName: null,
      totalLines: 0,
      totalQuantity: 0,
    };
    const qty = Math.max(0, Number(row.quantity || 0));
    const productName = String(productNameById.get(String(row.product_id || '').trim()) || row.product_id || '').trim();
    if (!current.firstProductName && productName) {
      current.firstProductName = productName;
    }
    current.totalLines += 1;
    current.totalQuantity += qty;
    byOrder.set(orderId, current);
  }

  const missingOrderIds = normalizedIds.filter((id) => !byOrder.has(id));
  if (missingOrderIds.length) {
    const { data: deliveredRows, error: deliveredError } = await db
      .from('product_accounts')
      .select('used_order_id,product_id')
      .in('used_order_id', missingOrderIds)
      .order('created_at', { ascending: true });
    if (deliveredError) {
      throw deliveredError;
    }

    const productIds = [...new Set((deliveredRows || []).map((row) => String(row.product_id || '').trim()).filter(Boolean))];
    let productNameById = new Map();
    if (productIds.length) {
      const { data: products, error: productsError } = await db
        .from('products')
        .select('id,name')
        .in('id', productIds);
      if (productsError) {
        throw productsError;
      }
      productNameById = new Map((products || []).map((row) => [row.id, row.name]));
    }

    for (const row of (deliveredRows || [])) {
      const orderId = String(row.used_order_id || '').trim();
      const productId = String(row.product_id || '').trim();
      if (!orderId || !productId) continue;
      const current = byOrder.get(orderId) || { byProduct: new Map() };
      if (!current.byProduct) {
        current.byProduct = new Map();
      }
      current.byProduct.set(productId, (current.byProduct.get(productId) || 0) + 1);
      byOrder.set(orderId, current);
    }

    for (const orderId of missingOrderIds) {
      const fallback = byOrder.get(orderId);
      if (!fallback?.byProduct || fallback.byProduct.size === 0) {
        continue;
      }
      const productIdsOrdered = [...fallback.byProduct.keys()];
      const firstProductId = productIdsOrdered[0];
      const totalQuantity = [...fallback.byProduct.values()].reduce((sum, qty) => sum + Number(qty || 0), 0);
      byOrder.set(orderId, {
        firstProductName: productNameById.get(firstProductId) || firstProductId,
        totalLines: fallback.byProduct.size,
        totalQuantity,
      });
    }
  }

  return byOrder;
}

async function loadUserPurchaseStats(userId) {
  const { data, error } = await db
    .from('orders')
    .select('total_amount,currency')
    .eq('user_id', userId)
    .eq('status', 'paid')
    .limit(5000);

  if (error) {
    throw error;
  }

  const rows = data || [];
  const totalByCurrency = new Map();
  for (const row of rows) {
    const amount = Number(row.total_amount || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      continue;
    }
    const currency = String(row.currency || 'VND').toUpperCase();
    totalByCurrency.set(currency, (totalByCurrency.get(currency) || 0) + amount);
  }

  return {
    paidOrdersCount: rows.length,
    totalByCurrency,
  };
}

function formatCurrencyTotals(totalByCurrency) {
  const entries = [...(totalByCurrency?.entries?.() || [])];
  if (!entries.length) {
    return '0 VND';
  }
  return entries
    .map(([currency, total]) => `${formatPriceVnd(total)} ${currency}`)
    .join(' + ');
}

async function loadOrderByIdForUser(orderId, userId) {
  const { data, error } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency,payment_method')
    .eq('id', orderId)
    .eq('user_id', userId)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data;
}

async function loadUserOrderDetail(orderId, userId) {
  const { data: order, error: orderError } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency,payment_method,created_at')
    .eq('id', orderId)
    .eq('user_id', userId)
    .maybeSingle();
  if (orderError) {
    throw orderError;
  }
  if (!order) {
    return null;
  }

  const { data: items, error: itemsError } = await db
    .from('order_items')
    .select('product_id,quantity,unit_price,total_price')
    .eq('order_id', orderId)
    .order('created_at', { ascending: true });
  if (itemsError) {
    throw itemsError;
  }

  const itemRows = items || [];
  const productIds = [...new Set(itemRows.map((item) => String(item.product_id || '').trim()).filter(Boolean))];
  let productsById = new Map();
  if (productIds.length) {
    const { data: productRows, error: productError } = await db
      .from('products')
      .select('id,name,delivery_type')
      .in('id', productIds);
    if (productError) {
      throw productError;
    }
    productsById = new Map((productRows || []).map((row) => [row.id, row]));
  }

  const { data: accounts, error: accountsError } = await db
    .from('product_accounts')
    .select('product_id,account_data,created_at')
    .eq('used_order_id', orderId)
    .order('created_at', { ascending: true });
  if (accountsError) {
    throw accountsError;
  }

  let itemList = itemRows.map((item) => {
    const product = productsById.get(String(item.product_id || '').trim());
    return {
      ...item,
      products: {
        name: product?.name || null,
        delivery_type: product?.delivery_type || null,
      },
    };
  });
  const accountRows = accounts || [];

  // Fallback for legacy orders that may have missing order_items rows:
  // infer product lines from delivered product_accounts.
  if (!itemList.length && accountRows.length) {
    const grouped = new Map();
    for (const row of accountRows) {
      const productId = String(row.product_id || '').trim();
      if (!productId) continue;
      grouped.set(productId, (grouped.get(productId) || 0) + 1);
    }

    const productIds = [...grouped.keys()];
    let productsById = new Map();
    if (productIds.length) {
      const { data: productRows, error: productError } = await db
        .from('products')
        .select('id,name,delivery_type')
        .in('id', productIds);
      if (productError) {
        throw productError;
      }
      productsById = new Map((productRows || []).map((row) => [row.id, row]));
    }

    itemList = productIds.map((productId) => {
      const product = productsById.get(productId);
      return {
        product_id: productId,
        quantity: grouped.get(productId) || 0,
        unit_price: null,
        total_price: null,
        products: {
          name: product?.name || productId,
          delivery_type: product?.delivery_type || 'auto',
        },
      };
    });
  }

  return { ...order, items: itemList, accounts: accountRows };
}

async function loadAdminOrderDetail(orderId) {
  const { data: order, error: orderError } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency,payment_method,created_at')
    .eq('id', orderId)
    .maybeSingle();
  if (orderError) {
    throw orderError;
  }
  if (!order) {
    return null;
  }

  const { data: items, error: itemsError } = await db
    .from('order_items')
    .select('product_id,quantity,unit_price,total_price')
    .eq('order_id', orderId)
    .order('created_at', { ascending: true });
  if (itemsError) {
    throw itemsError;
  }

  const itemRows = items || [];
  const productIds = [...new Set(itemRows.map((item) => String(item.product_id || '').trim()).filter(Boolean))];
  let productsById = new Map();
  if (productIds.length) {
    const { data: productRows, error: productError } = await db
      .from('products')
      .select('id,name')
      .in('id', productIds);
    if (productError) {
      throw productError;
    }
    productsById = new Map((productRows || []).map((row) => [row.id, row]));
  }

  const normalizedItems = itemRows.map((item) => {
    const product = productsById.get(String(item.product_id || '').trim());
    return {
      ...item,
      products: {
        name: product?.name || null,
      },
    };
  });

  const enrichedOrder = await attachBuyerToOrder(order);
  return { ...enrichedOrder, items: normalizedItems };
}

async function findAdminOrderByKeyword(keyword) {
  const raw = String(keyword || '').trim();
  if (!raw) {
    return null;
  }

  const normalized = raw.replace(/^#/, '').trim();
  const normalizedLower = normalized.toLowerCase();
  const normalizedUpper = normalized.toUpperCase();
  const tokenUpper = normalizedUpper.startsWith('DH')
    ? normalizedUpper.slice(2)
    : normalizedUpper;

  const { data, error } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency,payment_method,created_at')
    .order('created_at', { ascending: false })
    .limit(5000);
  if (error) {
    throw error;
  }

  const rows = data || [];
  const matched = rows.find((row) => {
    const orderId = String(row.id || '');
    const orderIdLower = orderId.toLowerCase();
    const shortCode = buildMmobankTransferCode(orderId).toUpperCase();
    const supportCode = buildSupportOrderCode(orderId).toUpperCase();
    const legacyCode = `DH${orderId.replace(/-/g, '').slice(0, 10).toUpperCase()}`;

    if (orderIdLower === normalizedLower || orderIdLower.startsWith(normalizedLower)) {
      return true;
    }

    if (normalizedUpper === shortCode || normalizedUpper === supportCode || normalizedUpper === legacyCode) {
      return true;
    }

    if (tokenUpper && shortCode.startsWith(tokenUpper)) {
      return true;
    }

    return false;
  });
  return matched || null;
}

function buildAdminOrderInspectText(order) {
  const supportCode = buildSupportOrderCode(order.id);
  const lines = [
    '━━━━━━━━━━━━━━━━━━━━━━',
    '🔎 CHECK ĐƠN HÀNG',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
    `🆔 Mã đơn: #${order.id}`,
    `🧷 Mã hỗ trợ: ${supportCode || 'N/A'}`,
    `👤 Người mua: ${formatBuyerLabel(order)}`,
    `📌 Trạng thái: ${order.status}`,
    `💳 Thanh toán: ${order.payment_method || 'N/A'}`,
    `💰 Tổng tiền: ${formatPriceVnd(order.total_amount)} ${order.currency || 'VND'}`,
    `🕒 Tạo lúc: ${formatDateTimeVietnam(order.created_at)}`,
    '',
    '📦 Sản phẩm trong đơn:',
  ];

  if (!order.items || !order.items.length) {
    lines.push('- (không có item)');
  } else {
    for (const item of order.items) {
      lines.push(
        `- ${item.products?.name || item.product_id} | SL:${item.quantity} | Đơn giá:${formatPriceVnd(item.unit_price)} | Thành tiền:${formatPriceVnd(item.total_price)}`,
      );
    }
  }

  return lines.join('\n');
}

function buildOrderHistoryPanel(orders, locale) {
  const rows = Array.isArray(orders) ? orders : [];
  const normalizeStatus = (value) => String(value || '').toLowerCase();
  const statusIcon = (status) => {
    if (status === 'paid') {
      return '\uD83D\uDFE2';
    }
    if (status === 'confirmed') {
      return '\uD83D\uDD35';
    }
    if (status === 'cancelled') {
      return '\uD83D\uDD34';
    }
    return '\u26AA';
  };

  const lines = [];
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  lines.push(locale === 'en' ? '\uD83D\uDCCA ORDER HISTORY' : '\uD83D\uDCCA LICH SU DON HANG');
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  lines.push('');
  lines.push(locale === 'en' ? '\uD83D\uDFE2 Paid | \uD83D\uDD35 Waiting | \uD83D\uDD34 Cancelled' : '\uD83D\uDFE2 Đã thanh toán | \uD83D\uDD35 Chờ xử lý | \uD83D\uDD34 Đã hủy');
  lines.push('');
  lines.push('──────────────────────');
  lines.push('');

  let totalAmount = 0;
  for (const order of rows) {
    const amount = Number(order.total_amount || 0);
    totalAmount += Number.isFinite(amount) ? amount : 0;
    const status = normalizeStatus(order.status);
    const icon = statusIcon(status);
    const idText = String(order.id || '').slice(0, 8).toUpperCase();
    const supportCode = buildSupportOrderCode(order.id);
    const currency = order.currency || 'VND';
    lines.push(`${icon} ${supportCode || `DH${idText}`}`);
    const preview = order.item_preview || null;
    if (preview?.firstProductName) {
      const extraLines = Math.max(0, Number(preview.totalLines || 0) - 1);
      const suffix = extraLines > 0
        ? (locale === 'en' ? ` +${extraLines} more` : ` +${extraLines} sản phẩm`)
        : '';
      lines.push(`${locale === 'en' ? '📦 Item' : '📦 Sản phẩm'}: ${preview.firstProductName}${suffix}`);
    }
    lines.push(`\uD83D\uDCB0 ${formatPriceVnd(amount)} ${currency}`);
    lines.push('');
  }

  lines.push('──────────────────────');
  lines.push(locale === 'en' ? 'Tap an order button below to view full details.' : 'Bấm vào nút đơn hàng bên dưới để xem chi tiết đầy đủ.');
  lines.push('');
  lines.push(`${locale === 'en' ? '\uD83D\uDCCC Total orders' : '\uD83D\uDCCC Tổng đơn'}: ${rows.length}`);
  lines.push(`${locale === 'en' ? '\uD83D\uDCB5 Total amount' : '\uD83D\uDCB5 Tổng tiền'}: ${formatPriceVnd(totalAmount)} VND`);
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  return lines.join('\n');
}

function buildOrderHistoryKeyboard(orders, locale) {
  const rows = [];
  const normalizeStatus = (value) => String(value || '').toLowerCase();
  const statusIcon = (status) => {
    if (status === 'paid') {
      return '🟢';
    }
    if (status === 'confirmed') {
      return '🔵';
    }
    if (status === 'cancelled') {
      return '🔴';
    }
    return '⚪';
  };

  for (const order of orders) {
    const status = normalizeStatus(order.status);
    const idText = String(order.id || '').slice(0, 8).toUpperCase();
    const supportCode = buildSupportOrderCode(order.id);
    rows.push([
      Markup.button.callback(
        `${statusIcon(status)} ${supportCode || `DH${idText}`} • ${formatPriceVnd(order.total_amount)} ${order.currency || 'VND'}`,
        `myord:${order.id}`,
      ),
    ]);
  }

  rows.push([
    Markup.button.callback(locale === 'en' ? '🔄 Refresh' : '🔄 Làm mới', 'menu_history'),
    Markup.button.callback(locale === 'en' ? '🗑 Close' : '🗑 Đóng', 'history_close'),
  ]);

  return Markup.inlineKeyboard(rows);
}

function buildUserOrderDetailText(detail, locale) {
  const status = String(detail?.status || '').toLowerCase();
  const isPaid = status === 'paid';
  const supportCode = buildSupportOrderCode(detail?.id);
  const lines = [
    '━━━━━━━━━━━━━━━━━━━━━━',
    locale === 'en' ? '🧾 ORDER DETAIL' : '🧾 CHI TIẾT ĐƠN HÀNG',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
    `🆔 #${detail.id}`,
    `${locale === 'en' ? '🧷 Support code' : '🧷 Mã hỗ trợ'}: ${supportCode || 'N/A'}`,
    `📌 ${locale === 'en' ? 'Status' : 'Trạng thái'}: ${status || '-'}`,
    `💰 ${locale === 'en' ? 'Total' : 'Tổng tiền'}: ${formatPriceVnd(detail.total_amount)} ${detail.currency || 'VND'}`,
    `💳 ${locale === 'en' ? 'Payment' : 'Thanh toán'}: ${detail.payment_method || 'N/A'}`,
    '',
    locale === 'en' ? '📦 Items:' : '📦 Sản phẩm:',
  ];

  const itemList = Array.isArray(detail.items) ? detail.items : [];
  if (!itemList.length) {
    lines.push(locale === 'en' ? '- (no item)' : '- (không có sản phẩm)');
  } else {
    for (const item of itemList) {
      const rawItemTotal = Number(item.total_price);
      const hasItemTotal = Number.isFinite(rawItemTotal) && rawItemTotal > 0;
      const itemTotalText = hasItemTotal
        ? `${formatPriceVnd(rawItemTotal)} ${detail.currency || 'VND'}`
        : (locale === 'en' ? '(see order total above)' : '(xem tổng tiền ở trên)');
      lines.push(
        `- ${item.products?.name || item.product_id} | SL:${item.quantity} | ${itemTotalText}`,
      );
    }
  }

  if (!isPaid) {
    lines.push('');
    lines.push(locale === 'en'
      ? '⏳ Payment not completed yet. Account details appear after paid.'
      : '⏳ Đơn chưa thanh toán. TK/MK sẽ hiện sau khi đơn ở trạng thái đã thanh toán.');
    return lines.join('\n');
  }

  const accountsByProduct = new Map();
  for (const row of (detail.accounts || [])) {
    if (!row?.product_id || !row?.account_data) {
      continue;
    }
    const arr = accountsByProduct.get(row.product_id) || [];
    arr.push(row.account_data);
    accountsByProduct.set(row.product_id, arr);
  }

  lines.push('');
  lines.push(locale === 'en' ? '🔐 Account details (TK | MK | 2FA):' : '🔐 Thông tin tài khoản (TK | MK | 2FA):');

  let hasAnyAccount = false;
  for (const item of itemList) {
    const productName = item.products?.name || item.product_id;
    const deliveryType = String(item.products?.delivery_type || '').toLowerCase();
    lines.push('');
    lines.push(`• ${productName}`);

    if (deliveryType && deliveryType !== 'auto') {
      lines.push(locale === 'en' ? '  - Manual delivery product. Contact admin/Zalo.' : '  - Sản phẩm giao thủ công. Liên hệ admin/Zalo.');
      continue;
    }

    const productAccounts = accountsByProduct.get(item.product_id) || [];
    if (!productAccounts.length) {
      lines.push(locale === 'en' ? '  - No account assigned yet.' : '  - Chưa có tài khoản được cấp.');
      continue;
    }

    hasAnyAccount = true;
    productAccounts.forEach((accountData, index) => {
      const parsed = parseAccountData(accountData);
      lines.push(`  ${index + 1}) ${parsed.account} | ${parsed.password} | ${parsed.twofa}`);
    });
  }

  if (!hasAnyAccount) {
    lines.push('');
    lines.push(locale === 'en'
      ? '⚠️ No account credentials found for this order.'
      : '⚠️ Chưa có dữ liệu TK/MK cho đơn này.');
  }

  return lines.join('\n');
}

function buildUserOrderDetailKeyboard(orderId, locale) {
  return Markup.inlineKeyboard([
    [Markup.button.callback(locale === 'en' ? '🗑 Delete message' : '🗑 Xóa tin nhắn', `ordclr:${orderId}`)],
    [Markup.button.callback(locale === 'en' ? '↩ Back to orders' : '↩ Quay lại đơn hàng', 'menu_history')],
  ]);
}

function resolveSupportContacts(channels) {
  const info = {
    shopName: supportShopName,
    zalo: supportZaloNumber,
    zaloGroup: supportZaloGroup,
    telegram: supportTelegramContact,
  };

  for (const channel of (channels || [])) {
    const value = String(channel?.value || '').trim();
    if (!value) {
      continue;
    }

    const type = String(channel?.type || '').toLowerCase();
    const name = String(channel?.name || '').toLowerCase();
    const isZaloGroup = /zalo\.me\/g\//i.test(value) || /zalo/.test(name) && /(group|nh[oó]m|box)/.test(name);

    if (type === 'phone' || (/zalo/.test(name) && !isZaloGroup)) {
      info.zalo = value;
      continue;
    }

    if (isZaloGroup || (type === 'url' && /zalo\.me\/g\//i.test(value))) {
      info.zaloGroup = value;
      continue;
    }

    if (type === 'telegram' || /^@/.test(value) || /t\.me\//i.test(value)) {
      info.telegram = value;
      continue;
    }

    if (/shop/.test(name)) {
      info.shopName = value;
    }
  }

  return info;
}

function buildSupportPanel(channels, locale) {
  const info = resolveSupportContacts(channels);
  if (locale === 'en') {
    return [
      '💬 SUPPORT',
      '',
      '📱 Contact:',
      `Shop Name: ${info.shopName}`,
      '',
      `📞 Zalo: ${info.zalo || 'N/A'}`,
      `💬 Zalo Group: ${info.zaloGroup || 'N/A'}`,
      `📲 Telegram: ${info.telegram || 'N/A'}`,
      '',
      'Need help? Contact us via any channel above and send your support code (DHxxxxxx).',
    ].join('\n');
  }

  return [
    '💬 HỖ TRỢ',
    '',
    '📱 Liên hệ:',
    `Shop Name: ${info.shopName}`,
    '',
    `📞 Zalo: ${info.zalo || 'Chưa cập nhật'}`,
    `💬 Box Zalo: ${info.zaloGroup || 'Chưa cập nhật'}`,
    `📲 Telegram: ${info.telegram || 'Chưa cập nhật'}`,
    '',
    'Cần hỗ trợ? Gửi cho admin mã hỗ trợ đơn (dạng DHxxxxxx) để kiểm tra nhanh.',
  ].join('\n');
}

async function loadSupportChannels() {
  const { data, error } = await db
    .from('support_channels')
    .select('name,type,value')
    .eq('is_active', true)
    .order('name', { ascending: true })
    .limit(20);

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadAdminOrders() {
  const { data, error } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency,created_at')
    .in('status', ['draft', 'confirmed'])
    .order('created_at', { ascending: false })
    .limit(10);

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadAdminOrderById(orderId) {
  const { data, error } = await db
    .from('orders')
    .select('id,user_id,status,total_amount,currency,created_at')
    .eq('id', orderId)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return attachBuyerToOrder(data);
}

async function loadUsersByIds(userIds) {
  const ids = [...new Set((userIds || []).map((id) => String(id || '').trim()).filter(Boolean))];
  if (!ids.length) {
    return new Map();
  }

  const { data, error } = await db
    .from('users')
    .select('id,telegram_id,username,display_name')
    .in('id', ids);
  if (error) {
    throw error;
  }

  return new Map((data || []).map((row) => [row.id, row]));
}

async function attachBuyerToOrder(order) {
  if (!order) {
    return order;
  }
  const usersById = await loadUsersByIds([order.user_id]);
  return {
    ...order,
    buyer: usersById.get(String(order.user_id || '').trim()) || null,
  };
}

async function attachBuyerToOrders(orders) {
  const rows = Array.isArray(orders) ? orders : [];
  if (!rows.length) {
    return [];
  }
  const usersById = await loadUsersByIds(rows.map((row) => row.user_id));
  return rows.map((row) => ({
    ...row,
    buyer: usersById.get(String(row.user_id || '').trim()) || null,
  }));
}

function formatBuyerLabel(order) {
  const buyer = order?.buyer || null;
  const displayName = String(buyer?.display_name || '').trim();
  const username = String(buyer?.username || '').trim();
  const telegramId = buyer?.telegram_id == null ? '' : String(buyer.telegram_id).trim();

  const parts = [];
  if (displayName) {
    parts.push(displayName);
  }
  if (username) {
    parts.push(`@${username}`);
  }
  if (telegramId) {
    parts.push(`tele:${telegramId}`);
  }
  if (!parts.length) {
    return 'tele:(khong ro)';
  }
  return parts.join(' | ');
}

function buildAdminOrdersText(orders) {
  if (!orders.length) {
    return '📦 Không có đơn chờ xử lý.';
  }

  const lines = [
    '━━━━━━━━━━━━━━━━━━━━━━',
    '📦 ĐƠN CHỜ XỬ LÝ',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
  ];

  for (const order of orders) {
    lines.push(
      `#${order.id.slice(0, 8)} | ${formatBuyerLabel(order)} | ${formatPriceVnd(order.total_amount)} ${order.currency || 'VND'} | ${order.status}`,
    );
  }

  lines.push('');
  lines.push('Bấm vào đơn tương ứng để xử lý trạng thái.');
  return lines.join('\n');
}

function buildAdminOrdersKeyboard(orders) {
  const rows = [];
  for (const order of orders) {
    rows.push([
      Markup.button.callback(
        `#${String(order.id || '').slice(0, 8)} • ${formatPriceVnd(order.total_amount)} ${(order.currency || 'VND')}`,
        `admord:${order.id}`,
      ),
    ]);
  }
  rows.push([
    Markup.button.callback('🔄 Làm mới', 'admin_orders_new'),
    Markup.button.callback('🏠 Admin', 'menu_admin'),
  ]);
  return Markup.inlineKeyboard(rows);
}

function adminOrderDetailText(order) {
  return [
    '━━━━━━━━━━━━━━━━━━━━━━',
    '🧾 CHI TIẾT ĐƠN',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
    `🆔 Mã đơn: #${order.id}`,
    `👤 Người mua: ${formatBuyerLabel(order)}`,
    `📌 Trạng thái: ${order.status}`,
    `💰 Tổng tiền: ${formatPriceVnd(order.total_amount)} ${order.currency || 'VND'}`,
  ].join('\n');
}

function orderActionKeyboard(orderId, currentStatus) {
  const buttons = [];

  if (currentStatus !== 'confirmed') {
    buttons.push(Markup.button.callback('✅ Xác nhận', `ordst:${orderId}:confirmed`));
  }
  if (currentStatus !== 'paid') {
    buttons.push(Markup.button.callback('💸 Đã thanh toán', `ordst:${orderId}:paid`));
  }
  if (currentStatus !== 'cancelled') {
    buttons.push(Markup.button.callback('❌ Hủy', `ordst:${orderId}:cancelled`));
  }

  return Markup.inlineKeyboard([
    buttons.slice(0, 3),
    [Markup.button.callback('↩ Danh sách đơn', 'admin_orders_new')],
  ]);
}

async function loadAdminProducts() {
  const { data, error } = await db
    .from('products')
    .select('id,name,description,price,currency,stock_quantity,is_active,updated_at,delivery_type')
    .order('updated_at', { ascending: false })
    .limit(40);

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadAdminProductAccountsSummary(productId, previewLimit = 20, scope = 'all') {
  const safeScope = normalizeDashboardAccountScope(scope);
  const previewQuery = db
    .from('product_accounts')
    .select('id,account_data,is_used,used_order_id,created_at,used_at')
    .eq('product_id', productId)
    .order('created_at', { ascending: true })
    .limit(previewLimit);
  if (safeScope === 'use') {
    previewQuery.eq('is_used', false);
  } else if (safeScope === 'used') {
    previewQuery.eq('is_used', true);
  }

  const [availableResp, usedResp, previewResp] = await Promise.all([
    db.from('product_accounts').select('id', { count: 'exact', head: true }).eq('product_id', productId).eq('is_used', false),
    db.from('product_accounts').select('id', { count: 'exact', head: true }).eq('product_id', productId).eq('is_used', true),
    previewQuery,
  ]);

  if (availableResp.error) throw availableResp.error;
  if (usedResp.error) throw usedResp.error;
  if (previewResp.error) throw previewResp.error;

  return {
    available: availableResp.count || 0,
    used: usedResp.count || 0,
    scope: safeScope,
    preview: previewResp.data || [],
  };
}

function buildAdminProductAccountsText(product, summary) {
  const lines = [
    '━━━━━━━━━━━━━━━━━━━━━━',
    '📚 KHO KEY/ACCOUNT',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
    `Sản phẩm: ${product.name}`,
    `✅ Còn sẵn: ${summary.available}`,
    `🗂 Đã dùng: ${summary.used}`,
    `📦 Tồn trên products: ${product.stock_quantity ?? 0}`,
    '',
    `Preview ${summary.preview.length} dòng đầu:`,
    '',
  ];

  if (!summary.preview.length) {
    lines.push('(Chưa có tài khoản trong kho)');
    return lines.join('\n');
  }

  for (let i = 0; i < summary.preview.length; i += 1) {
    const row = summary.preview[i];
    const parsed = parseAccountData(row.account_data);
    const status = row.is_used ? '🔴 used' : '🟢 available';
    const idShort = String(row.id || '').slice(0, 8);
    const orderPart = row.used_order_id ? ` | order:${String(row.used_order_id).slice(0, 8)}` : '';
    lines.push(`${i + 1}. #${idShort} | ${status}${orderPart}`);
    lines.push(`   TK: ${parsed.account}`);
    lines.push(`   MK: ${parsed.password}`);
    lines.push(`   2FA: ${parsed.twofa}`);
    lines.push('');
  }

  return lines.join('\n');
}

function buildAdminProductAccountsKeyboard(productId, summary) {
  const rows = [
    [
      Markup.button.callback('🔄 Làm mới', `admaccounts:${productId}`),
      Markup.button.callback('↩ Chi tiết SP', `admprd:${productId}`),
    ],
  ];

  for (const row of (summary.preview || []).slice(0, 5)) {
    const idShort = String(row.id || '').slice(0, 8);
    if (row.is_used) {
      rows.push([
        Markup.button.callback(`♻️ Unuse #${idShort}`, `admaccstate:${row.id}:0`),
      ]);
    } else {
      rows.push([
        Markup.button.callback(`✅ Use #${idShort}`, `admaccstate:${row.id}:1`),
        Markup.button.callback(`🗑 Del #${idShort}`, `admaccdel:${row.id}`),
      ]);
    }
  }

  return Markup.inlineKeyboard(rows);
}

async function loadProductAccountById(accountId) {
  const { data, error } = await db
    .from('product_accounts')
    .select('id,product_id,is_used,account_data')
    .eq('id', accountId)
    .maybeSingle();
  if (error) {
    throw error;
  }
  return data;
}

async function deleteProductAccountById(accountId) {
  const { error } = await db
    .from('product_accounts')
    .delete()
    .eq('id', accountId);
  if (error) {
    throw error;
  }
}

async function setProductAccountUsedState(accountId, isUsed, orderId = null) {
  const patch = isUsed
    ? { is_used: true, used_order_id: orderId || null, used_at: new Date().toISOString() }
    : { is_used: false, used_order_id: null, used_at: null };

  const { error } = await db
    .from('product_accounts')
    .update(patch)
    .eq('id', accountId);
  if (error) {
    throw error;
  }
}

async function updateAdminProduct(productId, patch) {
  const { data, error } = await db
    .from('products')
    .update(patch)
    .eq('id', productId)
    .select('id,name,price,currency,stock_quantity,is_active')
    .single();

  if (error) {
    throw error;
  }

  return data;
}

async function hardDeleteProduct(productId) {
  const { error } = await db
    .from('products')
    .delete()
    .eq('id', productId);

  if (error) {
    throw error;
  }
}

async function updateProductAccountDataById(accountId, accountData) {
  const payload = String(accountData || '').trim();
  if (!payload) {
    throw new Error('account_data_empty');
  }

  const { error } = await db
    .from('product_accounts')
    .update({ account_data: payload })
    .eq('id', accountId);
  if (error) {
    throw error;
  }
}

async function countOrderItemsByProductId(productId) {
  const { count, error } = await db
    .from('order_items')
    .select('id', { count: 'exact', head: true })
    .eq('product_id', productId);
  if (error) {
    throw error;
  }
  return count || 0;
}

async function loadReport() {
  const [
    totalOrdersResp,
    paidOrdersResp,
    confirmedOrdersResp,
    revenueResp,
  ] = await Promise.all([
    db.from('orders').select('id', { count: 'exact', head: true }),
    db.from('orders').select('id', { count: 'exact', head: true }).eq('status', 'paid'),
    db.from('orders').select('id', { count: 'exact', head: true }).eq('status', 'confirmed'),
    db.from('orders').select('total_amount').eq('status', 'paid').limit(5000),
  ]);

  if (totalOrdersResp.error) throw totalOrdersResp.error;
  if (paidOrdersResp.error) throw paidOrdersResp.error;
  if (confirmedOrdersResp.error) throw confirmedOrdersResp.error;
  if (revenueResp.error) throw revenueResp.error;

  const revenue = (revenueResp.data || []).reduce((sum, row) => sum + Number(row.total_amount || 0), 0);

  return {
    totalOrders: totalOrdersResp.count || 0,
    paidOrders: paidOrdersResp.count || 0,
    confirmedOrders: confirmedOrdersResp.count || 0,
    revenue,
  };
}

async function loadAdminSnapshot() {
  try {
    const [openOrdersResp, activeProductsResp, lowStockResp] = await Promise.all([
      db.from('orders').select('id', { count: 'exact', head: true }).in('status', ['draft', 'confirmed']),
      db.from('products').select('id', { count: 'exact', head: true }).eq('is_active', true),
      db.from('products').select('id', { count: 'exact', head: true }).lte('stock_quantity', 3).eq('is_active', true),
    ]);

    if (openOrdersResp.error) throw openOrdersResp.error;
    if (activeProductsResp.error) throw activeProductsResp.error;
    if (lowStockResp.error) throw lowStockResp.error;

    return {
      openOrders: openOrdersResp.count || 0,
      activeProducts: activeProductsResp.count || 0,
      lowStockProducts: lowStockResp.count || 0,
    };
  } catch (error) {
    console.error('loadAdminSnapshot failed:', error);
    return {
      openOrders: 0,
      activeProducts: 0,
      lowStockProducts: 0,
    };
  }
}

function buildAdminMainText(snapshot) {
  return [
    '━━━━━━━━━━━━━━━━━━━━━━',
    '🛠 ADMIN DASHBOARD',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
    `📦 Đơn chờ xử lý: ${snapshot.openOrders}`,
    `🛍 Sản phẩm đang bán: ${snapshot.activeProducts}`,
    `⚠️ Sản phẩm sắp hết (<=3): ${snapshot.lowStockProducts}`,
    '',
    'Chọn tác vụ bên dưới để quản lý nhanh.',
  ].join('\n');
}

async function sendAdminMainPanel(ctx, shouldEdit = false) {
  try {
    const snapshot = await loadAdminSnapshot();
    const text = buildAdminMainText(snapshot);
    const keyboard = buildAdminMainKeyboard();

    if (shouldEdit) {
      await replaceOrReply(ctx, text, keyboard);
      return;
    }

    await safeReply(ctx, text, keyboard);
  } catch (error) {
    console.error('sendAdminMainPanel failed:', error);
    const fallbackText = [
      '━━━━━━━━━━━━━━━━━━━━━━',
      '🛠 ADMIN DASHBOARD',
      '━━━━━━━━━━━━━━━━━━━━━━',
      '',
      'Không tải được thống kê nhanh.',
      'Bạn vẫn có thể dùng các chức năng quản trị bên dưới.',
    ].join('\n');
    const keyboard = buildAdminMainKeyboard();

    if (shouldEdit) {
      await replaceOrReply(ctx, fallbackText, keyboard);
      return;
    }

    await safeReply(ctx, fallbackText, keyboard);
  }
}

async function safeReply(ctx, text, extra) {
  try {
    await ctx.reply(text, extra);
  } catch (error) {
    // no-op
  }
}

async function safeAnswerCbQuery(ctx, text, extra) {
  try {
    await ctx.answerCbQuery(text, extra);
  } catch (error) {
    // no-op
  }
}

async function replaceOrReply(ctx, text, extra) {
  try {
    await ctx.editMessageText(text, extra);
    return;
  } catch (error) {
    await safeReply(ctx, text, extra);
  }
}

async function replaceCaptionOrReply(ctx, caption, extra) {
  try {
    await ctx.editMessageCaption(caption, extra);
    return;
  } catch (error) {
    await safeReply(ctx, caption, extra);
  }
}

function parsePositiveMoney(input) {
  const normalized = String(input || '').trim().replace(/[,\s]/g, '');
  if (!/^\d+(\.\d+)?$/.test(normalized)) {
    return null;
  }

  const n = Number(normalized);
  if (!Number.isFinite(n) || n < 0) {
    return null;
  }

  return n;
}

function parseNonNegativeInt(input) {
  const normalized = String(input || '').trim();
  if (!/^\d+$/.test(normalized)) {
    return null;
  }
  return Number(normalized);
}

function formatPriceVnd(value) {
  const n = Number(value || 0);
  return n.toLocaleString('vi-VN');
}

function formatDateTimeVietnam(value) {
  if (!value) {
    return 'N/A';
  }

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  try {
    return new Intl.DateTimeFormat('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(date);
  } catch (error) {
    return date.toISOString();
  }
}

function buildMmobankTransferCode(orderId) {
  const compact = String(orderId || '').trim().split('-')[0].replace(/[^a-zA-Z0-9]/g, '');
  return compact.slice(0, 8).toUpperCase();
}

function buildSupportOrderCode(orderId) {
  const short = buildMmobankTransferCode(orderId);
  if (!short) {
    return '';
  }
  return `DH${short}`;
}

function extractTransferCodeFromText(text) {
  const normalized = String(text || '').toUpperCase();
  const oldStyle = normalized.match(/DH[A-Z0-9]{4,20}/);
  if (oldStyle) {
    return oldStyle[0];
  }
  const shortStyle = normalized.match(/\b[A-F0-9]{8}\b/);
  return shortStyle ? shortStyle[0] : null;
}

function toObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value;
}

function pickFirstString(values) {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }
  return '';
}

function pickFirstNumber(values) {
  for (const value of values) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === 'string') {
      const normalized = value.trim().replace(/[,\s]/g, '');
      if (!normalized) {
        continue;
      }
      const parsed = Number(normalized);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return null;
}

function extractMmobankEvent(payload) {
  const root = toObject(payload);
  const nestedData = toObject(root.data);
  const nestedTransfer = toObject(root.transfer);
  const nestedTransaction = toObject(root.transaction);

  const content = pickFirstString([
    root.content,
    root.description,
    root.transferContent,
    root.transfer_content,
    root.memo,
    root.addInfo,
    nestedData.content,
    nestedData.description,
    nestedData.transferContent,
    nestedData.transfer_content,
    nestedData.memo,
    nestedData.addInfo,
    nestedTransfer.content,
    nestedTransfer.description,
    nestedTransfer.transferContent,
    nestedTransaction.content,
    nestedTransaction.description,
  ]);

  const amount = pickFirstNumber([
    root.transferAmount,
    root.transfer_amount,
    root.amount,
    root.creditAmount,
    nestedData.transferAmount,
    nestedData.transfer_amount,
    nestedData.amount,
    nestedData.creditAmount,
    nestedTransfer.transferAmount,
    nestedTransfer.amount,
    nestedTransaction.transferAmount,
    nestedTransaction.amount,
  ]);

  const transactionId = pickFirstString([
    root.id,
    root.transactionId,
    root.transaction_id,
    root.reference,
    root.referenceCode,
    nestedData.id,
    nestedData.transactionId,
    nestedData.transaction_id,
    nestedTransfer.id,
    nestedTransfer.transactionId,
    nestedTransaction.id,
    nestedTransaction.transactionId,
  ]);

  const transferType = pickFirstString([
    root.transferType,
    root.type,
    root.transactionType,
    nestedData.transferType,
    nestedData.type,
    nestedData.transactionType,
    nestedTransfer.transferType,
    nestedTransaction.transferType,
    nestedTransaction.type,
  ]).toLowerCase();

  const accountNo = pickFirstString([
    root.accountNumber,
    root.accountNo,
    root.account_number,
    nestedData.accountNumber,
    nestedData.accountNo,
    nestedData.account_number,
    nestedTransfer.accountNumber,
    nestedTransfer.accountNo,
    nestedTransaction.accountNumber,
    nestedTransaction.accountNo,
  ]);

  const transferCode = extractTransferCodeFromText(content);
  return {
    transferCode,
    content,
    amount,
    transactionId,
    transferType,
    accountNo,
  };
}

function extractMmobankEvents(payload) {
  const root = toObject(payload);
  if (Array.isArray(root.payload)) {
    return root.payload.map((item) => extractMmobankEvent(item));
  }
  if (root.payload && typeof root.payload === 'object') {
    return [extractMmobankEvent(root.payload)];
  }
  const event = extractMmobankEvent(root);
  if (!event.transferCode && !event.content && !event.transactionId && !Number.isFinite(Number(event.amount))) {
    return [];
  }
  return [event];
}

function buildVietQrUrl({ bankCode, accountNo, accountName, amount, transferContent }) {
  if (!bankCode || !accountNo) {
    return null;
  }

  const base = `https://img.vietqr.io/image/${encodeURIComponent(bankCode)}-${encodeURIComponent(accountNo)}-compact2.png`;
  const params = new URLSearchParams();
  if (Number.isFinite(Number(amount)) && Number(amount) > 0) {
    params.set('amount', String(Math.round(Number(amount))));
  }
  if (transferContent) {
    params.set('addInfo', transferContent);
  }
  if (accountName) {
    params.set('accountName', accountName);
  }

  return `${base}?${params.toString()}`;
}

function buildMmobankInstruction(order) {
  const transferContent = buildMmobankTransferCode(order.id);
  const amount = Number(order.total_amount || 0);
  const qrUrl = buildVietQrUrl({
    bankCode: mmobankBankCode,
    accountNo: mmobankAccountNo,
    accountName: mmobankAccountName,
    amount,
    transferContent,
  });

  const lines = [
    '━━━━━━━━━━━━━━━━━━',
    '🏦 THÔNG TIN THANH TOÁN',
    '━━━━━━━━━━━━━━━━━━',
    `Ngân hàng: ${mmobankBankCode || '(chưa cấu hình)'}`,
    `Số tài khoản: ${mmobankAccountNo || '(chưa cấu hình)'}`,
    `Chủ TK: ${mmobankAccountName || '(không bắt buộc)'}`,
    `Số tiền: ${formatPriceVnd(amount)} ${order.currency || 'VND'}`,
    `Nội dung CK: ${transferContent.toLowerCase()}`,
    `⏱ Tự hủy sau ${paymentTimeoutSeconds}s nếu chưa thanh toán`,
    '━━━━━━━━━━━━━━━━━━',
  ];

  if (!mmobankAccountNo) {
    lines.push('Lưu ý: Chưa cấu hình MMOBANK_ACCOUNT_NO trong .env.');
  } else if (!mmobankBankCode) {
    lines.push('Lưu ý: Chưa cấu hình MMOBANK_BANK_CODE nên không tạo được QR.');
  }

  return { text: lines.join('\n'), qrUrl, transferContent };
}

function buildOrderCreatedMessage(order, product, quantity, unitPrice) {
  const transferContent = String(order?.id || '').trim().split('-')[0] || buildMmobankTransferCode(order?.id);
  const supportCode = buildSupportOrderCode(order?.id);
  const currency = order?.currency || 'VND';
  const total = Number(order?.total_amount || 0);
  const qty = Number(quantity || 0);
  const unit = Number(unitPrice || 0);
  const isManual = product?.delivery_type === 'manual';
  const manualNote = String(product?.manual_contact_note || '').trim()
    || `Sản phẩm giao thủ công. Liên hệ Zalo ${supportZaloNumber} để nhận tài khoản.`;

  const lines = [
    '━━━━━━━━━━━━━━━━━━',
    '✅ ĐƠN HÀNG ĐÃ ĐƯỢC TẠO',
    '━━━━━━━━━━━━━━━━━━',
    '',
    '🆔 Order ID:',
    `#${order.id}`,
    `🧷 Mã hỗ trợ: ${supportCode || '(N/A)'}`,
    '',
    `📦 Số lượng: ${qty}`,
    `💵 Giá: ${formatPriceVnd(unit)} ${currency}`,
    `💰 Tổng tiền: ${formatPriceVnd(total)} ${currency}`,
    '',
    '💳 Vui lòng chuyển khoản đúng nội dung:',
    transferContent.toLowerCase(),
    '',
  ];

  if (isManual) {
    lines.push('📌 Sản phẩm giao thủ công.');
    lines.push(`📱 Liên hệ Zalo: ${supportZaloNumber}`);
    lines.push(manualNote);
  } else {
    lines.push('🤖 Sau khi hệ thống xác nhận thanh toán,');
    lines.push('tài khoản sẽ được gửi tự động trong vài giây.');
  }

  lines.push('');
  lines.push('━━━━━━━━━━━━━━━━━━');
  lines.push('Cảm ơn bạn đã tin tưởng!');
  return lines.join('\n');
}

const PRODUCT_CATEGORY_KEYS = ['all', 'code', 'account', 'support'];

function normalizeProductCategoryKey(rawValue) {
  const key = String(rawValue || 'all').toLowerCase();
  return PRODUCT_CATEGORY_KEYS.includes(key) ? key : 'all';
}

function inferProductCategoryKey(product) {
  const mergedText = `${String(product?.name || '')} ${String(product?.description || '')}`.toLowerCase();
  const deliveryType = String(product?.delivery_type || '').toLowerCase();
  const explicitType = mergedText.match(/\[type:(code|account|support)\]/);
  if (explicitType?.[1]) {
    return explicitType[1];
  }
  const explicitLegacyKey = mergedText.match(/\[type:key\]/);
  if (explicitLegacyKey) {
    return 'code';
  }

  if (
    mergedText.includes('code')
    || mergedText.includes('key')
    || mergedText.includes('kích hoạt')
    || mergedText.includes('kich hoat')
    || mergedText.includes('license')
    || mergedText.includes('credit')
  ) {
    return 'code';
  }

  if (
    deliveryType === 'manual'
    || mergedText.includes('support')
    || mergedText.includes('gia hạn')
    || mergedText.includes('gia han')
    || mergedText.includes('nâng')
    || mergedText.includes('nang cap')
    || mergedText.includes('liên hệ')
    || mergedText.includes('lien he')
  ) {
    return 'support';
  }

  return 'account';
}

function productCategoryMeta(categoryKey, locale) {
  const key = normalizeProductCategoryKey(categoryKey);
  const en = locale === 'en';
  if (key === 'code') {
    return { key, icon: '🔑', label: en ? 'Code' : 'Code' };
  }
  if (key === 'account') {
    return { key, icon: '👤', label: en ? 'Account' : 'Account' };
  }
  if (key === 'support') {
    return { key, icon: '💬', label: en ? 'Support' : 'Support' };
  }
  return { key: 'all', icon: '🧰', label: en ? 'All' : 'Tất cả' };
}

function usesAccountInventory(product) {
  const category = inferProductCategoryKey(product);
  return category === 'code' || category === 'account';
}

function filterProductsByCategory(products, categoryKey) {
  const selected = normalizeProductCategoryKey(categoryKey);
  if (selected === 'all') {
    return products;
  }
  return products.filter((product) => inferProductCategoryKey(product) === selected);
}

function compactProductButtonLabel(product) {
  const maxNameLength = 34;
  const rawName = String(product.name || '').trim();
  const shortName = rawName.length > maxNameLength ? `${rawName.slice(0, maxNameLength - 1)}...` : rawName;
  const priceText = `${formatPriceVnd(product.price)}${product.currency === 'VND' ? 'đ' : ` ${product.currency || 'VND'}`}`;
  const stockText = Number.isFinite(Number(product.stock_quantity)) ? Number(product.stock_quantity) : 0;
  const categoryKey = inferProductCategoryKey(product);
  const category = productCategoryMeta(categoryKey, 'vi');
  if (categoryKey === 'support') {
    return `${category.icon} ${shortName} • ${priceText} • Liên hệ`;
  }
  return `${category.icon} ${shortName} • ${priceText} • 📦 ${stockText}`;
}

function formatDong(value) {
  return `${formatPriceVnd(value)}\u0111`;
}

function calcUnitPriceByQuantity(basePrice, quantity) {
  void quantity;
  return Math.round(Number(basePrice || 0));
}

function buildProductDetailPanel(locale, product) {
  const box = '\uD83D\uDCE6';
  const money = '\uD83D\uDCB0';
  const stockText = Number.isFinite(Number(product.stock_quantity)) ? Number(product.stock_quantity) : '-';

  if (locale === 'en') {
    return [
      '━━━━━━━━━━━━━━━━━━━━━━',
      '🧩 PRODUCT DETAIL',
      '━━━━━━━━━━━━━━━━━━━━━━',
      `${box} ${product.name}`,
      `${money} Price: ${formatDong(product.price)}`,
      `${box} Stock: ${stockText}`,
      '',
      'Choose quantity and continue payment.',
    ].join('\n');
  }

  return [
    '━━━━━━━━━━━━━━━━━━━━━━',
    '🧩 CHI TIẾT SẢN PHẨM',
    '━━━━━━━━━━━━━━━━━━━━━━',
    `${box} ${product.name}`,
    `${money} Gi\u00e1: ${formatDong(product.price)}`,
    `${box} C\u00f2n: ${stockText}`,
    '',
    'Chọn số lượng rồi tiếp tục thanh toán.',
  ].join('\n');
}

function buildCataloguePrompt(locale, categoryKey = 'all', shownCount = 0, totalCount = 0) {
  const selectedMeta = productCategoryMeta(categoryKey, locale);
  if (locale === 'en') {
    return [
      '🛍 Choose Category',
      '',
      '📦 Product type:',
      '• 🔑 [Code]',
      '↳ Activation key / credit',
      '• 👤 [Account]',
      '↳ Account + password + 2FA (optional)',
      '• 💬 [Support]',
      '↳ Contact support service',
      '',
      `🎯 Filter: ${selectedMeta.icon} ${selectedMeta.label} (${shownCount}/${totalCount})`,
      '',
      'Choose a category to view packages 👇',
    ].join('\n');
  }

  return [
    '🛍 Chọn danh mục',
    '',
    '📦 Loại hàng:',
    '• 🔑 [Code]',
    '↳ Mã kích hoạt',
    '• 👤 [Account]',
    '↳ Tài khoản + mật khẩu + 2FA (Tùy chọn)',
    '• 💬 [Support]',
    '↳ Hỗ trợ liên hệ',
    '',
    `🎯 Đang lọc: ${selectedMeta.icon} ${selectedMeta.label} (${shownCount}/${totalCount})`,
    '',
    'Chọn một danh mục để xem gói 👇',
  ].join('\n');
}

function buildCatalogueKeyboard(products, locale, categoryKey = 'all') {
  const selected = normalizeProductCategoryKey(categoryKey);
  const refreshLabel = locale === 'en' ? '🔄 Refresh' : '🔄 Làm mới';
  const closeLabel = locale === 'en' ? '🗑 Close' : '🗑 Đóng';
  const rows = [];

  const groupKeys = ['code', 'account', 'support'];
  rows.push(groupKeys.map((key) => {
    const meta = productCategoryMeta(key, locale);
    const selectedMark = selected === key ? '✅ ' : '';
    return Markup.button.callback(`${selectedMark}${meta.icon} ${meta.label}`, `cat:${key}`);
  }));
  {
    const allMeta = productCategoryMeta('all', locale);
    const selectedMark = selected === 'all' ? '✅ ' : '';
    rows.push([Markup.button.callback(`${selectedMark}${allMeta.icon} ${allMeta.label}`, 'cat:all')]);
  }

  const filteredProducts = filterProductsByCategory(products, selected);
  if (filteredProducts.length === 0) {
    rows.push([Markup.button.callback(locale === 'en' ? 'No package in this category' : 'Không có gói trong danh mục', 'noop')]);
  } else {
    for (const product of filteredProducts) {
      rows.push([Markup.button.callback(compactProductButtonLabel(product), `prd:${product.id}`)]);
    }
  }

  rows.push([
    Markup.button.callback(refreshLabel, `catrf:${selected}`),
    Markup.button.callback(closeLabel, 'catalogue_close'),
  ]);
  return Markup.inlineKeyboard(rows);
}

async function sendCataloguePanel(ctx, locale, options = {}) {
  const normalizedOptions = typeof options === 'boolean'
    ? { shouldEdit: options, categoryKey: 'all' }
    : (options || {});
  const shouldEdit = Boolean(normalizedOptions.shouldEdit);
  const categoryKey = normalizeProductCategoryKey(normalizedOptions.categoryKey || 'all');
  const products = await loadActiveProducts();
  if (products.length === 0) {
    if (shouldEdit) {
      try {
        await ctx.editMessageText(t(locale, 'emptyCatalogue'));
      } catch (error) {
        await safeReply(ctx, t(locale, 'emptyCatalogue'));
      }
      return;
    }

    await ctx.reply(t(locale, 'emptyCatalogue'));
    return;
  }

  const filteredProducts = filterProductsByCategory(products, categoryKey);
  const text = buildCataloguePrompt(locale, categoryKey, filteredProducts.length, products.length);
  const keyboard = buildCatalogueKeyboard(products, locale, categoryKey);

  if (shouldEdit) {
    try {
      await ctx.editMessageText(text, keyboard);
      return;
    } catch (error) {
      await safeReply(ctx, text, keyboard);
      return;
    }
  }

  await ctx.reply(text, keyboard);
}

async function processPurchase(ctx, user, locale, product, quantity = 1) {
  const qty = Number(quantity);
  if (!Number.isInteger(qty) || qty <= 0) {
    await safeReply(ctx, locale === 'en' ? 'Invalid quantity.' : 'Số lượng không hợp lệ.');
    return;
  }

  if (!usesAccountInventory(product) && typeof product.stock_quantity === 'number' && product.stock_quantity < qty) {
    await safeReply(ctx, t(locale, 'outOfStock'));
    return;
  }

  let order;
  try {
    order = await createSingleItemOrder(user.id, product, qty);
  } catch (error) {
    const reason = String(error?.message || '').toLowerCase();
    if (reason.includes('out_of_stock') || reason.includes('insufficient_stock')) {
      await safeReply(ctx, t(locale, 'outOfStock'));
      return;
    }
    throw error;
  }

  scheduleOrderExpiry(order.id);
  const unitPrice = calcUnitPriceByQuantity(product.price, qty);
  const createdMessage = await ctx.reply(buildOrderCreatedMessage(order, product, qty, unitPrice));
  if (createdMessage?.chat?.id && Number.isInteger(createdMessage.message_id)) {
    await rememberOrderPaymentMessageRef(order.id, createdMessage.chat.id, createdMessage.message_id, 'created');
  }

  const mmobank = buildMmobankInstruction(order);
  let paymentMessage = null;
  if (mmobank.qrUrl) {
    try {
      paymentMessage = await ctx.replyWithPhoto(mmobank.qrUrl, {
        caption: mmobank.text,
        ...Markup.inlineKeyboard([
          [Markup.button.callback('Tôi đã chuyển khoản', `paydone:${order.id}`)],
          [Markup.button.callback('Hủy đơn', `paycancel:${order.id}`)],
        ]),
      });
    } catch (error) {
      paymentMessage = await ctx.reply(
        mmobank.text,
        Markup.inlineKeyboard([
          [Markup.button.callback('Tôi đã chuyển khoản', `paydone:${order.id}`)],
          [Markup.button.callback('Hủy đơn', `paycancel:${order.id}`)],
        ]),
      );
    }
  } else {
    paymentMessage = await ctx.reply(
      mmobank.text,
      Markup.inlineKeyboard([
        [Markup.button.callback('Tôi đã chuyển khoản', `paydone:${order.id}`)],
        [Markup.button.callback('Hủy đơn', `paycancel:${order.id}`)],
      ]),
    );
  }

  if (paymentMessage?.chat?.id && Number.isInteger(paymentMessage.message_id)) {
    await rememberOrderPaymentMessageRef(order.id, paymentMessage.chat.id, paymentMessage.message_id, 'payment');
  }

  await notifyAdminsNewOrder(order.id, order.total_amount, order.currency || 'VND');
}

function adminProductsListText(products, categoryKey = 'all', totalCount = 0) {
  const selectedMeta = productCategoryMeta(categoryKey, 'vi');
  const lines = [];
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  lines.push('🛍 QUẢN LÝ SẢN PHẨM');
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  lines.push('');
  lines.push(`📂 Danh mục: ${selectedMeta.icon} ${selectedMeta.label} (${products.length}/${totalCount})`);
  lines.push('');

  if (products.length === 0) {
    lines.push('Không có sản phẩm trong danh mục đã chọn.');
    lines.push('');
    lines.push('Chọn danh mục khác hoặc thêm sản phẩm mới.');
    return lines.join('\n');
  }

  for (const product of products) {
    const status = product.is_active ? '🟢 bán' : '⚪ ẩn';
    const category = productCategoryMeta(inferProductCategoryKey(product), 'vi');
    lines.push(
      `${category.icon} ${String(product.name || '').slice(0, 36)} | ${formatPriceVnd(product.price)} ${product.currency || 'VND'} | 📦 ${product.stock_quantity ?? '-'} | ${status}`,
    );
  }
  lines.push('');
  lines.push('Chọn sản phẩm để sửa nhanh.');
  return lines.join('\n');
}

function buildAdminProductsKeyboard(products, categoryKey = 'all') {
  const selected = normalizeProductCategoryKey(categoryKey);
  const filteredProducts = filterProductsByCategory(products, selected);
  const rows = [];

  const groupKeys = ['code', 'account', 'support'];
  rows.push(groupKeys.map((key) => {
    const meta = productCategoryMeta(key, 'vi');
    const selectedMark = selected === key ? '✅ ' : '';
    return Markup.button.callback(`${selectedMark}${meta.icon} ${meta.label}`, `admcat:${key}`);
  }));
  {
    const allMeta = productCategoryMeta('all', 'vi');
    const selectedMark = selected === 'all' ? '✅ ' : '';
    rows.push([Markup.button.callback(`${selectedMark}${allMeta.icon} ${allMeta.label}`, 'admcat:all')]);
  }

  if (filteredProducts.length === 0) {
    rows.push([Markup.button.callback('Không có sản phẩm trong mục', 'noop')]);
  } else {
    for (const product of filteredProducts) {
      const statusIcon = product.is_active ? '🟢' : '⚪';
      const category = productCategoryMeta(inferProductCategoryKey(product), 'vi');
      const label = `${statusIcon}${category.icon} ${String(product.name || '').slice(0, 28)}`;
      rows.push([Markup.button.callback(label, `admprd:${product.id}`)]);
    }
  }

  rows.push([
    Markup.button.callback('➕ Thêm mới', 'admin_add_product_start'),
    Markup.button.callback('🔄 Làm mới', `admcat:${selected}`),
  ]);
  rows.push([
    Markup.button.callback('🏠 Admin', 'menu_admin'),
    Markup.button.callback('🗑 Đóng', 'admin_products_close'),
  ]);

  return Markup.inlineKeyboard(rows);
}

function adminProductDetailText(product) {
  const stockHint = usesAccountInventory(product)
    ? 'Tồn kho KEY/ACCOUNT = số dòng kho chưa dùng (đồng bộ tự động)'
    : 'Tồn kho nhập thủ công';
  return [
    '━━━━━━━━━━━━━━━━━━━━━━',
    '🧩 CHI TIẾT SẢN PHẨM',
    '━━━━━━━━━━━━━━━━━━━━━━',
    '',
    `Tên: ${product.name}`,
    `Giá: ${formatPriceVnd(product.price)} ${product.currency || 'VND'}`,
    `Tồn kho: ${product.stock_quantity ?? '-'}`,
    `Trạng thái: ${product.is_active ? 'đang bán' : 'tạm ẩn'}`,
    `Kiểu giao: ${product.delivery_type === 'auto' ? 'auto' : 'thủ công'}`,
    `Ghi chú tồn: ${stockHint}`,
    '',
    'Chọn thao tác chỉnh sửa bên dưới.',
  ].join('\n');
}

function adminProductDetailKeyboard(product) {
  const toggleTo = product.is_active ? '0' : '1';
  const rows = [
    [Markup.button.callback(product.is_active ? '⏸ Tạm ẩn sản phẩm' : '▶️ Mở bán lại', `prdtg:${product.id}:${toggleTo}`)],
    [Markup.button.callback('💲 Sửa giá', `admsetprice:${product.id}`)],
  ];

  if (usesAccountInventory(product)) {
    rows.push([
      Markup.button.callback('➕ 1 dòng kho', `admaddacc1:${product.id}`),
      Markup.button.callback('📥 Nhiều dòng kho', `admaddacc:${product.id}`),
    ]);
    rows.push([
      Markup.button.callback('📚 Xem kho KEY/ACCOUNT', `admaccounts:${product.id}`),
      Markup.button.callback('♻ Đồng bộ tồn', `admsyncstock:${product.id}`),
    ]);
  } else {
    rows.push([Markup.button.callback('📦 Sửa tồn', `admsetstock:${product.id}`)]);
  }

  rows.push([Markup.button.callback('🗑 Xóa sản phẩm', `admdelete:${product.id}`)]);
  rows.push([Markup.button.callback('↩ Danh sách', 'admin_products_v2')]);
  return Markup.inlineKeyboard(rows);
}

async function sendAdminProductsPanel(ctx, options = {}) {
  const normalizedOptions = typeof options === 'boolean'
    ? { shouldEdit: options, categoryKey: 'all' }
    : (options || {});
  const shouldEdit = Boolean(normalizedOptions.shouldEdit);
  const categoryKey = normalizeProductCategoryKey(normalizedOptions.categoryKey || 'all');
  const products = await loadAdminProducts();
  if (products.length === 0) {
    if (shouldEdit) {
      try {
        await ctx.editMessageText('Chưa có sản phẩm.');
      } catch (error) {
        await safeReply(ctx, 'Chưa có sản phẩm.');
      }
      return;
    }
    await ctx.reply('Chưa có sản phẩm.');
    return;
  }

  const filteredProducts = filterProductsByCategory(products, categoryKey);
  const text = adminProductsListText(filteredProducts, categoryKey, products.length);
  const keyboard = buildAdminProductsKeyboard(products, categoryKey);
  if (shouldEdit) {
    try {
      await ctx.editMessageText(text, keyboard);
      return;
    } catch (error) {
      await safeReply(ctx, text, keyboard);
      return;
    }
  }
  await ctx.reply(text, keyboard);
}

async function sendAdminProductDetailMessage(ctx, productId) {
  const product = await loadProductAny(productId);
  if (!product) {
    await safeReply(ctx, 'Không tìm thấy sản phẩm.');
    return null;
  }
  await safeReply(ctx, adminProductDetailText(product), adminProductDetailKeyboard(product));
  return product;
}

function setPendingAdminInput(ctx, payload) {
  pendingAdminInputs.set(String(ctx.from.id), payload);
}

function clearPendingAdminInput(ctx) {
  pendingAdminInputs.delete(String(ctx.from.id));
}

function buildHelpMessage(locale, admin = false) {
  if (admin) {
    return [
      '📘 BROADCAST HELP',
      '',
      '/menu - Xem danh sách gói',
      '/orders - Xem đơn hàng',
      '/me - Thông tin tài khoản',
      '/clear [so_tin] - Dọn nhanh đoạn chat gần nhất',
      '/notifyall <noi_dung> - Gửi thông báo cho toàn bộ user',
      '/help - Trợ giúp',
    ].join('\n');
  }

  if (locale === 'en') {
    return [
      '📘 HELP',
      '',
      '/start - Browse packages',
      '/menu - Browse packages',
      '/orders - View your orders',
      '/me - Your account info',
      '/clear [count] - Clean recent chat messages',
      '/help - Help',
    ].join('\n');
  }

  return [
    '📘 TRỢ GIÚP',
    '',
    '/start - Xem danh sách gói',
    '/menu - Xem danh sách gói',
    '/orders - Xem đơn hàng',
    '/me - Thông tin tài khoản',
    '/clear [so_tin] - Dọn nhanh đoạn chat gần nhất',
    '/help - Trợ giúp',
  ].join('\n');
}

async function registerChatMenuCommands() {
  const userCommands = [
    { command: 'start', description: 'Xem danh sach goi' },
    { command: 'menu', description: 'Xem danh sach goi' },
    { command: 'orders', description: 'Xem don hang' },
    { command: 'me', description: 'Thong tin tai khoan' },
    { command: 'clear', description: 'Don nhanh doan chat' },
    { command: 'help', description: 'Tro giup' },
  ];

  const adminCommands = [
    ...userCommands,
    { command: 'notifyall', description: 'Gui thong bao tat ca user' },
  ];

  try {
    await bot.telegram.setMyDescription('Mua gói dịch vụ số, thanh toán nhanh, giao hàng tự động.');
    await bot.telegram.setMyShortDescription('Mua gói dịch vụ số, thanh toán nhanh, giao hàng tự động.');
  } catch (error) {
    // ignore description sync failures
  }

  // Default command set for all private chats.
  await bot.telegram.setMyCommands(userCommands, { scope: { type: 'all_private_chats' } });

  // Override command set for known admin Telegram IDs.
  for (const rawId of runtimeAdminIds) {
    const chatId = Number(rawId);
    if (!Number.isInteger(chatId)) {
      continue;
    }
    try {
      await bot.telegram.setMyCommands(adminCommands, { scope: { type: 'chat', chat_id: chatId } });
    } catch (error) {
      // ignore: admin chat may not have started bot yet
    }
  }
}

bot.use(async (ctx, next) => {
  if (!ctx.callbackQuery) {
    return next();
  }

  const userId = String(ctx.from?.id || '');
  if (!userId) {
    return next();
  }

  if (inFlightCallbackUsers.has(userId)) {
    await ctx.answerCbQuery('Đang xử lý, vui lòng chờ...');
    return;
  }

  inFlightCallbackUsers.add(userId);
  try {
    await next();
  } finally {
    inFlightCallbackUsers.delete(userId);
  }
});

bot.start(async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await sendHomePanel(ctx, user, locale);
});

bot.command('help', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await ctx.reply(buildHelpMessage(locale, canUseNotifyAll(ctx, user)));
});

bot.command('catalogue', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await sendCataloguePanel(ctx, locale);
});

bot.command('menu', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await sendCataloguePanel(ctx, locale);
});

bot.command('history', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const orders = await loadRecentUserOrders(user.id);
  if (orders.length === 0) {
    await ctx.reply(t(locale, 'emptyHistory'));
    return;
  }

  await ctx.reply(buildOrderHistoryPanel(orders, locale), buildOrderHistoryKeyboard(orders, locale));
});

bot.command('orders', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const orders = await loadRecentUserOrders(user.id);
  if (orders.length === 0) {
    await ctx.reply(t(locale, 'emptyHistory'));
    return;
  }

  await ctx.reply(buildOrderHistoryPanel(orders, locale), buildOrderHistoryKeyboard(orders, locale));
});

bot.command('me', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const stats = await loadUserPurchaseStats(user.id);
  const lines = [
    locale === 'en' ? '👤 ACCOUNT INFO' : '👤 THÔNG TIN TÀI KHOẢN',
    '',
    `ID: ${user.id}`,
    `Telegram: ${ctx.from?.id || '-'}`,
    `Username: @${ctx.from?.username || 'N/A'}`,
    locale === 'en'
      ? `Role: ${isAdmin(ctx, user) ? 'admin' : 'customer'}`
      : `Vai trò: ${isAdmin(ctx, user) ? 'admin' : 'khách hàng'}`,
    locale === 'en'
      ? `Language: ${getLocale(user)}`
      : `Ngôn ngữ: ${getLocale(user)}`,
    '',
    locale === 'en'
      ? `✅ Purchased orders: ${stats.paidOrdersCount}`
      : `✅ Đơn đã mua: ${stats.paidOrdersCount}`,
    locale === 'en'
      ? `💰 Total spent: ${formatCurrencyTotals(stats.totalByCurrency)}`
      : `💰 Tổng tiền đã mua: ${formatCurrencyTotals(stats.totalByCurrency)}`,
  ];
  await ctx.reply(
    lines.join('\n'),
    Markup.inlineKeyboard([
      [Markup.button.callback(locale === 'en' ? '📦 View purchased orders' : '📦 Xem đơn hàng đã mua', 'me_orders_paid')],
    ]),
  );
});

bot.command('support', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const channels = await loadSupportChannels();
  if (channels.length === 0) {
    await ctx.reply(t(locale, 'supportEmpty'));
    return;
  }

  await ctx.reply(buildSupportPanel(channels, locale), { link_preview_options: { is_disabled: true } });
});

bot.command('language', async (ctx) => {
  await ctx.reply(
    'Chọn ngôn ngữ / Choose language',
    Markup.inlineKeyboard([
      [Markup.button.callback('Tiếng Việt', 'lang:vi')],
      [Markup.button.callback('English', 'lang:en')],
    ]),
  );
});

bot.command('admin', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.reply(t(locale, 'noAdmin'));
    return;
  }

  await sendAdminMainPanel(ctx);
});

bot.command('clear', async (ctx) => {
  await ensureUser(ctx);

  const payload = getCommandPayload(ctx.message.text, 'clear');
  const parsedCount = parseNonNegativeInt(payload);
  const requested = parsedCount === null ? 30 : parsedCount;
  const count = Math.min(Math.max(requested, 1), 100);

  const chatId = ctx.chat?.id;
  const currentMessageId = ctx.message?.message_id;
  if (!Number.isInteger(chatId) || !Number.isInteger(currentMessageId)) {
    await ctx.reply('Không thể xác định chat hiện tại để xóa tin.');
    return;
  }

  let deleted = 0;
  let failed = 0;
  for (let offset = 0; offset < count; offset += 1) {
    const messageId = currentMessageId - offset;
    if (messageId <= 0) {
      break;
    }
    try {
      await bot.telegram.deleteMessage(chatId, messageId);
      deleted += 1;
    } catch (error) {
      failed += 1;
    }
  }

  await ctx.reply(`Đã clear xong. Xóa thành công: ${deleted}${failed ? ` | Bỏ qua: ${failed}` : ''}`);
});

bot.command('checkorder', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.reply(t(locale, 'noAdmin'));
    return;
  }

  const keyword = getCommandPayload(ctx.message.text, 'checkorder');
  if (!keyword) {
    await ctx.reply('Dùng: /checkorder <ma_don|ma_ho_tro_DHxxxxxx|8_ky_tu_dau>');
    return;
  }

  const matched = await findAdminOrderByKeyword(keyword);
  if (!matched) {
    await ctx.reply('Không tìm thấy đơn phù hợp.');
    return;
  }

  const detail = await loadAdminOrderDetail(matched.id);
  if (!detail) {
    await ctx.reply('Không tìm thấy chi tiết đơn.');
    return;
  }

  await ctx.reply(buildAdminOrderInspectText(detail), orderActionKeyboard(detail.id, detail.status));
});

bot.command('kho', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.reply(t(locale, 'noAdmin'));
    return;
  }

  const keyword = getCommandPayload(ctx.message.text, 'kho');
  if (!keyword) {
    await ctx.reply('Dùng: /kho <ma_sp_hoac_ten_sp>');
    return;
  }

  const product = await findProductForAdminKeyword(keyword);
  if (!product) {
    await ctx.reply('Không tìm thấy sản phẩm.');
    return;
  }
  if (!usesAccountInventory(product)) {
    await ctx.reply('Sản phẩm SUPPORT không có kho key/account để xem.');
    return;
  }

  const summary = await loadAdminProductAccountsSummary(product.id, 20);
  await ctx.reply(
    buildAdminProductAccountsText(product, summary),
    buildAdminProductAccountsKeyboard(product.id, summary),
  );
});

bot.action('menu_admin', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  await sendAdminMainPanel(ctx, true);
});

bot.action('admin_home_refresh', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery('Đã cập nhật');
  await sendAdminMainPanel(ctx, true);
});

bot.action('admin_home_close', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  try {
    await ctx.deleteMessage();
  } catch (error) {
    await safeReply(ctx, 'Đã đóng admin panel.');
  }
});



bot.action('admin_add_product_start', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  setPendingAdminInput(ctx, { type: 'add_product' });
  await ctx.answerCbQuery();
  await ctx.reply(
    'Nhập sản phẩm mới theo mẫu:\n'
    + 'ten|gia|currency|loai(key|account|support)|mo_ta\n'
    + 'Ví dụ KEY:\n'
    + 'Kling 1k credit|100000|VND|key|Giao key tự động sau thanh toán\n\n'
    + 'Ví dụ ACCOUNT:\n'
    + 'Netflix Premium 1 tháng|99000|VND|account|Giao tài khoản tự động sau thanh toán\n\n'
    + 'Ví dụ SUPPORT:\n'
    + 'Canva Pro nâng cấp|149000|VND|support|Liên hệ Zalo 0563228054 để được hỗ trợ\n'
    + 'Nhập /cancel để hủy.',
  );
});

bot.command('claimadmin', async (ctx) => {
  await ensureUser(ctx);
  await ctx.reply('Đã tắt tính năng admin trong chat bot. Chỉ còn lệnh /notifyall cho tài khoản được cấu hình.');
});

async function loadAllKnownUserTelegramIds() {
  const { data, error } = await db
    .from('users')
    .select('telegram_id')
    .not('telegram_id', 'is', null)
    .order('created_at', { ascending: true })
    .limit(50000);

  if (error) {
    throw error;
  }

  const unique = new Set();
  for (const row of (data || [])) {
    const telegramId = Number(row?.telegram_id);
    if (Number.isInteger(telegramId) && telegramId > 0) {
      unique.add(telegramId);
    }
  }
  return [...unique];
}

async function broadcastMessageToAllKnownUsers(text) {
  const telegramIds = await loadAllKnownUserTelegramIds();
  const result = {
    total: telegramIds.length,
    sent: 0,
    failed: 0,
    errors: [],
  };

  for (const telegramId of telegramIds) {
    try {
      await bot.telegram.sendMessage(telegramId, text);
      result.sent += 1;
    } catch (error) {
      result.failed += 1;
      if (result.errors.length < 10) {
        result.errors.push(`#${telegramId}: ${String(error?.message || 'send_failed').slice(0, 160)}`);
      }
    }

    if (notifyAllDelayMs > 0) {
      await sleep(notifyAllDelayMs);
    }
  }

  return result;
}

bot.command('notify', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.reply(t(locale, 'noAdmin'));
    return;
  }

  const payload = getCommandPayload(ctx.message.text, 'notify');
  const [targetIdRaw, ...messageParts] = payload.split(/\s+/);
  const targetId = Number(targetIdRaw);
  const message = messageParts.join(' ').trim();

  if (!targetIdRaw || !Number.isInteger(targetId) || !message) {
    await ctx.reply('Dùng: /notify <telegram_id> <noi_dung>');
    return;
  }

  try {
    await bot.telegram.sendMessage(targetId, message);
    await ctx.reply(`Đã gửi tin nhắn tới ${targetId}.`);
  } catch (error) {
    await ctx.reply(`Gửi thất bại: ${error.message}`);
  }
});

bot.command('notifyall', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!canUseNotifyAll(ctx, user)) {
    await ctx.reply(t(locale, 'noAdmin'));
    return;
  }

  const message = getCommandPayload(ctx.message.text, 'notifyall');
  if (!message) {
    await ctx.reply('Dùng: /notifyall <noi_dung>');
    return;
  }

  await ctx.reply('Đang gửi thông báo tới toàn bộ người dùng...');
  const result = await broadcastMessageToAllKnownUsers(message);

  const lines = [
    '📣 Kết quả notifyall',
    `Tổng user: ${result.total}`,
    `Gửi thành công: ${result.sent}`,
    `Gửi thất bại: ${result.failed}`,
  ];
  if (result.errors.length > 0) {
    lines.push('');
    lines.push('Một số lỗi mẫu:');
    lines.push(...result.errors);
  }

  await ctx.reply(lines.join('\n'));
});

bot.command('addproduct', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.reply(t(locale, 'noAdmin'));
    return;
  }

  const payload = getCommandPayload(ctx.message.text, 'addproduct');
  const parsed = parseAddProductPayload(payload);
  if (!parsed.ok) {
    await ctx.reply(
      'Sai cú pháp.\n'
      + 'Dùng: /addproduct <ten>|<gia>|<currency>|<loai:key|account|support>|<mo_ta>\n'
      + 'Ví dụ KEY: /addproduct Kling 1k credit|100000|VND|key|Giao key tự động sau thanh toán\n'
      + 'Ví dụ ACCOUNT: /addproduct Netflix Premium 1 tháng|99000|VND|account|Giao tài khoản tự động sau thanh toán\n'
      + 'Ví dụ SUPPORT: /addproduct Canva Pro nâng cấp|149000|VND|support|Liên hệ Zalo 0563228054 để được hỗ trợ',
    );
    return;
  }

  const created = await createProduct(parsed.data);
  await ctx.reply(
    `Đã thêm sản phẩm thành công.\n`
    + `ID: ${created.id}\n`
    + `Tên: ${created.name}\n`
    + `Giá: ${created.price} ${created.currency}\n`
    + `Tồn: ${created.stock_quantity}`,
  );
});

bot.action('noop', async (ctx) => {
  await ctx.answerCbQuery();
});

bot.action('menu_catalogue', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await ctx.answerCbQuery();
  await sendCataloguePanel(ctx, locale, { shouldEdit: true, categoryKey: 'all' });
});

bot.action(/^cat:(all|code|account|support)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const categoryKey = ctx.match[1];
  await ctx.answerCbQuery();
  await sendCataloguePanel(ctx, locale, { shouldEdit: true, categoryKey });
});

bot.action(/^catrf:(all|code|account|support)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const categoryKey = ctx.match[1];
  await ctx.answerCbQuery(locale === 'en' ? 'Updated' : 'Đã cập nhật');
  await sendCataloguePanel(ctx, locale, { shouldEdit: true, categoryKey });
});

bot.action(/^prd:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const productId = ctx.match[1];
  await ctx.answerCbQuery();

  const product = await loadProduct(productId);
  if (!product) {
    await safeReply(ctx, t(locale, 'productMissing'));
    return;
  }

  const details = buildProductDetailPanel(locale, product);
  await replaceOrReply(ctx, details, Markup.inlineKeyboard([
    [
      Markup.button.callback('🛒 x1', `buyq:${product.id}:1`),
      Markup.button.callback('🛒 x3', `buyq:${product.id}:3`),
      Markup.button.callback('🛒 x5', `buyq:${product.id}:5`),
    ],
    [Markup.button.callback('✍️ Nhập số lượng', `buyqinput:${product.id}`)],
    [Markup.button.callback('\uD83D\uDDD1 Đóng', 'prd_close')],
  ]));
});

bot.action(/^buyq:(.+):(\d+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const productId = ctx.match[1];
  const quantity = Number(ctx.match[2] || 1);
  await ctx.answerCbQuery(`x${quantity}`);

  const product = await loadProduct(productId);
  if (!product) {
    await safeReply(ctx, t(locale, 'productMissing'));
    return;
  }

  await processPurchase(ctx, user, locale, product, quantity);
});

bot.action(/^buyqinput:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const productId = ctx.match[1];
  const product = await loadProduct(productId);
  if (!product) {
    await ctx.answerCbQuery('Not found');
    await safeReply(ctx, t(locale, 'productMissing'));
    return;
  }

  pendingUserInputs.set(String(ctx.from.id), { type: 'buy_quantity', productId });
  await ctx.answerCbQuery();
  await ctx.reply(`Nhập số lượng cần mua cho "${product.name}" (số nguyên >= 1).`);
});

bot.action('prd_close', async (ctx) => {
  await ctx.answerCbQuery();
  try {
    await ctx.deleteMessage();
  } catch (error) {
    await safeReply(ctx, '\u0110\u00e3 \u0111\u00f3ng chi ti\u1ebft s\u1ea3n ph\u1ea9m.');
  }
});
bot.action(/^buy:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const productId = ctx.match[1];
  await ctx.answerCbQuery();

  const product = await loadProduct(productId);
  if (!product) {
    await safeReply(ctx, t(locale, 'productMissing'));
    return;
  }

  await processPurchase(ctx, user, locale, product, 1);
});
bot.action('menu_history', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await ctx.answerCbQuery();

  const orders = await loadRecentUserOrders(user.id);
  if (orders.length === 0) {
    await ctx.reply(t(locale, 'emptyHistory'));
    return;
  }

  const text = buildOrderHistoryPanel(orders, locale);
  await replaceOrReply(ctx, text, buildOrderHistoryKeyboard(orders, locale));
});

bot.action('me_orders_paid', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await ctx.answerCbQuery();

  const orders = await loadRecentUserPaidOrders(user.id);
  if (orders.length === 0) {
    await safeReply(ctx, locale === 'en' ? 'You do not have any purchased orders yet.' : 'Bạn chưa có đơn hàng đã mua.');
    return;
  }

  const text = buildOrderHistoryPanel(orders, locale);
  await replaceOrReply(ctx, text, buildOrderHistoryKeyboard(orders, locale));
});

bot.action(/^myord:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const orderId = String(ctx.match[1] || '').trim();
  if (!orderId) {
    await ctx.answerCbQuery(locale === 'en' ? 'Order not found' : 'Không tìm thấy đơn', { show_alert: true });
    return;
  }

  const detail = await loadUserOrderDetail(orderId, user.id);
  if (!detail) {
    await ctx.answerCbQuery(locale === 'en' ? 'Order not found' : 'Không tìm thấy đơn', { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  await replaceOrReply(ctx, buildUserOrderDetailText(detail, locale), buildUserOrderDetailKeyboard(orderId, locale));
});

bot.action('history_close', async (ctx) => {
  await ctx.answerCbQuery();
  try {
    await ctx.deleteMessage();
  } catch (error) {
    // no-op
  }
});

bot.action('menu_support', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await ctx.answerCbQuery();

  const channels = await loadSupportChannels();
  if (channels.length === 0) {
    await ctx.reply(t(locale, 'supportEmpty'));
    return;
  }

  await ctx.reply(buildSupportPanel(channels, locale), { link_preview_options: { is_disabled: true } });
});

bot.action('menu_language', async (ctx) => {
  await ctx.answerCbQuery();
  await ctx.reply(
    'Ch\u1ecdn ng\u00f4n ng\u1eef / Choose language',
    Markup.inlineKeyboard([
      [Markup.button.callback('Ti\u1ebfng Vi\u1ec7t', 'lang:vi')],
      [Markup.button.callback('English', 'lang:en')],
    ]),
  );
});

bot.action('catalogue_refresh', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await ctx.answerCbQuery(locale === 'en' ? 'Updated' : '\u0110\u00e3 c\u1eadp nh\u1eadt');
  await sendCataloguePanel(ctx, locale, { shouldEdit: true, categoryKey: 'all' });
});

bot.action('catalogue_close', async (ctx) => {
  await ctx.answerCbQuery();
  try {
    await ctx.deleteMessage();
  } catch (error) {
    await safeReply(ctx, '\u0110\u00e3 \u0111\u00f3ng danh m\u1ee5c.');
  }
});

bot.action(/^paydone:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const orderId = ctx.match[1];
  const order = await loadOrderByIdForUser(orderId, user.id);

  if (!order) {
    await ctx.answerCbQuery(locale === 'en' ? 'Order not found' : 'Không tìm thấy đơn', { show_alert: true });
    return;
  }

  const transferContent = buildMmobankTransferCode(order.id);
  await ctx.answerCbQuery(locale === 'en' ? 'Sent to admin' : 'Đã báo admin');
  const confirmation = locale === 'en'
    ? `Payment notice sent. Admin will verify your transfer.\nOrder: #${order.id}\nTransfer content: ${transferContent}`
    : `Đã gửi báo thanh toán cho admin. Vui lòng chờ xác nhận.\nĐơn: #${order.id}\nNội dung CK: ${transferContent}`;
  const clearKeyboard = { reply_markup: { inline_keyboard: [] } };
  if (ctx.callbackQuery?.message?.photo) {
    await replaceCaptionOrReply(ctx, confirmation, clearKeyboard);
  } else {
    await replaceOrReply(ctx, confirmation, clearKeyboard);
  }

  const adminIds = [...runtimeAdminIds].map((id) => Number(id)).filter(Number.isInteger);
  for (const telegramId of adminIds) {
    try {
      await bot.telegram.sendMessage(
        telegramId,
        `Bao thanh toan MMOBANK\nDon: #${order.id}\nUser: ${user.id}\nSo tien: ${order.total_amount} ${order.currency || 'VND'}\nNoi dung CK: ${transferContent}`,
      );
    } catch (error) {
      // no-op
    }
  }
});

bot.action(/^paycancel:(.+)$/, async (ctx) => {
  try {
    const user = await ensureUser(ctx);
    const locale = getLocale(user);
    const orderId = ctx.match[1];

    const result = await cancelOrderByUser(orderId, user.id);
    if (!result.ok) {
      if (result.reason === 'not_found') {
        await safeAnswerCbQuery(ctx, locale === 'en' ? 'Order not found' : 'Không tìm thấy đơn', { show_alert: true });
        return;
      }
      if (result.reason === 'paid') {
        await safeAnswerCbQuery(ctx, locale === 'en' ? 'Order already paid' : 'Đơn đã thanh toán', { show_alert: true });
        return;
      }
      await safeAnswerCbQuery(ctx, locale === 'en' ? 'Unable to cancel now' : 'Không thể hủy lúc này', { show_alert: true });
      return;
    }

    await safeAnswerCbQuery(ctx, result.alreadyCancelled ? 'Đã hủy trước đó' : 'Đã hủy đơn');
    try {
      await ctx.deleteMessage();
    } catch (deleteError) {
      // no-op: can already be deleted by clearOrderPaymentMessages
    }
  } catch (error) {
    console.error('paycancel handler failed:', error);
    await safeAnswerCbQuery(ctx, 'Đơn có thể đã hết hạn hoặc đã xử lý.', { show_alert: true });
  }
});

bot.action(/^lang:(vi|en)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const target = ctx.match[1];
  const updatedUser = await setUserLanguage(user.id, target);
  await ctx.answerCbQuery('OK');
  await ctx.reply(t(target, 'langCurrent'));
  await sendHomePanel(ctx, updatedUser, target);
});

bot.action('admin_orders_new', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  const orders = await loadAdminOrders();

  await replaceOrReply(ctx, buildAdminOrdersText(orders), buildAdminOrdersKeyboard(orders));
});

bot.action(/^admord:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const orderId = ctx.match[1];
  const order = await loadAdminOrderById(orderId);
  if (!order) {
    await ctx.answerCbQuery('Không tìm thấy đơn', { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  await replaceOrReply(ctx, adminOrderDetailText(order), orderActionKeyboard(order.id, order.status));
});

bot.action(/^ordst:(.+):(draft|confirmed|paid|cancelled)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);

  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const orderId = ctx.match[1];
  const status = ctx.match[2];
  const result = await updateOrderStatusFromAdmin(orderId, status, user.id, 'Updated from admin panel');
  if (!result.ok) {
    if (result.reason === 'not_found') {
      await ctx.answerCbQuery('Không tìm thấy đơn', { show_alert: true });
      return;
    }
    await ctx.answerCbQuery('Không thể cập nhật trạng thái', { show_alert: true });
    return;
  }
  const updated = result.order;

  await notifyOrderOwnerStatusChanged(updated);

  await ctx.answerCbQuery('Updated');
  await replaceOrReply(
    ctx,
    `${adminOrderDetailText(updated)}\n\n${t(locale, 'orderStatusUpdated', { id: updated.id, status: updated.status })}`,
    orderActionKeyboard(updated.id, updated.status),
  );
});

bot.action(/^ordclr:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const orderId = String(ctx.match[1] || '').trim();
  if (!orderId) {
    await ctx.answerCbQuery('Order not found', { show_alert: true });
    return;
  }

  const { data: order, error } = await db
    .from('orders')
    .select('id,user_id')
    .eq('id', orderId)
    .maybeSingle();
  if (error || !order) {
    await ctx.answerCbQuery('Order not found', { show_alert: true });
    return;
  }

  if (order.user_id !== user.id && !isAdmin(ctx, user)) {
    await ctx.answerCbQuery('Khong co quyen', { show_alert: true });
    return;
  }

  await ctx.answerCbQuery('Đang dọn tin nhắn...');
  await clearOrderPaymentMessages(orderId);
  try {
    await ctx.deleteMessage();
  } catch (errorDelete) {
    // no-op
  }
});

bot.action(/^prdtg:(.+):(0|1)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const productId = ctx.match[1];
  const target = ctx.match[2] === '1';

  const { error } = await db
    .from('products')
    .update({ is_active: target })
    .eq('id', productId);

  if (error) {
    throw error;
  }

  await ctx.answerCbQuery('OK');
  await ctx.reply(`Sản phẩm ${productId} -> ${target ? 'đang bán' : 'tạm ẩn'}`);
});

bot.action('admin_products_v2', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  clearPendingAdminInput(ctx);
  await ctx.answerCbQuery();
  await sendAdminProductsPanel(ctx, { shouldEdit: true, categoryKey: 'all' });
});

bot.action('admin_products_refresh', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery('Updated');
  await sendAdminProductsPanel(ctx, { shouldEdit: true, categoryKey: 'all' });
});

bot.action(/^admcat:(all|code|account|support)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const categoryKey = ctx.match[1];
  await ctx.answerCbQuery('OK');
  await sendAdminProductsPanel(ctx, { shouldEdit: true, categoryKey });
});

bot.action('admin_products_close', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  try {
    await ctx.deleteMessage();
  } catch (error) {
    await safeReply(ctx, 'Đã đóng panel quản lý.');
  }
});

bot.action(/^admprd:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const productId = ctx.match[1];
  const product = await loadProductAny(productId);
  if (!product) {
    await ctx.answerCbQuery('Not found');
    await safeReply(ctx, 'Không tìm thấy sản phẩm.');
    return;
  }

  await ctx.answerCbQuery();
  await replaceOrReply(ctx, adminProductDetailText(product), adminProductDetailKeyboard(product));
});

bot.action(/^admsetprice:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const productId = ctx.match[1];
  const product = await loadProductAny(productId);
  if (!product) {
    await ctx.answerCbQuery('Not found');
    return;
  }

  setPendingAdminInput(ctx, { type: 'edit_price', productId });
  await ctx.answerCbQuery();
  await ctx.reply(
    `Nhập giá mới cho "${product.name}" (chỉ nhập số). Ví dụ: 120000\n`
    + 'Nhập /cancel để hủy.',
  );
});

bot.action(/^admaddacc1:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const productId = ctx.match[1];
  const product = await loadProductAny(productId);
  if (!product) {
    await ctx.answerCbQuery('Not found');
    return;
  }
  if (!usesAccountInventory(product)) {
    await ctx.answerCbQuery('Chỉ sản phẩm KEY/ACCOUNT mới thêm dữ liệu kho', { show_alert: true });
    return;
  }

  setPendingAdminInput(ctx, { type: 'add_one_account', productId });
  await ctx.answerCbQuery();
  await ctx.reply(
    `Nhập 1 dòng kho cho "${product.name}" (định dạng tự do).\n`
    + 'Ví dụ: email@gmail.com|MatKhau123|2FA:ABCD-EFGH\n'
    + 'Nhập /cancel để hủy.',
  );
});

bot.action(/^admaddacc:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const productId = ctx.match[1];
  const product = await loadProductAny(productId);
  if (!product) {
    await ctx.answerCbQuery('Not found');
    return;
  }
  if (!usesAccountInventory(product)) {
    await ctx.answerCbQuery('Chỉ sản phẩm KEY/ACCOUNT mới thêm dữ liệu kho', { show_alert: true });
    return;
  }

  setPendingAdminInput(ctx, { type: 'add_accounts', productId });
  await ctx.answerCbQuery();
  await ctx.reply(
    `Nhập danh sách dòng kho cho "${product.name}" (mỗi dòng 1 mục, định dạng tự do).\n`
    + 'Ví dụ:\n'
    + 'email1@gmail.com|MatKhau123|2FA:ABCD-EFGH\n'
    + 'email2@gmail.com|MatKhau456\n\n'
    + 'Nhập /cancel để hủy.',
  );
});

bot.action(/^admsetstock:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const productId = ctx.match[1];
  const product = await loadProductAny(productId);
  if (!product) {
    await ctx.answerCbQuery('Not found');
    return;
  }
  if (usesAccountInventory(product)) {
    const synced = await syncProductStockFromAutoAccounts(productId);
    const refreshedProduct = await loadProductAny(productId);
    await ctx.answerCbQuery('SP KEY/ACCOUNT: tồn kho đồng bộ theo dữ liệu kho', { show_alert: true });
    if (refreshedProduct) {
      await replaceOrReply(
        ctx,
        `${adminProductDetailText(refreshedProduct)}\n\n♻ Đã đồng bộ tồn: ${synced.stock_quantity}`,
        adminProductDetailKeyboard(refreshedProduct),
      );
    }
    return;
  }

  setPendingAdminInput(ctx, { type: 'edit_stock', productId });
  await ctx.answerCbQuery();
  await ctx.reply(
    `Nhập tồn mới cho "${product.name}" (số nguyên >= 0). Ví dụ: 50\n`
    + 'Nhập /cancel để hủy.',
  );
});

bot.action(/^admsyncstock:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const productId = ctx.match[1];
  const product = await loadProductAny(productId);
  if (!product) {
    await ctx.answerCbQuery('Not found');
    return;
  }
  if (!usesAccountInventory(product)) {
    await ctx.answerCbQuery('Chỉ dùng cho sản phẩm KEY/ACCOUNT', { show_alert: true });
    return;
  }

  const synced = await syncProductStockFromAutoAccounts(productId);
  const refreshedProduct = await loadProductAny(productId);
  await ctx.answerCbQuery('Đã đồng bộ tồn');
  if (refreshedProduct) {
    await replaceOrReply(
      ctx,
      `${adminProductDetailText(refreshedProduct)}\n\n♻ Đã đồng bộ tồn: ${synced.stock_quantity}`,
      adminProductDetailKeyboard(refreshedProduct),
    );
  }
});

bot.action(/^admaccounts:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const productId = ctx.match[1];
  const product = await loadProductAny(productId);
  if (!product) {
    await ctx.answerCbQuery('Not found');
    return;
  }
  if (!usesAccountInventory(product)) {
    await ctx.answerCbQuery('Sản phẩm SUPPORT không có kho key/account', { show_alert: true });
    return;
  }

  const summary = await loadAdminProductAccountsSummary(productId, 20);
  await ctx.answerCbQuery();
  await replaceOrReply(
    ctx,
    buildAdminProductAccountsText(product, summary),
    buildAdminProductAccountsKeyboard(productId, summary),
  );
});

bot.action(/^admaccdel:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const accountId = String(ctx.match[1] || '').trim();
  const account = await loadProductAccountById(accountId);
  if (!account) {
    await ctx.answerCbQuery('Không tìm thấy account', { show_alert: true });
    return;
  }
  if (account.is_used) {
    await ctx.answerCbQuery('Account đã dùng, không xóa trực tiếp', { show_alert: true });
    return;
  }

  await deleteProductAccountById(accountId);
  await syncProductStockFromAutoAccounts(account.product_id);
  const product = await loadProductAny(account.product_id);
  if (!product) {
    await ctx.answerCbQuery('Đã xóa account');
    return;
  }
  const summary = await loadAdminProductAccountsSummary(account.product_id, 20);
  await ctx.answerCbQuery('Đã xóa account');
  await replaceOrReply(
    ctx,
    buildAdminProductAccountsText(product, summary),
    buildAdminProductAccountsKeyboard(account.product_id, summary),
  );
});

bot.action(/^admaccstate:(.+):(0|1)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const accountId = String(ctx.match[1] || '').trim();
  const target = ctx.match[2] === '1';
  const account = await loadProductAccountById(accountId);
  if (!account) {
    await ctx.answerCbQuery('Không tìm thấy account', { show_alert: true });
    return;
  }

  await setProductAccountUsedState(accountId, target, null);
  await syncProductStockFromAutoAccounts(account.product_id);
  const product = await loadProductAny(account.product_id);
  if (!product) {
    await ctx.answerCbQuery(target ? 'Đã chuyển used' : 'Đã chuyển available');
    return;
  }
  const summary = await loadAdminProductAccountsSummary(account.product_id, 20);
  await ctx.answerCbQuery(target ? 'Đã chuyển used' : 'Đã chuyển available');
  await replaceOrReply(
    ctx,
    buildAdminProductAccountsText(product, summary),
    buildAdminProductAccountsKeyboard(account.product_id, summary),
  );
});

bot.action(/^admdelete:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  const productId = ctx.match[1];
  const product = await loadProductAny(productId);
  if (!product) {
    await ctx.answerCbQuery('Not found');
    return;
  }

  try {
    const linkedOrderItems = await countOrderItemsByProductId(productId);
    if (linkedOrderItems > 0) {
      await updateAdminProduct(productId, { is_active: false });
      await safeAnswerCbQuery(
        ctx,
        `Sản phẩm đã có ${linkedOrderItems} dòng đơn hàng, không thể xóa cứng. Đã chuyển tạm ẩn.`,
        { show_alert: true },
      );

      const refreshedProduct = await loadProductAny(productId);
      if (refreshedProduct) {
        await replaceOrReply(
          ctx,
          `${adminProductDetailText(refreshedProduct)}\n\n⚠️ Sản phẩm có dữ liệu đơn hàng nên chỉ tạm ẩn, không thể xóa hẳn.`,
          adminProductDetailKeyboard(refreshedProduct),
        );
      }
      return;
    }

    await hardDeleteProduct(productId);
    await safeAnswerCbQuery(ctx, 'Đã xóa sản phẩm');
    await sendAdminProductsPanel(ctx, { shouldEdit: true, categoryKey: 'all' });
    await safeReply(ctx, `Đã xóa sản phẩm: ${product.name}`);
  } catch (error) {
    console.error('admdelete failed:', error);
    try {
      await updateAdminProduct(productId, { is_active: false });
      await safeAnswerCbQuery(ctx, 'Không thể xóa cứng, đã chuyển tạm ẩn.', { show_alert: true });
      const refreshedProduct = await loadProductAny(productId);
      if (refreshedProduct) {
        await replaceOrReply(
          ctx,
          `${adminProductDetailText(refreshedProduct)}\n\n⚠️ Đã chuyển tạm ẩn do có ràng buộc dữ liệu.`,
          adminProductDetailKeyboard(refreshedProduct),
        );
      }
    } catch (nestedError) {
      await safeAnswerCbQuery(ctx, 'Xóa sản phẩm thất bại, thử lại sau.', { show_alert: true });
    }
  }
});

bot.on('text', async (ctx, next) => {
  const userPending = pendingUserInputs.get(String(ctx.from.id));
  if (userPending) {
    const user = await ensureUser(ctx);
    const locale = getLocale(user);
    const text = (ctx.message?.text || '').trim();
    if (/^\/cancel\b/i.test(text)) {
      pendingUserInputs.delete(String(ctx.from.id));
      await ctx.reply('Đã hủy thao tác.');
      return;
    }

    if (userPending.type === 'buy_quantity') {
      const quantity = parseNonNegativeInt(text);
      if (quantity === null || quantity < 1) {
        await ctx.reply('Số lượng không hợp lệ. Hãy nhập số nguyên >= 1.');
        return;
      }

      const product = await loadProduct(userPending.productId);
      pendingUserInputs.delete(String(ctx.from.id));
      if (!product) {
        await ctx.reply(t(locale, 'productMissing'));
        return;
      }

      await processPurchase(ctx, user, locale, product, quantity);
      return;
    }
  }

  const pending = pendingAdminInputs.get(String(ctx.from.id));
  if (!pending) {
    return next();
  }

  const user = await ensureUser(ctx);
  if (!isAdmin(ctx, user)) {
    clearPendingAdminInput(ctx);
    return next();
  }

  const text = (ctx.message?.text || '').trim();
  if (!text) {
    return next();
  }

  if (/^\/cancel\b/i.test(text)) {
    const pendingProductId = pending?.productId || null;
    clearPendingAdminInput(ctx);
    await ctx.reply('Đã hủy thao tác.');
    if (pendingProductId) {
      await sendAdminProductDetailMessage(ctx, pendingProductId);
    }
    return;
  }

  if (text.startsWith('/')) {
    return next();
  }

  try {
    if (pending.type === 'add_product') {
      const parsed = parseAddProductPayload(text);
      if (!parsed.ok) {
        await ctx.reply(
          'Sai cú pháp. Mẫu: ten|gia|currency|loai(key|account|support)|mo_ta\n'
          + 'Ví dụ KEY: Kling 1k credit|100000|VND|key|Giao key tự động sau thanh toán\n'
          + 'Ví dụ ACCOUNT: Netflix Premium 1 tháng|99000|VND|account|Giao tài khoản tự động sau thanh toán\n'
          + 'Ví dụ SUPPORT: Canva Pro nâng cấp|149000|VND|support|Liên hệ Zalo 0563228054 để được hỗ trợ',
        );
        return;
      }

      const created = await createProduct(parsed.data);
      clearPendingAdminInput(ctx);
      await ctx.reply(
        `Đã thêm sản phẩm: ${created.name}\n`
        + `Giá: ${created.price} ${created.currency}\n`
        + `Tồn: ${created.stock_quantity}`,
      );
      await sendAdminProductsPanel(ctx);
      return;
    }

    if (pending.type === 'add_one_account') {
      const product = await loadProductAny(pending.productId);
      if (!product) {
        clearPendingAdminInput(ctx);
        await ctx.reply('Không tìm thấy sản phẩm.');
        return;
      }
      if (!usesAccountInventory(product)) {
        clearPendingAdminInput(ctx);
        await ctx.reply('Sản phẩm SUPPORT không thể thêm dữ liệu kho key/account.');
        await sendAdminProductDetailMessage(ctx, pending.productId);
        return;
      }

      const line = String(text || '').trim();
      if (!line) {
        await ctx.reply('Dữ liệu kho trống.');
        return;
      }
      const result = await addProductAccountsBulk(pending.productId, line);
      const synced = await syncProductStockFromAutoAccounts(pending.productId);
      clearPendingAdminInput(ctx);
      await ctx.reply(
        `Đã thêm dữ liệu kho cho "${product.name}".\n`
        + `Thêm mới: ${result.added}\n`
        + `Bỏ qua (trùng): ${result.skipped}\n`
        + `Tồn hiện tại: ${synced.stock_quantity}`,
      );
      await sendAdminProductDetailMessage(ctx, pending.productId);
      return;
    }

    if (pending.type === 'edit_price') {
      const value = parsePositiveMoney(text);
      if (value === null) {
        await ctx.reply('Giá không hợp lệ. Hãy nhập số dương, ví dụ 120000.');
        return;
      }

      const updated = await updateAdminProduct(pending.productId, { price: value });
      clearPendingAdminInput(ctx);
      await ctx.reply(`Đã cập nhật giá: ${updated.name} -> ${updated.price} ${updated.currency || 'VND'}`);
      await ctx.reply(adminProductDetailText(updated), adminProductDetailKeyboard(updated));
      return;
    }

    if (pending.type === 'edit_stock') {
      const product = await loadProductAny(pending.productId);
      if (!product) {
        clearPendingAdminInput(ctx);
        await ctx.reply('Không tìm thấy sản phẩm.');
        return;
      }
      if (usesAccountInventory(product)) {
        clearPendingAdminInput(ctx);
        const synced = await syncProductStockFromAutoAccounts(pending.productId);
        await ctx.reply(`Sản phẩm KEY/ACCOUNT không nhập tồn tay. Đã đồng bộ tồn: ${synced.stock_quantity}`);
        await sendAdminProductDetailMessage(ctx, pending.productId);
        return;
      }

      const value = parseNonNegativeInt(text);
      if (value === null) {
        await ctx.reply('Tồn không hợp lệ. Hãy nhập số nguyên >= 0, ví dụ 50.');
        return;
      }

      const updated = await updateAdminProduct(pending.productId, { stock_quantity: value });
      clearPendingAdminInput(ctx);
      await ctx.reply(`Đã cập nhật tồn: ${updated.name} -> ${updated.stock_quantity}`);
      await ctx.reply(adminProductDetailText(updated), adminProductDetailKeyboard(updated));
      return;
    }

    if (pending.type === 'add_accounts') {
      const product = await loadProductAny(pending.productId);
      if (!product) {
        clearPendingAdminInput(ctx);
        await ctx.reply('Không tìm thấy sản phẩm.');
        return;
      }
      if (!usesAccountInventory(product)) {
        clearPendingAdminInput(ctx);
        await ctx.reply('Sản phẩm SUPPORT không thể thêm dữ liệu kho key/account.');
        await sendAdminProductDetailMessage(ctx, pending.productId);
        return;
      }

      const result = await addProductAccountsBulk(pending.productId, text);
      const synced = await syncProductStockFromAutoAccounts(pending.productId);
      clearPendingAdminInput(ctx);
      await ctx.reply(
        `Đã xử lý ${result.total} dòng cho "${product.name}".\n`
        + `Thêm mới: ${result.added}\n`
        + `Bỏ qua (trùng): ${result.skipped}\n`
        + `Tồn hiện tại: ${synced.stock_quantity}`,
      );
      await sendAdminProductDetailMessage(ctx, pending.productId);
      return;
    }

  } catch (error) {
    clearPendingAdminInput(ctx);
    await safeReply(ctx, `Xử lý thất bại: ${error.message}`);
    return;
  }

  return next();
});

bot.action('admin_reports', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  const report = await loadReport();

  await replaceOrReply(
    ctx,
    `${t(locale, 'reportTitle')}\n`
    + `- Total orders: ${report.totalOrders}\n`
    + `- Confirmed: ${report.confirmedOrders}\n`
    + `- Paid: ${report.paidOrders}\n`
    + `- Revenue: ${formatPriceVnd(report.revenue)} VND`,
    Markup.inlineKeyboard([
      [
        Markup.button.callback('🔄 Làm mới', 'admin_reports'),
        Markup.button.callback('🏠 Admin', 'menu_admin'),
      ],
    ]),
  );
});

bot.catch(async (err, ctx) => {
  try {
    await ctx.reply('Có lỗi xảy ra. Vui lòng thử lại sau.');
  } catch (nestedError) {
    // no-op
  }
  console.error('Bot error', err);
});

const webhookServer = startMmobankWebhookServer();

bot.launch().then(async () => {
  await registerChatMenuCommands();
  await restorePendingOrderExpirySchedules();
  startOrderExpirySweeper();
  console.log('Bot launched.');
}).catch((err) => {
  console.error('Failed to launch bot', err);
  stopOrderExpirySweeper();
  try {
    webhookServer.close();
  } catch (error) {
    // no-op
  }
  process.exit(1);
});

process.once('SIGINT', () => {
  stopOrderExpirySweeper();
  try {
    webhookServer.close();
  } catch (error) {
    // no-op
  }
  bot.stop('SIGINT');
});

process.once('SIGTERM', () => {
  stopOrderExpirySweeper();
  try {
    webhookServer.close();
  } catch (error) {
    // no-op
  }
  bot.stop('SIGTERM');
});

module.exports = bot;




