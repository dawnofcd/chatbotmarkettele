require('dotenv').config();

const crypto = require('crypto');
const { Telegraf, Markup } = require('telegraf');
const { createClient } = require('./pg_client');

const botToken = process.env.TELEGRAM_TOKEN;
const databaseUrl = process.env.DATABASE_URL;
const adminSecretKey = process.env.ADMIN_SECRET_KEY || '';
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
const runtimeAdminIds = new Set(adminTelegramIds);
const pendingAdminInputs = new Map();
const pendingUserInputs = new Map();

const TEXTS = {
  vi: {
    welcome: 'Xin chào! Tôi là bot bán tài khoản.',
    menuHint: 'Chọn một mục bên dưới:',
    emptyCatalogue: 'Danh mục hiện đang trống.',
    emptyHistory: 'Bạn chưa có đơn hàng nào.',
    supportEmpty: 'Chưa có kênh hỗ trợ.',
    noAdmin: 'Bạn không có quyền admin.',
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
  const iconHistory = '\uD83D\uDCDC';
  const iconSupport = '\uD83D\uDCAC';
  const iconLang = '\uD83C\uDF10';

  if (locale === 'en') {
    const rows = [
      [
        Markup.button.callback(`${iconCart} Catalogue`, 'menu_catalogue'),
        Markup.button.callback(`${iconHistory} History`, 'menu_history'),
      ],
      [
        Markup.button.callback(`${iconSupport} Support`, 'menu_support'),
        Markup.button.callback(`${iconLang} Language`, 'menu_language'),
      ],
    ];

    if (hasAdminAccess) {
      rows.push([Markup.button.callback('Admin', 'menu_admin')]);
    }

    return Markup.inlineKeyboard(rows);
  }

  const rows = [
    [
      Markup.button.callback(`${iconCart} Danh \u006d\u1ee5c`, 'menu_catalogue'),
      Markup.button.callback(`${iconHistory} L\u1ecbch s\u1eed`, 'menu_history'),
    ],
    [
      Markup.button.callback(`${iconSupport} H\u1ed7 tr\u1ee3`, 'menu_support'),
      Markup.button.callback(`${iconLang} Ng\u00f4n ng\u1eef`, 'menu_language'),
    ],
  ];

  if (hasAdminAccess) {
    rows.push([Markup.button.callback('Admin', 'menu_admin')]);
  }

  return Markup.inlineKeyboard(rows);
}

function buildHomeMessage(locale, firstName) {
  const iconAnnounce = '\uD83D\uDCE2';
  const envAnnouncement = locale === 'en'
    ? process.env.HOME_ANNOUNCEMENT_EN
    : process.env.HOME_ANNOUNCEMENT_VI;

  if (envAnnouncement) {
    return envAnnouncement;
  }

  if (locale === 'en') {
    return [
      `${iconAnnounce} ADMIN ANNOUNCEMENT`,
      '',
      `Hello ${firstName || 'there'}!`,
      '- Check the latest account deals in Catalogue.',
      '- Need help? Tap Support for fast contact.',
      '- Use Language to switch between Vietnamese/English.',
    ].join('\n');
  }

  return [
    `${iconAnnounce} TH\u00d4NG B\u00c1O T\u1eea ADMIN`,
    '',
    `Xin ch\u00e0o ${firstName || 'b\u1ea1n'}!`,
    '- C\u00f3 deal t\u00e0i kho\u1ea3n m\u1edbi trong Danh m\u1ee5c.',
    '- C\u1ea7n h\u1ed7 tr\u1ee3 nhanh? B\u1ea5m H\u1ed7 tr\u1ee3 \u0111\u1ec3 li\u00ean h\u1ec7.',
    '- C\u00f3 th\u1ec3 \u0111\u1ed5i ng\u00f4n ng\u1eef t\u1ea1i m\u1ee5c Ng\u00f4n ng\u1eef.',
  ].join('\n');
}

async function sendHomePanel(ctx, userRecord, locale) {
  const firstName = ctx.from.first_name || (locale === 'en' ? 'there' : 'b\u1ea1n');
  await ctx.reply(buildHomeMessage(locale, firstName), mainMenu(locale, isAdmin(ctx, userRecord)));
}
const adminMenu = Markup.inlineKeyboard([
  [
    Markup.button.callback('Đơn mới', 'admin_orders_new'),
    Markup.button.callback('Sản phẩm', 'admin_products_v2'),
  ],
  [Markup.button.callback('Thống kê', 'admin_reports')],
  [
    Markup.button.callback('Thêm sản phẩm', 'admin_add_product_help'),
    Markup.button.callback('Thông báo theo loại', 'admin_notify_category_help'),
  ],
  [Markup.button.callback('Cập nhật nhiều SP', 'admin_bulk_update_start')],
]);

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

function isAdmin(ctx, userRecord) {
  const telegramId = String(ctx.from.id);
  return runtimeAdminIds.has(telegramId) || userRecord?.role === 'admin';
}

function isSecretKeyValid(input) {
  if (!adminSecretKey || !input) {
    return false;
  }

  const expected = Buffer.from(adminSecretKey);
  const actual = Buffer.from(input);
  if (expected.length !== actual.length) {
    return false;
  }

  return crypto.timingSafeEqual(expected, actual);
}

function getCommandPayload(text, command) {
  const pattern = new RegExp(`^/${command}(?:@\\w+)?\\s*`, 'i');
  return (text || '').replace(pattern, '').trim();
}

function chunkArray(items, chunkSize) {
  const chunks = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    chunks.push(items.slice(i, i + chunkSize));
  }
  return chunks;
}

function slugifyName(name) {
  return String(name || '')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 50);
}

function parseAddProductPayload(payload) {
  const parts = payload.split('|').map((p) => p.trim());
  const [categoryId, name, priceRaw, stockRaw, currencyRaw, ...descriptionParts] = parts;
  const description = descriptionParts.join('|').trim();
  const price = Number(priceRaw);
  const stock = Number(stockRaw);
  const currency = (currencyRaw || 'VND').toUpperCase();

  if (!categoryId || !name || !Number.isFinite(price) || !Number.isInteger(stock)) {
    return { ok: false };
  }

  return {
    ok: true,
    data: {
      categoryId,
      name,
      price,
      stock,
      currency,
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

function parseBulkProductUpdates(input) {
  const lines = String(input || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const updates = [];
  const invalidLines = [];

  for (const line of lines) {
    const [productId, priceRaw, stockRaw, activeRaw] = line.split('|').map((p) => p.trim());
    const price = Number(priceRaw);
    const stock = Number(stockRaw);
    const hasActive = activeRaw !== undefined && activeRaw !== '';
    const active = hasActive ? ['1', 'true', 'on', 'active'].includes(String(activeRaw).toLowerCase()) : null;

    if (!productId || !Number.isFinite(price) || !Number.isInteger(stock) || stock < 0) {
      invalidLines.push(line);
      continue;
    }

    updates.push({
      productId,
      patch: hasActive
        ? { price, stock_quantity: stock, is_active: active }
        : { price, stock_quantity: stock },
    });
  }

  return { updates, invalidLines, total: lines.length };
}

async function getAllUserTelegramIds() {
  const pageSize = 500;
  let from = 0;
  const ids = [];

  while (true) {
    const { data, error } = await db
      .from('users')
      .select('telegram_id')
      .not('telegram_id', 'is', null)
      .range(from, from + pageSize - 1);

    if (error) {
      throw error;
    }

    if (!data || data.length === 0) {
      break;
    }

    ids.push(...data.map((row) => Number(row.telegram_id)).filter(Boolean));
    if (data.length < pageSize) {
      break;
    }

    from += pageSize;
  }

  return [...new Set(ids)];
}

async function loadActiveCategories() {
  const { data, error } = await db
    .from('categories')
    .select('id,name')
    .eq('is_active', true)
    .order('name', { ascending: true });

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadAllCategories() {
  const { data, error } = await db
    .from('categories')
    .select('id,name,is_active')
    .order('name', { ascending: true })
    .limit(200);

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadCategoryById(categoryId) {
  const { data, error } = await db
    .from('categories')
    .select('id,name,is_active')
    .eq('id', categoryId)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data;
}

async function loadProductsByCategory(categoryId) {
  const { data, error } = await db
    .from('products')
    .select('id,name,description,delivery_type,manual_contact_note,price,currency,stock_quantity,is_active')
    .eq('category_id', categoryId)
    .eq('is_active', true)
    .order('name', { ascending: true })
    .limit(50);

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadActiveProducts(limit = 50) {
  const { data, error } = await db
    .from('products')
    .select('id,name,price,currency,stock_quantity')
    .eq('is_active', true)
    .order('updated_at', { ascending: false })
    .limit(limit);

  if (error) {
    throw error;
  }

  return data || [];
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
    .select('id,category_id,name,description,delivery_type,manual_contact_note,price,currency,stock_quantity,is_active')
    .eq('id', productId)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data;
}

async function createProduct(input) {
  const slugBase = slugifyName(input.name) || `product-${Date.now()}`;
  const slug = `${slugBase}-${Math.random().toString(36).slice(2, 7)}`;
  const { data, error } = await db
    .from('products')
    .insert({
      category_id: input.categoryId,
      name: input.name,
      slug,
      description: input.description || null,
      delivery_type: 'manual',
      manual_contact_note: 'Sau khi chuy\u1ec3n kho\u1ea3n th\u00e0nh c\u00f4ng, vui l\u00f2ng nh\u1eafn admin \u0111\u1ec3 \u0111\u01b0\u1ee3c c\u1ea5p t\u00e0i kho\u1ea3n.',
      price: input.price,
      currency: input.currency || 'VND',
      stock_quantity: input.stock,
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

async function getUserTelegramIdsByCategory(categoryId) {
  const { data: products, error: productsError } = await db
    .from('products')
    .select('id')
    .eq('category_id', categoryId)
    .limit(5000);

  if (productsError) {
    throw productsError;
  }

  const productIds = (products || []).map((row) => row.id).filter(Boolean);
  if (productIds.length === 0) {
    return [];
  }

  const orderIdSet = new Set();
  for (const productChunk of chunkArray(productIds, 200)) {
    const { data: items, error: itemsError } = await db
      .from('order_items')
      .select('order_id')
      .in('product_id', productChunk)
      .limit(5000);

    if (itemsError) {
      throw itemsError;
    }

    for (const row of items || []) {
      if (row.order_id) {
        orderIdSet.add(row.order_id);
      }
    }
  }

  const orderIds = [...orderIdSet];
  if (orderIds.length === 0) {
    return [];
  }

  const userIdSet = new Set();
  for (const orderChunk of chunkArray(orderIds, 200)) {
    const { data: orders, error: ordersError } = await db
      .from('orders')
      .select('user_id')
      .in('id', orderChunk)
      .limit(5000);

    if (ordersError) {
      throw ordersError;
    }

    for (const row of orders || []) {
      if (row.user_id) {
        userIdSet.add(row.user_id);
      }
    }
  }

  const userIds = [...userIdSet];
  if (userIds.length === 0) {
    return [];
  }

  const telegramIdSet = new Set();
  for (const userChunk of chunkArray(userIds, 200)) {
    const { data: users, error: usersError } = await db
      .from('users')
      .select('telegram_id')
      .in('id', userChunk)
      .not('telegram_id', 'is', null)
      .limit(5000);

    if (usersError) {
      throw usersError;
    }

    for (const row of users || []) {
      const tg = Number(row.telegram_id);
      if (Number.isInteger(tg)) {
        telegramIdSet.add(tg);
      }
    }
  }

  return [...telegramIdSet];
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
      total_amount: total,
      currency: product.currency || 'VND',
    })
    .select('id,status,total_amount,currency')
    .single();

  if (orderError) {
    throw orderError;
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

  if (typeof product.stock_quantity === 'number') {
    const { error: stockError } = await db
      .from('products')
      .update({ stock_quantity: Math.max(product.stock_quantity - qty, 0) })
      .eq('id', product.id);

    if (stockError) {
      throw stockError;
    }
  }

  await db.from('order_history').insert({
    order_id: order.id,
    changed_by: userId,
    status: 'confirmed',
    comment: 'Order created from Telegram bot',
  });

  return order;
}

async function notifyAdminsNewOrder(orderId, total, currency) {
  const adminIds = [...runtimeAdminIds].map((id) => Number(id)).filter(Number.isInteger);
  if (adminIds.length === 0) {
    return;
  }

  const message = `Đơn mới #${orderId}\\nTổng tiền: ${total} ${currency}`;
  for (const telegramId of adminIds) {
    try {
      await bot.telegram.sendMessage(telegramId, message);
    } catch (error) {
      // Ignore failed admin notifications to keep order flow responsive.
    }
  }
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

  return data || [];
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

function orderActionKeyboard(orderId, currentStatus) {
  const buttons = [];

  if (currentStatus !== 'confirmed') {
    buttons.push(Markup.button.callback('Confirm', `ordst:${orderId}:confirmed`));
  }
  if (currentStatus !== 'paid') {
    buttons.push(Markup.button.callback('Paid', `ordst:${orderId}:paid`));
  }
  if (currentStatus !== 'cancelled') {
    buttons.push(Markup.button.callback('Cancel', `ordst:${orderId}:cancelled`));
  }

  return Markup.inlineKeyboard([buttons.slice(0, 3)]);
}

async function loadAdminProducts() {
  const { data, error } = await db
    .from('products')
    .select('id,name,price,currency,stock_quantity,is_active,updated_at')
    .order('updated_at', { ascending: false })
    .limit(12);

  if (error) {
    throw error;
  }

  return data || [];
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

async function safeReply(ctx, text, extra) {
  try {
    await ctx.reply(text, extra);
  } catch (error) {
    // no-op
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

function compactProductButtonLabel(product) {
  const box = '\uD83D\uDCE6';
  const maxNameLength = 36;
  const rawName = String(product.name || '').trim();
  const shortName = rawName.length > maxNameLength ? `${rawName.slice(0, maxNameLength - 1)}...` : rawName;
  const priceText = `${formatPriceVnd(product.price)} ${product.currency || 'VND'}`;
  const stockText = Number.isFinite(Number(product.stock_quantity)) ? Number(product.stock_quantity) : '-';
  return `${shortName} | ${priceText} | ${box} ${stockText}`;
}

function formatDong(value) {
  return `${formatPriceVnd(value)}\u0111`;
}

function calcTierPrice(basePrice, ratio) {
  const base = Number(basePrice || 0);
  if (!Number.isFinite(base) || base <= 0) {
    return 0;
  }
  return Math.round(base * ratio);
}

function calcUnitPriceByQuantity(basePrice, quantity) {
  if (quantity >= 10) {
    return calcTierPrice(basePrice, 0.8);
  }
  if (quantity >= 5) {
    return calcTierPrice(basePrice, 0.875);
  }
  return Math.round(Number(basePrice || 0));
}

function buildProductDetailPanel(locale, product) {
  const box = '\uD83D\uDCE6';
  const money = '\uD83D\uDCB0';
  const chart = '\uD83D\uDCC9';
  const stockText = Number.isFinite(Number(product.stock_quantity)) ? Number(product.stock_quantity) : '-';
  const p5 = calcTierPrice(product.price, 0.875);
  const p10 = calcTierPrice(product.price, 0.8);

  if (locale === 'en') {
    return [
      `${box} ${product.name}`,
      `${money} Price: ${formatDong(product.price)}`,
      `${box} Stock: ${stockText}`,
      `${chart} Tier price:`,
      '',
      `- From 5: ${formatDong(p5)}`,
      `- From 10: ${formatDong(p10)}`,
      '',
      'Choose payment method:',
    ].join('\n');
  }

  return [
    `${box} ${product.name}`,
    `${money} Gi\u00e1: ${formatDong(product.price)}`,
    `${box} C\u00f2n: ${stockText}`,
    `${chart} Gi\u00e1 theo SL:`,
    '',
    `- T\u1eeb 5: ${formatDong(p5)}`,
    `- T\u1eeb 10: ${formatDong(p10)}`,
    '',
    'Ch\u1ecdn ph\u01b0\u01a1ng th\u1ee9c thanh to\u00e1n:',
  ].join('\n');
}

function buildCataloguePrompt(locale) {
  const point = '\uD83D\uDC49';
  return locale === 'en'
    ? `${point} CHOOSE A PRODUCT BELOW:`
    : `${point} CH\u1eccN S\u1ea2N PH\u1ea8M B\u00caN D\u01af\u1edaI:`;
}

function buildCatalogueKeyboard(products, locale) {
  const refresh = '\uD83D\uDD04';
  const trash = '\uD83D\uDDD1';
  const rows = products.map((p) => [Markup.button.callback(compactProductButtonLabel(p), `prd:${p.id}`)]);
  rows.push([
    Markup.button.callback(`${refresh} C\u1eadp nh\u1eadt`, 'catalogue_refresh'),
    Markup.button.callback(`${trash} X\u00f3a`, 'catalogue_close'),
  ]);
  return Markup.inlineKeyboard(rows);
}
async function sendCataloguePanel(ctx, locale, shouldEdit = false) {
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

  const text = buildCataloguePrompt(locale);
  const keyboard = buildCatalogueKeyboard(products, locale);

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

  if (typeof product.stock_quantity === 'number' && product.stock_quantity < qty) {
    await safeReply(ctx, t(locale, 'outOfStock'));
    return;
  }

  const order = await createSingleItemOrder(user.id, product, qty);
  const unitPrice = calcUnitPriceByQuantity(product.price, qty);
  const message = t(locale, 'orderCreated', {
    id: order.id,
    total: order.total_amount,
    currency: order.currency || 'VND',
  });

  await ctx.reply(`${message}\nSố lượng: ${qty}\nĐơn giá: ${unitPrice} ${order.currency || 'VND'}`);

  if (product.delivery_type === 'auto') {
    const accounts = [];
    for (let i = 0; i < qty; i += 1) {
      const account = await claimAutoAccount(product.id, order.id);
      if (!account?.account_data) {
        break;
      }
      accounts.push(account.account_data);
    }

    if (accounts.length > 0) {
      await ctx.reply(
        `Tài khoản của bạn (${accounts.length}/${qty}) - định dạng tk|mk|2fa:\n${accounts.map((a, idx) => `${idx + 1}. ${a}`).join('\n')}\n\nLưu ý: Đổi mật khẩu ngay sau khi nhận.`,
      );
      if (accounts.length < qty) {
        await ctx.reply('Còn thiếu một số tài khoản auto. Vui lòng nhắn admin để được cấp bổ sung.');
      }
    } else {
      await ctx.reply(
        'Tạm hết tài khoản auto. Vui lòng nhắn admin để được cấp bổ sung sau khi chuyển khoản thành công.',
      );
    }
  } else {
    const manualNote = product.manual_contact_note
      || 'Loại không auto. Sau khi chuyển khoản thành công, vui lòng nhắn admin để nhận tài khoản.';
    await ctx.reply(manualNote);
  }

  await notifyAdminsNewOrder(order.id, order.total_amount, order.currency || 'VND');
}

function adminProductsListText(products) {
  const lines = products.map((p) => {
    const status = p.is_active ? 'đang bán' : 'tạm ẩn';
    return `- ${p.name} | ${p.price} ${p.currency || 'VND'} | tồn:${p.stock_quantity ?? '-'} | ${status}`;
  });

  return [
    'QUAN LY SAN PHAM',
    '',
    ...lines,
    '',
    'Chọn sản phẩm để sửa / xóa / thêm tài khoản auto.',
  ].join('\n');
}

function buildAdminProductsKeyboard(products) {
  const rows = products.map((p) => {
    const icon = p.is_active ? 'ON' : 'OFF';
    const label = `[${icon}] ${String(p.name || '').slice(0, 28)}`;
    return [Markup.button.callback(label, `admprd:${p.id}`)];
  });

  rows.push([
    Markup.button.callback('Thêm mới', 'admin_add_product_start'),
    Markup.button.callback('Làm mới', 'admin_products_refresh'),
  ]);
  rows.push([Markup.button.callback('Cập nhật nhiều SP', 'admin_bulk_update_start')]);
  rows.push([Markup.button.callback('Đóng', 'admin_products_close')]);

  return Markup.inlineKeyboard(rows);
}

function adminProductDetailText(product) {
  return [
    `SAN PHAM: ${product.name}`,
    `Giá: ${product.price} ${product.currency || 'VND'}`,
    `Tồn kho: ${product.stock_quantity ?? '-'}`,
    `Trạng thái: ${product.is_active ? 'đang bán' : 'tạm ẩn'}`,
    `Kiểu giao: ${product.delivery_type === 'auto' ? 'auto' : 'thủ công'}`,
    '',
    'Tác vụ nhanh: Sửa giá, sửa tồn, bật/tắt, thêm TK auto.',
  ].join('\n');
}

function adminProductDetailKeyboard(product) {
  const toggleTo = product.is_active ? '0' : '1';
  return Markup.inlineKeyboard([
    [Markup.button.callback(product.is_active ? 'Tạm ẩn sản phẩm' : 'Mở bán lại', `prdtg:${product.id}:${toggleTo}`)],
    [Markup.button.callback('Thêm TK auto (nhiều dòng)', `admaddacc:${product.id}`)],
    [
      Markup.button.callback('Sửa giá', `admsetprice:${product.id}`),
      Markup.button.callback('Sửa tồn', `admsetstock:${product.id}`),
    ],
    [Markup.button.callback('Xóa sản phẩm', `admdelete:${product.id}`)],
    [Markup.button.callback('Danh sách', 'admin_products_v2')],
  ]);
}

async function sendAdminProductsPanel(ctx, shouldEdit = false) {
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

  const text = adminProductsListText(products);
  const keyboard = buildAdminProductsKeyboard(products);
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

function setPendingAdminInput(ctx, payload) {
  pendingAdminInputs.set(String(ctx.from.id), payload);
}

function clearPendingAdminInput(ctx) {
  pendingAdminInputs.delete(String(ctx.from.id));
}

async function registerChatMenuCommands() {
  const commands = [
    { command: 'start', description: 'Open main menu' },
    { command: 'catalogue', description: 'Browse products' },
    { command: 'history', description: 'View my orders' },
    { command: 'support', description: 'Contact support' },
    { command: 'language', description: 'Change language' },
    { command: 'admin', description: 'Open admin panel' },
  ];

  await bot.telegram.setMyCommands(commands);
}

bot.start(async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await sendHomePanel(ctx, user, locale);
});

bot.command('catalogue', async (ctx) => {
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

  const lines = orders.map((o) => `#${o.id} | ${STATUS_LABEL[o.status] || o.status} | ${o.total_amount} ${o.currency || 'VND'}`);
  await ctx.reply(lines.join('\n'));
});

bot.command('support', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const channels = await loadSupportChannels();
  if (channels.length === 0) {
    await ctx.reply(t(locale, 'supportEmpty'));
    return;
  }

  const lines = channels.map((c) => `- ${c.name}: ${c.value}`);
  await ctx.reply(lines.join('\n'));
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

  await ctx.reply(t(locale, 'adminPanel'), adminMenu);
});

bot.action('menu_admin', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  await ctx.reply(t(locale, 'adminPanel'), adminMenu);
});



bot.action('admin_add_product_help', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  await ctx.reply(
    'Thêm sản phẩm:\n'
    + '/listcategories\n'
    + '/addproduct <category_id>|<ten>|<gia>|<ton>|<currency>|<mo_ta>\n'
    + 'Ví dụ:\n'
    + '/addproduct 1111-2222|Tài khoản Premium|99000|10|VND|Sử dụng 30 ngày',
  );
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
    + 'category_id|ten|gia|ton|currency|mo_ta\n'
    + 'Ví dụ:\n'
    + '1111-2222|Tài khoản Premium|99000|10|VND|Sử dụng 30 ngày\n'
    + 'Nhập /cancel để hủy.',
  );
});

bot.action('admin_notify_category_help', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  await ctx.reply(
    'Thông báo theo loại hàng:\n'
    + '/listcategories\n'
    + '/notifycat <category_id> <noi_dung>\n'
    + 'Ví dụ:\n'
    + '/notifycat 1111-2222 Có deal mới cho nhóm sản phẩm này!',
  );
});

bot.command('claimadmin', async (ctx) => {
  const parts = ctx.message.text.trim().split(/\s+/);
  const secret = parts[1] || '';
  const user = await ensureUser(ctx);
  const locale = getLocale(user);

  if (!isSecretKeyValid(secret)) {
    await ctx.reply(t(locale, 'invalidKey'));
    return;
  }

  const telegramId = String(ctx.from.id);
  runtimeAdminIds.add(telegramId);

  if (user.role !== 'admin') {
    const { error } = await db
      .from('users')
      .update({ role: 'admin' })
      .eq('id', user.id);

    if (error) {
      throw error;
    }
  }

  await ctx.reply(t(locale, 'adminGranted'));
});

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
    await ctx.reply(t(locale, 'notifyUsage'));
    return;
  }

  try {
    await bot.telegram.sendMessage(targetId, message);
    await ctx.reply(`Đã gửi tin nhắn tới ${targetId}.`);
  } catch (error) {
    await ctx.reply(`Gửi thất bại: ${error.message}`);
  }
});

bot.command('broadcast', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.reply(t(locale, 'noAdmin'));
    return;
  }

  const message = getCommandPayload(ctx.message.text, 'broadcast');
  if (!message) {
    await ctx.reply(t(locale, 'broadcastUsage'));
    return;
  }

  let ids = [];
  try {
    ids = await getAllUserTelegramIds();
  } catch (error) {
    await ctx.reply(`Không tải được danh sách người dùng: ${error.message}`);
    return;
  }

  let success = 0;
  let failed = 0;
  for (const telegramId of ids) {
    try {
      await bot.telegram.sendMessage(telegramId, message);
      success += 1;
    } catch (error) {
      failed += 1;
    }
  }

  await ctx.reply(`Broadcast xong. Thành công: ${success}, thất bại: ${failed}.`);
});


bot.command('listcategories', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.reply(t(locale, 'noAdmin'));
    return;
  }

  const categories = await loadAllCategories();
  if (categories.length === 0) {
    await ctx.reply('Chưa có category.');
    return;
  }

  const lines = categories.map((c) => `${c.id} | ${c.name} | ${c.is_active ? 'active' : 'inactive'}`);
  await ctx.reply(`Danh sách category (${categories.length}):\n${lines.join('\\n')}`);
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
      + 'Dùng: /addproduct <category_id>|<ten>|<gia>|<ton>|<currency>|<mo_ta>\n'
      + 'Ví dụ: /addproduct 1111-2222|Tài khoản Premium|99000|10|VND|Sử dụng 30 ngày',
    );
    return;
  }

  const category = await loadCategoryById(parsed.data.categoryId);
  if (!category) {
    await ctx.reply('Không tìm thấy category_id.');
    return;
  }

  const created = await createProduct(parsed.data);
  await ctx.reply(
    `Đã thêm sản phẩm thành công.\n`
    + `ID: ${created.id}\n`
    + `Tên: ${created.name}\n`
    + `Giá: ${created.price} ${created.currency}\n`
    + `Tồn: ${created.stock_quantity}\n`
    + `Category: ${category.name}`,
  );
});

bot.action('admin_bulk_update_start', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  setPendingAdminInput(ctx, { type: 'bulk_update_products' });
  await ctx.answerCbQuery();
  await ctx.reply(
    'Nhập danh sách cập nhật nhiều sản phẩm (mỗi dòng 1 sản phẩm):\n'
    + 'product_id|gia|ton|active(0/1)\n'
    + 'active là tùy chọn.\n\n'
    + 'Ví dụ:\n'
    + '1111-2222|99000|20|1\n'
    + '3333-4444|49000|5|0\n'
    + '5555-6666|150000|10\n\n'
    + 'Nhập /cancel để hủy.',
  );
});

bot.command('notifycat', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.reply(t(locale, 'noAdmin'));
    return;
  }

  const payload = getCommandPayload(ctx.message.text, 'notifycat');
  const [categoryId, ...messageParts] = payload.split(/\s+/);
  const message = messageParts.join(' ').trim();

  if (!categoryId || !message) {
    await ctx.reply('Dùng: /notifycat <category_id> <noi_dung>');
    return;
  }

  const category = await loadCategoryById(categoryId);
  if (!category) {
    await ctx.reply('Không tìm thấy category_id.');
    return;
  }

  const targetIds = await getUserTelegramIdsByCategory(categoryId);
  if (targetIds.length === 0) {
    await ctx.reply(`Không có user nào đã mua trong loại "${category.name}".`);
    return;
  }

  let success = 0;
  let failed = 0;
  for (const telegramId of targetIds) {
    try {
      await bot.telegram.sendMessage(telegramId, `[${category.name}] ${message}`);
      success += 1;
    } catch (error) {
      failed += 1;
    }
  }

  await ctx.reply(`Notify category xong (${category.name}). Thành công: ${success}, thất bại: ${failed}.`);
});

bot.action('menu_catalogue', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await ctx.answerCbQuery();
  await sendCataloguePanel(ctx, locale);
});

bot.action(/^cat:(.+)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  const categoryId = ctx.match[1];
  await ctx.answerCbQuery();

  const products = await loadProductsByCategory(categoryId);
  if (products.length === 0) {
    await safeReply(ctx, t(locale, 'emptyCatalogue'));
    return;
  }

  const rows = products.map((p) => [Markup.button.callback(`${p.name} - ${p.price} ${p.currency || 'VND'}`, `prd:${p.id}`)]);
  rows.push([Markup.button.callback(locale === 'en' ? 'Back to menu' : 'V\u1ec1 menu', 'menu_catalogue')]);

  await ctx.reply(locale === 'en' ? 'Products:' : 'S\u1ea3n ph\u1ea9m:', Markup.inlineKeyboard(rows));
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
  await ctx.reply(details, Markup.inlineKeyboard([
    [
      Markup.button.callback('Mua x1', `buyq:${product.id}:1`),
      Markup.button.callback('Mua x3', `buyq:${product.id}:3`),
      Markup.button.callback('Mua x5', `buyq:${product.id}:5`),
    ],
    [Markup.button.callback('Nhập số lượng', `buyqinput:${product.id}`)],
    [Markup.button.callback('\uD83D\uDDD1 X\u00f3a', 'prd_close')],
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

  const lines = orders.map((o) => `#${o.id} | ${STATUS_LABEL[o.status] || o.status} | ${o.total_amount} ${o.currency || 'VND'}`);
  await ctx.reply(lines.join('\\n'));
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

  const lines = channels.map((c) => `- ${c.name}: ${c.value}`);
  await ctx.reply(lines.join('\\n'));
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
  await sendCataloguePanel(ctx, locale, true);
});

bot.action('catalogue_close', async (ctx) => {
  await ctx.answerCbQuery();
  try {
    await ctx.deleteMessage();
  } catch (error) {
    await safeReply(ctx, '\u0110\u00e3 \u0111\u00f3ng danh m\u1ee5c.');
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

  if (orders.length === 0) {
    await ctx.reply('Không có đơn mới.');
    return;
  }

  for (const order of orders) {
    const info = `#${order.id} | user:${order.user_id}\\n${order.total_amount} ${order.currency || 'VND'} | ${order.status}`;
    await ctx.reply(info, orderActionKeyboard(order.id, order.status));
  }
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

  const { data: updated, error } = await db
    .from('orders')
    .update({ status })
    .eq('id', orderId)
    .select('id,user_id,status,total_amount,currency')
    .single();

  if (error) {
    throw error;
  }

  await db.from('order_history').insert({
    order_id: updated.id,
    changed_by: user.id,
    status,
    comment: 'Updated from admin panel',
  });

  const { data: owner } = await db
    .from('users')
    .select('telegram_id')
    .eq('id', updated.user_id)
    .maybeSingle();

  if (owner?.telegram_id) {
    try {
      await bot.telegram.sendMessage(
        Number(owner.telegram_id),
        `Đơn #${updated.id} của bạn đã được cập nhật: ${updated.status}`,
      );
    } catch (errorNotify) {
      // no-op
    }
  }

  await ctx.answerCbQuery('Updated');
  await ctx.reply(t(locale, 'orderStatusUpdated', { id: updated.id, status: updated.status }));
});

bot.action('admin_products', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery();
  const products = await loadAdminProducts();

  if (products.length === 0) {
    await ctx.reply('Ch\u01b0a c\u00f3 s\u1ea3n ph\u1ea9m.');
    return;
  }

  for (const product of products) {
    const nextActive = product.is_active ? '0' : '1';
    const text = `${product.name} | ${product.price} ${product.currency || 'VND'} | tồn:${product.stock_quantity ?? '-'} | ${product.is_active ? 'đang bán' : 'tạm ẩn'}`;
    await ctx.reply(
      text,
      Markup.inlineKeyboard([
        [Markup.button.callback(product.is_active ? 'Tắt' : 'Bật', `prdtg:${product.id}:${nextActive}`)],
      ]),
    );
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
  await sendAdminProductsPanel(ctx);
});

bot.action('admin_products_refresh', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  if (!isAdmin(ctx, user)) {
    await ctx.answerCbQuery(t(locale, 'noAdmin'), { show_alert: true });
    return;
  }

  await ctx.answerCbQuery('Updated');
  await sendAdminProductsPanel(ctx, true);
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
  await ctx.reply(adminProductDetailText(product), adminProductDetailKeyboard(product));
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

  setPendingAdminInput(ctx, { type: 'add_accounts', productId });
  await ctx.answerCbQuery();
  await ctx.reply(
    `Nhập danh sách tài khoản AUTO cho "${product.name}" (mỗi dòng 1 tài khoản, định dạng tự do).\n`
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

  setPendingAdminInput(ctx, { type: 'edit_stock', productId });
  await ctx.answerCbQuery();
  await ctx.reply(
    `Nhập tồn mới cho "${product.name}" (số nguyên >= 0). Ví dụ: 50\n`
    + 'Nhập /cancel để hủy.',
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
    await hardDeleteProduct(productId);
    await ctx.answerCbQuery('Deleted');
    await ctx.reply(`Đã xóa sản phẩm: ${product.name}`);
  } catch (error) {
    await updateAdminProduct(productId, { is_active: false });
    await ctx.answerCbQuery('Disabled');
    await ctx.reply(`Sản phẩm có liên kết đơn hàng, đã chuyển tạm ẩn: ${product.name}`);
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
    clearPendingAdminInput(ctx);
    await ctx.reply('Đã hủy thao tác.');
    return;
  }

  if (text.startsWith('/')) {
    return next();
  }

  try {
    if (pending.type === 'add_product') {
      const parsed = parseAddProductPayload(text);
      if (!parsed.ok) {
        await ctx.reply('Sai cú pháp. Mẫu: category_id|ten|gia|ton|currency|mo_ta');
        return;
      }

      const category = await loadCategoryById(parsed.data.categoryId);
      if (!category) {
        await ctx.reply('Không tìm thấy category_id.');
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

    if (pending.type === 'bulk_update_products') {
      const parsed = parseBulkProductUpdates(text);
      if (parsed.total === 0) {
        await ctx.reply('Không có dòng hợp lệ. Hãy nhập theo mẫu product_id|gia|ton|active(0/1).');
        return;
      }

      let success = 0;
      let failed = 0;
      for (const item of parsed.updates) {
        try {
          await updateAdminProduct(item.productId, item.patch);
          success += 1;
        } catch (error) {
          failed += 1;
        }
      }

      clearPendingAdminInput(ctx);
      await ctx.reply(
        `Cập nhật nhiều sản phẩm xong.\n`
        + `Tổng dòng: ${parsed.total}\n`
        + `Hợp lệ: ${parsed.updates.length}\n`
        + `Thành công: ${success}\n`
        + `Thất bại: ${failed}\n`
        + `Sai cú pháp: ${parsed.invalidLines.length}`,
      );
      await sendAdminProductsPanel(ctx);
      return;
    }

    if (pending.type === 'add_accounts') {
      const product = await loadProductAny(pending.productId);
      if (!product) {
        clearPendingAdminInput(ctx);
        await ctx.reply('Không tìm thấy sản phẩm.');
        return;
      }

      const result = await addProductAccountsBulk(pending.productId, text);
      clearPendingAdminInput(ctx);
      await ctx.reply(
        `Đã xử lý ${result.total} dòng cho "${product.name}".\n`
        + `Thêm mới: ${result.added}\n`
        + `Bỏ qua (trùng): ${result.skipped}`,
      );
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

  await ctx.reply(
    `${t(locale, 'reportTitle')}\\n`
    + `- Total orders: ${report.totalOrders}\\n`
    + `- Confirmed: ${report.confirmedOrders}\\n`
    + `- Paid: ${report.paidOrders}\\n`
    + `- Revenue: ${report.revenue} VND`,
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

bot.launch().then(async () => {
  await registerChatMenuCommands();
  console.log('Bot launched.');
}).catch((err) => {
  console.error('Failed to launch bot', err);
  process.exit(1);
});

module.exports = bot;




