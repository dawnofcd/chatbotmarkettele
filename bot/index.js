require('dotenv').config();

const crypto = require('crypto');
const { Telegraf, Markup } = require('telegraf');
const { createClient } = require('@supabase/supabase-js');

const botToken = process.env.TELEGRAM_TOKEN;
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const adminSecretKey = process.env.ADMIN_SECRET_KEY || '';
const adminTelegramIds = new Set(
  (process.env.ADMIN_TELEGRAM_IDS || '')
    .split(',')
    .map((id) => id.trim())
    .filter(Boolean),
);

if (!botToken || !supabaseUrl || !supabaseKey) {
  throw new Error('TELEGRAM_TOKEN, SUPABASE_URL, and SUPABASE_ANON_KEY must be defined.');
}

const bot = new Telegraf(botToken);
const supabase = createClient(supabaseUrl, supabaseKey);
const runtimeAdminIds = new Set(adminTelegramIds);

const TEXTS = {
  vi: {
    welcome: 'Chao mung den voi Tai Nguyen Hero! Cham vao mot nut de tiep tuc.',
    noAdmin: 'Ban khong co quyen admin.',
    adminPanel: 'Bang dieu khien admin',
    invalidKey: 'Key khong hop le.',
    adminGranted: 'Cap quyen admin thanh cong. Dung /admin de mo bang dieu khien.',
    notifyUsage: 'Cach dung: /notify <telegram_id> <noi_dung>',
    broadcastUsage: 'Cach dung: /broadcast <noi_dung>',
    loadingCatalogue: 'Danh muc dang tai...',
    emptyCatalogue: 'Chua co danh muc hoac san pham dang ban.',
    emptyHistory: 'Ban chua co don hang nao.',
    supportEmpty: 'Kenh ho tro dang duoc cap nhat.',
    langCurrent: 'Ngon ngu hien tai: Tieng Viet',
    orderCreated: 'Dat hang thanh cong. Ma don: #{id}\\nTong tien: {total} {currency}',
    outOfStock: 'San pham da het hang.',
    productMissing: 'Khong tim thay san pham.',
    orderStatusUpdated: 'Da cap nhat don #{id} -> {status}',
    reportTitle: 'Bao cao nhanh',
  },
  en: {
    welcome: 'Welcome to Tai Nguyen Hero! Tap a button to continue.',
    noAdmin: 'You do not have admin permission.',
    adminPanel: 'Admin dashboard',
    invalidKey: 'Invalid key.',
    adminGranted: 'Admin granted. Use /admin to open dashboard.',
    notifyUsage: 'Usage: /notify <telegram_id> <message>',
    broadcastUsage: 'Usage: /broadcast <message>',
    loadingCatalogue: 'Loading catalogue...',
    emptyCatalogue: 'No active categories/products yet.',
    emptyHistory: 'No orders yet.',
    supportEmpty: 'Support channels are being updated.',
    langCurrent: 'Current language: English',
    orderCreated: 'Order created. ID: #{id}\\nTotal: {total} {currency}',
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

function mainMenu(locale) {
  if (locale === 'en') {
    return Markup.inlineKeyboard([
      [
        Markup.button.callback('Catalogue', 'menu_catalogue'),
        Markup.button.callback('History', 'menu_history'),
      ],
      [
        Markup.button.callback('Support', 'menu_support'),
        Markup.button.callback('Language', 'menu_language'),
      ],
    ]);
  }

  return Markup.inlineKeyboard([
    [
      Markup.button.callback('Danh muc', 'menu_catalogue'),
      Markup.button.callback('Lich su', 'menu_history'),
    ],
    [
      Markup.button.callback('Ho tro', 'menu_support'),
      Markup.button.callback('Ngon ngu', 'menu_language'),
    ],
  ]);
}

const adminMenu = Markup.inlineKeyboard([
  [
    Markup.button.callback('Don moi', 'admin_orders_new'),
    Markup.button.callback('San pham', 'admin_products'),
  ],
  [Markup.button.callback('Thong ke', 'admin_reports')],
]);

async function ensureUser(ctx) {
  const telegramId = String(ctx.from.id);
  const roleFromEnv = adminTelegramIds.has(telegramId) ? 'admin' : 'customer';

  const { data: existingUser, error: selectError } = await supabase
    .from('users')
    .select('*')
    .eq('telegram_id', Number(telegramId))
    .maybeSingle();

  if (selectError) {
    throw selectError;
  }

  if (existingUser) {
    if (roleFromEnv === 'admin' && existingUser.role !== 'admin') {
      const { data: updatedUser, error: updateError } = await supabase
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

  const { data: newUser, error: insertError } = await supabase
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
  const { data, error } = await supabase
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

async function getAllUserTelegramIds() {
  const pageSize = 500;
  let from = 0;
  const ids = [];

  while (true) {
    const { data, error } = await supabase
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
  const { data, error } = await supabase
    .from('categories')
    .select('id,name')
    .eq('is_active', true)
    .order('name', { ascending: true });

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadProductsByCategory(categoryId) {
  const { data, error } = await supabase
    .from('products')
    .select('id,name,description,price,currency,stock_quantity,is_active')
    .eq('category_id', categoryId)
    .eq('is_active', true)
    .order('name', { ascending: true })
    .limit(50);

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadProduct(productId) {
  const { data, error } = await supabase
    .from('products')
    .select('id,name,description,price,currency,stock_quantity,is_active')
    .eq('id', productId)
    .eq('is_active', true)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data;
}

async function createSingleItemOrder(userId, product) {
  const total = Number(product.price || 0);

  const { data: order, error: orderError } = await supabase
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

  const { error: itemError } = await supabase
    .from('order_items')
    .insert({
      order_id: order.id,
      product_id: product.id,
      unit_price: total,
      quantity: 1,
      total_price: total,
    });

  if (itemError) {
    throw itemError;
  }

  if (typeof product.stock_quantity === 'number') {
    const { error: stockError } = await supabase
      .from('products')
      .update({ stock_quantity: Math.max(product.stock_quantity - 1, 0) })
      .eq('id', product.id);

    if (stockError) {
      throw stockError;
    }
  }

  await supabase.from('order_history').insert({
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

  const message = `Don moi #${orderId}\\nTong tien: ${total} ${currency}`;
  for (const telegramId of adminIds) {
    try {
      await bot.telegram.sendMessage(telegramId, message);
    } catch (error) {
      // Ignore failed admin notifications to keep order flow responsive.
    }
  }
}

async function loadRecentUserOrders(userId) {
  const { data, error } = await supabase
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
  const { data, error } = await supabase
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
  const { data, error } = await supabase
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
  const { data, error } = await supabase
    .from('products')
    .select('id,name,price,currency,stock_quantity,is_active')
    .order('updated_at', { ascending: false })
    .limit(12);

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadReport() {
  const [
    totalOrdersResp,
    paidOrdersResp,
    confirmedOrdersResp,
    revenueResp,
  ] = await Promise.all([
    supabase.from('orders').select('id', { count: 'exact', head: true }),
    supabase.from('orders').select('id', { count: 'exact', head: true }).eq('status', 'paid'),
    supabase.from('orders').select('id', { count: 'exact', head: true }).eq('status', 'confirmed'),
    supabase.from('orders').select('total_amount').eq('status', 'paid').limit(5000),
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

bot.start(async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await ctx.reply(t(locale, 'welcome'), mainMenu(locale));
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
    const { error } = await supabase
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
    await ctx.reply(`Da gui thong bao den ${targetId}.`);
  } catch (error) {
    await ctx.reply(`Gui that bai: ${error.message}`);
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
    await ctx.reply(`Khong tai duoc danh sach user: ${error.message}`);
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

  await ctx.reply(`Broadcast xong. Thanh cong: ${success}, that bai: ${failed}.`);
});

bot.action('menu_catalogue', async (ctx) => {
  const user = await ensureUser(ctx);
  const locale = getLocale(user);
  await ctx.answerCbQuery();

  const categories = await loadActiveCategories();
  if (categories.length === 0) {
    await ctx.reply(t(locale, 'emptyCatalogue'));
    return;
  }

  const keyboard = categories.map((c) => [Markup.button.callback(c.name, `cat:${c.id}`)]);
  await ctx.reply(t(locale, 'loadingCatalogue'), Markup.inlineKeyboard(keyboard));
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
  rows.push([Markup.button.callback(locale === 'en' ? 'Back to menu' : 'Ve menu', 'menu_catalogue')]);

  await ctx.reply(locale === 'en' ? 'Products:' : 'San pham:', Markup.inlineKeyboard(rows));
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

  const details = [
    `${product.name}`,
    `${locale === 'en' ? 'Price' : 'Gia'}: ${product.price} ${product.currency || 'VND'}`,
    `${locale === 'en' ? 'Stock' : 'Ton'}: ${product.stock_quantity ?? '-'}`,
    product.description || '',
  ].filter(Boolean).join('\\n');

  await ctx.reply(
    details,
    Markup.inlineKeyboard([
      [Markup.button.callback(locale === 'en' ? 'Buy now' : 'Dat ngay', `buy:${product.id}`)],
      [Markup.button.callback(locale === 'en' ? 'Back' : 'Quay lai', 'menu_catalogue')],
    ]),
  );
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

  if (typeof product.stock_quantity === 'number' && product.stock_quantity <= 0) {
    await safeReply(ctx, t(locale, 'outOfStock'));
    return;
  }

  const order = await createSingleItemOrder(user.id, product);
  const message = t(locale, 'orderCreated', {
    id: order.id,
    total: order.total_amount,
    currency: order.currency || 'VND',
  });

  await ctx.reply(message);
  await notifyAdminsNewOrder(order.id, order.total_amount, order.currency || 'VND');
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
    'Chon ngon ngu / Choose language',
    Markup.inlineKeyboard([
      [Markup.button.callback('Tieng Viet', 'lang:vi')],
      [Markup.button.callback('English', 'lang:en')],
    ]),
  );
});

bot.action(/^lang:(vi|en)$/, async (ctx) => {
  const user = await ensureUser(ctx);
  const target = ctx.match[1];
  await setUserLanguage(user.id, target);
  await ctx.answerCbQuery('OK');
  await ctx.reply(t(target, 'langCurrent'));
  await ctx.reply(t(target, 'welcome'), mainMenu(target));
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
    await ctx.reply('Khong co don moi.');
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

  const { data: updated, error } = await supabase
    .from('orders')
    .update({ status })
    .eq('id', orderId)
    .select('id,user_id,status,total_amount,currency')
    .single();

  if (error) {
    throw error;
  }

  await supabase.from('order_history').insert({
    order_id: updated.id,
    changed_by: user.id,
    status,
    comment: 'Updated from admin panel',
  });

  const { data: owner } = await supabase
    .from('users')
    .select('telegram_id')
    .eq('id', updated.user_id)
    .maybeSingle();

  if (owner?.telegram_id) {
    try {
      await bot.telegram.sendMessage(
        Number(owner.telegram_id),
        `Don #${updated.id} cua ban da duoc cap nhat: ${updated.status}`,
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
    await ctx.reply('Chua co san pham.');
    return;
  }

  for (const product of products) {
    const nextActive = product.is_active ? '0' : '1';
    const text = `${product.name} | ${product.price} ${product.currency || 'VND'} | ton:${product.stock_quantity ?? '-'} | ${product.is_active ? 'active' : 'inactive'}`;
    await ctx.reply(
      text,
      Markup.inlineKeyboard([
        [Markup.button.callback(product.is_active ? 'Tat san pham' : 'Mo san pham', `prdtg:${product.id}:${nextActive}`)],
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

  const { error } = await supabase
    .from('products')
    .update({ is_active: target })
    .eq('id', productId);

  if (error) {
    throw error;
  }

  await ctx.answerCbQuery('OK');
  await ctx.reply(`San pham ${productId} -> ${target ? 'active' : 'inactive'}`);
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
    await ctx.reply(`Co loi xay ra: ${err.message}`);
  } catch (nestedError) {
    // no-op
  }
  console.error('Bot error', err);
});

bot.launch().then(() => {
  console.log('Bot launched.');
}).catch((err) => {
  console.error('Failed to launch bot', err);
  process.exit(1);
});

module.exports = bot;
