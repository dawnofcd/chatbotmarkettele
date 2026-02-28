-- Seed data for account-selling bot (idempotent)

-- 1) Category
insert into public.categories (name, slug, is_active)
values ('Tai khoan so', 'tai-khoan-so', true)
on conflict (slug) do update
set
  name = excluded.name,
  is_active = excluded.is_active;

-- 2) Products
insert into public.products (
  category_id,
  name,
  slug,
  description,
  delivery_type,
  manual_contact_note,
  price,
  currency,
  stock_quantity,
  is_active
)
select
  c.id,
  src.name,
  src.slug,
  src.description,
  src.delivery_type,
  src.manual_contact_note,
  src.price,
  src.currency,
  src.stock_quantity,
  src.is_active
from public.categories c
join (
  values
    (
      'Netflix Premium 1 thang',
      'netflix-premium-1-thang',
      'Tai khoan giao tu dong sau khi dat.',
      'auto',
      null,
      89000::numeric,
      'VND',
      20,
      true
    ),
    (
      'Canva Pro nang cap',
      'canva-pro-nang-cap',
      'Loai nay khong auto. Admin xu ly thu cong.',
      'manual',
      'Sau khi chuyen khoan thanh cong, vui long nhan admin de cung cap thong tin tai khoan.',
      149000::numeric,
      'VND',
      999,
      true
    ),
    (
      'Adobe Creative 3 thang Hotmail',
      'adobe-creative-3-thang-hotmail',
      'San pham mau dang tam an de test admin panel.',
      'manual',
      'Nhan admin de duoc cap thong tin sau khi thanh toan.',
      40000::numeric,
      'VND',
      297,
      false
    )
) as src(
  name,
  slug,
  description,
  delivery_type,
  manual_contact_note,
  price,
  currency,
  stock_quantity,
  is_active
) on true
where c.slug = 'tai-khoan-so'
on conflict (slug) do update
set
  category_id = excluded.category_id,
  name = excluded.name,
  description = excluded.description,
  delivery_type = excluded.delivery_type,
  manual_contact_note = excluded.manual_contact_note,
  price = excluded.price,
  currency = excluded.currency,
  stock_quantity = excluded.stock_quantity,
  is_active = excluded.is_active;

-- 3) Auto accounts for Netflix (insert missing only)
insert into public.product_accounts (product_id, account_data)
select p.id, src.account_data
from public.products p
join (
  values
    ('netflix01@gmail.com|MatKhau@123|2FA:ABCD-EFGH'),
    ('netflix02@gmail.com|MatKhau@123|2FA:IJKL-MNOP'),
    ('netflix03@gmail.com|MatKhau@123'),
    ('netflix04@gmail.com|MatKhau@123|2FA:QRST-UVWX'),
    ('netflix05@gmail.com|MatKhau@123')
) as src(account_data) on true
left join public.product_accounts pa
  on pa.product_id = p.id
 and pa.account_data = src.account_data
where p.slug = 'netflix-premium-1-thang'
  and pa.id is null;

-- 4) Support / payment channels
insert into public.support_channels (name, type, value, is_active)
values
  ('Hotline', 'phone', '1900 000', true),
  ('Telegram Support', 'telegram', 'https://t.me/your_support', true),
  ('Thanh toan VietQR', 'url', 'https://img.vietqr.io/image/VCB-123456789-compact2.png', true)
on conflict (type, value) do update
set
  name = excluded.name,
  is_active = excluded.is_active;
