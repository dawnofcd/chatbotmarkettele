-- Seed sample data
insert into public.categories (name, slug)
values
  ('Dien thoai', 'dien-thoai'),
  ('Phu kien', 'phu-kien')
on conflict (slug) do nothing;

insert into public.products (category_id, name, slug, description, delivery_type, manual_contact_note, price, currency, stock_quantity, is_active)
select c.id, 'Netflix Premium 1 thang', 'netflix-premium-1-thang', 'Tai khoan giao tu dong sau khi dat.', 'auto', null, 89000, 'VND', 20, true
from public.categories c where c.slug = 'dien-thoai'
on conflict (slug) do nothing;

insert into public.products (category_id, name, slug, description, delivery_type, manual_contact_note, price, currency, stock_quantity, is_active)
select c.id, 'Canva Pro nang cap', 'canva-pro-nang-cap', 'Loai nay khong auto. Admin xu ly thu cong.', 'manual', 'Sau khi chuyen khoan thanh cong, vui long nhan admin de cung cap thong tin tai khoan.', 149000, 'VND', 999, true
from public.categories c where c.slug = 'phu-kien'
on conflict (slug) do nothing;

insert into public.products (category_id, name, slug, description, delivery_type, manual_contact_note, price, currency, stock_quantity, is_active)
select c.id, 'Tai nghe Bluetooth', 'tai-nghe-bluetooth', 'Tai nghe chong on co ban', 'manual', 'San pham vat ly - admin xac nhan sau khi thanh toan.', 990000, 'VND', 25, true
from public.categories c where c.slug = 'phu-kien'
on conflict (slug) do nothing;

-- Product de test flow admin bat/tat san pham
insert into public.products (category_id, name, slug, description, delivery_type, manual_contact_note, price, currency, stock_quantity, is_active)
select c.id, 'Adobe Creative 3 thang Hotmail', 'adobe-creative-3-thang-hotmail', 'San pham mau dang tam an de test admin panel.', 'manual', 'Nhan admin de duoc cap thong tin sau khi thanh toan.', 40000, 'VND', 297, false
from public.categories c where c.slug = 'dien-thoai'
on conflict (slug) do nothing;

-- Seed account auto theo tung account_data de script idempotent
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

insert into public.support_channels (name, type, value, is_active)
values
  ('Hotline', 'phone', '1900 000', true),
  ('Telegram Support', 'telegram', 'https://t.me/your_support', true),
  ('Thanh toan VietQR', 'url', 'https://img.vietqr.io/image/VCB-123456789-compact2.png', true)
on conflict (type, value) do update
set
  name = excluded.name,
  is_active = excluded.is_active;
