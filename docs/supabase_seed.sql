-- Seed sample data
insert into public.categories (name, slug)
values
  ('Dien thoai', 'dien-thoai'),
  ('Phu kien', 'phu-kien')
on conflict (slug) do nothing;

insert into public.products (category_id, name, slug, description, price, currency, stock_quantity, is_active)
select c.id, 'iPhone 15', 'iphone-15', 'iPhone 15 128GB', 19990000, 'VND', 10, true
from public.categories c where c.slug = 'dien-thoai'
on conflict (slug) do nothing;

insert into public.products (category_id, name, slug, description, price, currency, stock_quantity, is_active)
select c.id, 'Samsung S24', 'samsung-s24', 'Samsung Galaxy S24 256GB', 18990000, 'VND', 8, true
from public.categories c where c.slug = 'dien-thoai'
on conflict (slug) do nothing;

insert into public.products (category_id, name, slug, description, price, currency, stock_quantity, is_active)
select c.id, 'Tai nghe Bluetooth', 'tai-nghe-bluetooth', 'Tai nghe chong on co ban', 990000, 'VND', 25, true
from public.categories c where c.slug = 'phu-kien'
on conflict (slug) do nothing;

insert into public.support_channels (name, type, value, is_active)
values
  ('Hotline', 'phone', '1900 000', true),
  ('Telegram Support', 'telegram', 'https://t.me/your_support', true)
on conflict do nothing;
