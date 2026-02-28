-- Supabase schema for Telegram sales bot
create extension if not exists pgcrypto;

create table if not exists public.users (
  id uuid primary key default gen_random_uuid(),
  telegram_id bigint unique not null,
  username text,
  display_name text,
  role text not null default 'customer' check (role in ('customer', 'admin')),
  language_code text not null default 'vi' check (language_code in ('vi', 'en')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.categories (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  slug text unique,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.products (
  id uuid primary key default gen_random_uuid(),
  category_id uuid references public.categories(id) on delete set null,
  name text not null,
  slug text unique,
  description text,
  delivery_type text not null default 'manual' check (delivery_type in ('auto', 'manual')),
  manual_contact_note text,
  price numeric(12,2) not null default 0,
  currency text not null default 'VND',
  media_url text,
  stock_quantity integer not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.products
  add column if not exists delivery_type text not null default 'manual';

alter table public.products
  add column if not exists manual_contact_note text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'products_delivery_type_check'
  ) then
    alter table public.products
      add constraint products_delivery_type_check
      check (delivery_type in ('auto', 'manual'));
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'products_stock_quantity_non_negative_check'
  ) then
    alter table public.products
      add constraint products_stock_quantity_non_negative_check
      check (stock_quantity >= 0);
  end if;
end
$$;

create table if not exists public.product_accounts (
  id uuid primary key default gen_random_uuid(),
  product_id uuid not null references public.products(id) on delete cascade,
  account_data text not null,
  is_used boolean not null default false,
  used_order_id uuid,
  used_at timestamptz,
  created_at timestamptz not null default now()
);

create table if not exists public.orders (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  status text not null default 'draft' check (status in ('draft', 'confirmed', 'paid', 'cancelled')),
  total_amount numeric(12,2) not null default 0,
  currency text not null default 'VND',
  payment_method text,
  shipping_address text,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'product_accounts_used_order_id_fkey'
  ) then
    alter table public.product_accounts
      add constraint product_accounts_used_order_id_fkey
      foreign key (used_order_id)
      references public.orders(id)
      on delete set null;
  end if;
end
$$;

create table if not exists public.order_items (
  id uuid primary key default gen_random_uuid(),
  order_id uuid not null references public.orders(id) on delete cascade,
  product_id uuid not null references public.products(id),
  unit_price numeric(12,2) not null,
  quantity integer not null check (quantity > 0),
  total_price numeric(12,2) not null,
  created_at timestamptz not null default now()
);

create table if not exists public.order_history (
  id uuid primary key default gen_random_uuid(),
  order_id uuid not null references public.orders(id) on delete cascade,
  changed_by uuid references public.users(id) on delete set null,
  status text not null,
  comment text,
  created_at timestamptz not null default now()
);

create table if not exists public.support_channels (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  type text not null check (type in ('phone', 'telegram', 'url', 'email')),
  value text not null,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_users_telegram_id on public.users (telegram_id);
create index if not exists idx_orders_user_id on public.orders (user_id);
create index if not exists idx_orders_status on public.orders (status);
create index if not exists idx_products_category_id on public.products (category_id);
create index if not exists idx_products_is_active on public.products (is_active);
create index if not exists idx_product_accounts_product_id on public.product_accounts (product_id);
create index if not exists idx_product_accounts_is_used on public.product_accounts (is_used);
create unique index if not exists uq_support_channels_type_value on public.support_channels (type, value);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_users_updated_at on public.users;
create trigger trg_users_updated_at before update on public.users
for each row execute function public.set_updated_at();

drop trigger if exists trg_categories_updated_at on public.categories;
create trigger trg_categories_updated_at before update on public.categories
for each row execute function public.set_updated_at();

drop trigger if exists trg_products_updated_at on public.products;
create trigger trg_products_updated_at before update on public.products
for each row execute function public.set_updated_at();

drop trigger if exists trg_orders_updated_at on public.orders;
create trigger trg_orders_updated_at before update on public.orders
for each row execute function public.set_updated_at();

drop trigger if exists trg_support_channels_updated_at on public.support_channels;
create trigger trg_support_channels_updated_at before update on public.support_channels
for each row execute function public.set_updated_at();

alter table public.users enable row level security;
alter table public.categories enable row level security;
alter table public.products enable row level security;
alter table public.product_accounts enable row level security;
alter table public.orders enable row level security;
alter table public.order_items enable row level security;
alter table public.order_history enable row level security;
alter table public.support_channels enable row level security;

-- NOTE: these policies are permissive for quick bot integration with anon key.
-- tighten later when adding JWT claims / service-role backend.
drop policy if exists p_users_all on public.users;
create policy p_users_all on public.users for all using (true) with check (true);

drop policy if exists p_categories_read on public.categories;
create policy p_categories_read on public.categories for select using (is_active = true);

drop policy if exists p_categories_write on public.categories;
create policy p_categories_write on public.categories for all using (true) with check (true);

drop policy if exists p_products_read on public.products;
create policy p_products_read on public.products for select using (is_active = true);

drop policy if exists p_products_write on public.products;
create policy p_products_write on public.products for all using (true) with check (true);

drop policy if exists p_product_accounts_all on public.product_accounts;
create policy p_product_accounts_all on public.product_accounts for all using (true) with check (true);

drop policy if exists p_orders_all on public.orders;
create policy p_orders_all on public.orders for all using (true) with check (true);

drop policy if exists p_order_items_all on public.order_items;
create policy p_order_items_all on public.order_items for all using (true) with check (true);

drop policy if exists p_order_history_all on public.order_history;
create policy p_order_history_all on public.order_history for all using (true) with check (true);

drop policy if exists p_support_read on public.support_channels;
create policy p_support_read on public.support_channels for select using (is_active = true);

drop policy if exists p_support_write on public.support_channels;
create policy p_support_write on public.support_channels for all using (true) with check (true);
