# Architecture overview (Supabase)

## Vision
Build a Telegram sales bot with 4 customer actions (Danh muc, Lich su, Ho tro, Ngon ngu) and a separate admin flow (orders, products, reports).

## Runtime
- Platform: Railway
- Bot mode: long polling
- Data: Supabase Cloud (PostgreSQL)

## Data model
- `users`: bot users + role + language
- `categories`: product groups
- `products`: catalogue inventory
- `orders`: order header
- `order_items`: order line items
- `order_history`: status audit
- `support_channels`: support links/phone

Detailed DDL: `docs/supabase_schema.sql`
Seed sample data: `docs/supabase_seed.sql`

## Bot flows
1. `/start`
- upsert/find user by `telegram_id`
- show main menu

2. Customer menu
- `Danh muc`: list categories -> list products -> detail -> `Dat ngay`
- `Lich su`: show latest orders
- `Ho tro`: show active support channels
- `Ngon ngu`: switch `vi/en`

3. Admin menu (`/admin`)
- `Don moi`: list draft/confirmed orders + quick status update
- `San pham`: list products + toggle active/inactive
- `Thong ke`: total orders, confirmed, paid, revenue

4. Utility commands
- `/notify <telegram_id> <message>`
- `/broadcast <message>`

## Constraints
- Polling mode should run as single instance to avoid update conflicts.
- For production hardening, replace permissive RLS with stricter policies.
