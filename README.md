# Tai Nguyen Hero (Railway + Supabase)

## Goal
Run Telegram sales bot on Railway (long polling) and store data in Supabase Cloud.

## Required environment variables
- `TELEGRAM_TOKEN`
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

Optional:
- `ADMIN_TELEGRAM_IDS`
- `ADMIN_SECRET_KEY`

## Supabase setup
1. Open Supabase SQL Editor.
2. Run `docs/supabase_schema.sql`.
3. Run `docs/supabase_seed.sql`.

## Local run
1. `npm install`
2. Fill `.env`
3. `npm start`

## Deploy on Railway
1. Push this repo to GitHub.
2. Railway -> New Project -> Deploy from GitHub repo.
3. In Railway Variables, set:
   - `TELEGRAM_TOKEN`
   - `SUPABASE_URL`
   - `SUPABASE_ANON_KEY`
   - `ADMIN_TELEGRAM_IDS` (optional)
   - `ADMIN_SECRET_KEY` (optional)
4. Start command: `npm start` (already in `railway.json`).
5. Redeploy and check logs for `Bot launched.`

## Available bot functions
- User: `/start`, Danh muc, Lich su, Ho tro, Ngon ngu, Dat ngay.
- Admin: `/admin`, `/claimadmin`, Don moi, cap nhat trang thai don, bat/tat san pham, thong ke.
- Admin utility: `/notify`, `/broadcast`.

## Notes
- Keep only one running instance when using long polling.
- Current RLS policies are permissive for anon-key bot integration; tighten before scaling.
