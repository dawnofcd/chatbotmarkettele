# Tai Nguyen Hero (Railway + PostgreSQL)

## Goal
Run Telegram sales bot on Railway (long polling) and store data in PostgreSQL.

## Required environment variables
- `TELEGRAM_TOKEN`
- `DATABASE_URL`

Optional:
- `PGSSL` (`true` by default)
- `ADMIN_TELEGRAM_IDS`
- `ADMIN_SECRET_KEY`
- `MMOBANK_ACCOUNT_NO`

Optional:
- `MMOBANK_WEBHOOK_PATH` (default: `/mmobank/webhook`)
- `MMOBANK_SECRET_KEY`
- `MMOBANK_BANK_CODE`

## Database setup
1. Open your PostgreSQL SQL console/tool.
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
   - `DATABASE_URL`
   - `PGSSL=true` (set `PGSSL=false` only for local non-SSL DB)
   - `ADMIN_TELEGRAM_IDS` (optional)
   - `ADMIN_SECRET_KEY` (optional)
   - `MMOBANK_ACCOUNT_NO`
   - `MMOBANK_WEBHOOK_PATH` (optional, default `/mmobank/webhook`)
   - `MMOBANK_SECRET_KEY` (optional)
   - `MMOBANK_BANK_CODE` (optional)
4. Start command: `npm start` (already in `railway.json`).
5. Redeploy and check logs for `Bot launched.`

## MMOBank webhook setup
1. Deploy bot, then get your public app URL from Railway.
2. Set webhook URL in MMOBank to:
   - `https://<your-app>.up.railway.app/mmobank/webhook`
   - If you changed `MMOBANK_WEBHOOK_PATH`, use that path instead.
3. If configured, MMOBank will call this endpoint with header `secret-key: <MMOBANK_SECRET_KEY>`.
4. Bot auto-marks order as `paid` when transfer content contains the order code `DH...`, amount is valid, and account number matches `MMOBANK_ACCOUNT_NO` (when provided).

## Available bot functions
- User: `/start`, Danh muc, Lich su, Ho tro, Ngon ngu, Dat ngay.
- Admin: `/admin`, `/claimadmin`, Don moi, cap nhat trang thai don, bat/tat san pham, thong ke.
- Admin utility: `/notify`, `/broadcast`.

## Admin guide
- See `docs/admin_guide.md`

## Notes
- Keep only one running instance when using long polling.
- SQL file names still use `supabase_*` for backward compatibility, but they are standard PostgreSQL scripts.
