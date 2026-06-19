# fb_audit

Generic **Meta Ads → PostgreSQL ETL**. A set of small, standalone Python scripts
that pull entity attributes, change history, and performance insights from the
Meta Marketing API and load them into Postgres — incrementally, rate-limit aware,
and safe to re-run.

This is the open, reusable core extracted from a larger private project. If you
need a dependable way to warehouse your own Meta Ads data, clone it and point it
at your account.

## What it collects

| Script | Pulls | Writes to |
|--------|-------|-----------|
| `account_atribute.py` | account technical attributes | `property_accounts` |
| `campaign_atribute.py` | campaign attributes | `property_campaigns` |
| `adset_atribute.py` | ad set attributes (targeting, budget, …) | `property_adsets` |
| `ad_atribute.py` | ad attributes (upsert on id) | `property_ads` |
| `creative_atribute.py` | creative attributes (copy, CTA, UTM, …) | `property_creatives` |
| `actions.py` | change history (bid/budget/status events) | `actions`, `actions_log` |
| `insights.py` | daily ad performance (date-range) | `insights`, `insights_log` |
| `insights_update.py` | daily incremental + atomic last-7-day refetch | `insights`, `insights_log` |
| `insights_breakdowns_update.py` | performance by age×gender and by placement | `insights_breakdowns_demographic`, `insights_breakdowns_placement` |
| `intraday_insights.py` | today's snapshot, full refresh each run | `intraday_insights` |

`backfill/` has shell wrappers that run the loaders in date-range batches.

## Quick start

```bash
git clone <this repo> && cd fb_audit
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env        # then fill in your token + DB creds
psql "$DATABASE_URL" -f schema_properties.sql
psql "$DATABASE_URL" -f schema_breakdowns.sql

python insights_update.py   # daily performance
python insights_breakdowns_update.py
```

Every script reads config from a `.env` file (see `.env.example`). Point at a
different file with `FB_AUDIT_ENV=/path/to/.env`. Restrict to specific accounts
with `ACCOUNT_IDS=123,456`.

## Design notes

- **Incremental + idempotent.** Each loader records what it has fetched in a
  `*_log` table and skips work already done. `insights_update` additionally
  re-fetches the last 7 days to capture late-attributed conversions.
- **Crash-safe refetch.** The daily insights / breakdowns refetch is atomic per
  `(account, day)`: old rows are replaced only inside one transaction that runs
  *after* a successful API fetch, so a failed or rate-limited call preserves the
  previous snapshot instead of leaving a gap.
- **Rate-limit aware.** A shared throttle check (`utils.check_limit`) reads Meta's
  usage header once per account; over-threshold or transient errors sleep and
  requeue with exponential back-off; an invalid token halts immediately.
- **Schema-safe writes.** Inserts only touch columns that actually exist, so a
  changing Meta API surface won't break the load.

See `docs/PIPELINE.md` for the full data flow and `docs/architecture.mmd` for a
diagram.

## Requirements

Python 3.9+, PostgreSQL, a Meta Marketing API access token with `ads_read`.
Dependencies in `requirements.txt`.

## License

MIT.
