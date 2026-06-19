# Pipeline

How data flows from the Meta Marketing API into Postgres.

## Script groups

| Group | Scripts | Tables |
|-------|---------|--------|
| Entity attributes | `account_atribute`, `campaign_atribute`, `adset_atribute`, `ad_atribute`, `creative_atribute` | `property_accounts`, `property_campaigns`, `property_adsets`, `property_ads`, `property_creatives` |
| Change history | `actions` | `actions`, `actions_log` |
| Performance — daily | `insights`, `insights_update` | `insights`, `insights_log` |
| Performance — breakdowns | `insights_breakdowns_update` | `insights_breakdowns_demographic`, `insights_breakdowns_placement` |
| Performance — intraday | `intraday_insights` | `intraday_insights` |

Each script is standalone: it reads `.env`, connects to Postgres, talks to Meta,
writes its tables, and exits. Run them from cron, a scheduler, or by hand.

## Shared infrastructure (`utils.py`)

- DB: `connection_to_database`, `reconnection_to_database`.
- Schema-safe writes: `get_table_columns`, `store_object` (insert / upsert,
  filtered to columns that exist), `store_rows`.
- Normalisation: `modify_object_data`, `modify_action_data`, `parse_fb_datetime`.
- Account filter: `get_allowed_account_ids` (honours `ACCOUNT_IDS`).
- Rate limit: `check_limit`, `rate_limit_exceeded`, `is_rate_limit_error`,
  `is_token_error`, `backoff_seconds`, `MAX_RETRIES`.

## Key behaviours

### Entity attributes (`*_atribute`)

Each run selects objects that either changed recently (joined against `actions`)
or appear in `insights` without a property row yet, skipping anything tombstoned
in `deleted_objects`. For each object it fetches the current attributes and writes
them, dropping API fields that aren't columns. A 404-style Meta error
(code=100, subcode 33/1487221) tombstones the object. `ad_atribute` upserts on
`id`; the others delete-then-insert when refreshing an existing row.

### Change history (`actions`)

Pulls ad activity (bid/budget/status changes, etc.) per account per day, skipping
`(account, day)` pairs already in `actions_log`. Datetimes are normalised to ISO
before insert. Date range overridable via `ACTIONS_START_DATE` / `ACTIONS_END_DATE`.

### Daily performance (`insights`, `insights_update`)

Per-ad daily metrics with attribution windows `1d_view, 1d_click, 7d_click,
28d_click`. `insights` is a plain date-range loader (good for backfill).
`insights_update` is the recurring job: it skips days already in `insights_log`
but always re-fetches the last 7 days for late attribution. That refetch is
**atomic per (account, day)** — the old rows are deleted and the new ones inserted
in a single transaction that runs only after a successful fetch, so a failed or
rate-limited call leaves the previous snapshot intact.

### Breakdowns (`insights_breakdowns_update`)

Two separate API passes per day — Meta does not allow these breakdown sets in one
call:

- **demographic** — `age` × `gender` → `insights_breakdowns_demographic`
- **placement** — `publisher_platform` × `platform_position` × `impression_device`
  → `insights_breakdowns_placement`

Same incremental + atomic-refetch logic as `insights_update`. Breakdown
dimensions must not be listed in `fields` (Meta returns code=100); they arrive via
the `breakdowns` parameter.

### Intraday (`intraday_insights`)

`date_preset=today`, full DELETE + INSERT per account each run, so the table always
holds the latest intraday state. The delete happens only after a complete fetch,
so a partial/failed fetch never overwrites a good snapshot.

## Backfill

`backfill/*.sh` drive the loaders in date-range batches with a sleep between
batches to stay under rate limits:

```bash
./backfill/backfill_insights.sh             2026-01-01 2026-03-31
./backfill/backfill_actions.sh              2026-01-01 2026-03-31
./backfill/backfill_insights_breakdowns.sh  2026-01-01 2026-03-31
```

Tunable via env: `PYTHON`, `BATCH_DAYS`, `SLEEP_BETWEEN`, `LOG_DIR`.

## Rate limiting

Before fetching an account, scripts read Meta's `x-fb-ads-insights-throttle`
header (`check_limit`). If `app_id_util_pct ≥ 60` or `acc_id_util_pct ≥ 70` the
account is requeued after a pause. Transient errors (rate-limit subcode, network)
retry with exponential back-off (5 → 10 → 20 s) up to `MAX_RETRIES`; an invalid
token (code 190) halts the run.

## Analysis layer (optional)

This repo stops at the warehouse tables. For natural-language SQL analysis over
the same database, use
[data_analyst-fb_audit](https://github.com/KhatkevichKirill/data_analyst-fb_audit):

1. Point analyst `DB_*` at this Postgres instance (read-only user).
2. Run `data-analyst init-view` to create `v_insights_daily` over `insights`.
3. Start the notebook UI with `data-analyst serve`.

The analyst knowledge base documents all tables written by these loaders,
including breakdown tables and TEXT-metric casting conventions.
