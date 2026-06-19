"""
insights_breakdowns_update — per-ad daily insights, broken down by audience and
placement dimensions, written to two separate tables.

Two independent Meta Marketing API passes per day:
  * demographic — breakdowns = age, gender            -> insights_breakdowns_demographic
  * placement   — breakdowns = publisher_platform,
                  platform_position, impression_device -> insights_breakdowns_placement

Why two passes: Meta does not allow demographic and placement breakdowns in the
same insights call. They must be requested separately.

Behaviour:
  * Incremental: an account/day already present in the corresponding *_log table
    is skipped...
  * ...EXCEPT the last `REFETCH_DAYS` days, which are always re-fetched to pick up
    late-attributed conversions.
  * The re-fetch is atomic per (account, day, pass): the old rows are deleted and
    the fresh rows inserted in a single transaction that only runs AFTER a
    successful Meta fetch. A failed/rate-limited fetch therefore preserves the
    previous snapshot instead of leaving a hole.
  * Rate-limit aware: throttle header is checked once per account; over-threshold
    or transient errors requeue the account with back-off, up to MAX_RETRIES.
  * Date range overridable via INSIGHTS_START_DATE / INSIGHTS_END_DATE
    (used by scripts/backfill/backfill_insights_breakdowns.sh).

Config comes from a .env file (FB_ACCESS_TOKEN, DB_*, FB_GRAPH_API_VERSION); path overridable via FB_AUDIT_ENV.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError, FacebookBadObjectError

from utils import (
    connection_to_database,
    get_allowed_account_ids,
    check_limit,
    rate_limit_exceeded,
    is_rate_limit_error,
    is_token_error,
    MAX_RETRIES,
)


load_dotenv(os.environ.get("FB_AUDIT_ENV", ".env"))

access_token = os.environ["FB_ACCESS_TOKEN"]
host = os.environ["DB_HOST"]
port = os.environ["DB_PORT"]
database = os.environ["DB_NAME"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]

graph_api_version = os.environ.get("FB_GRAPH_API_VERSION", "23.0")

DELTA_DAYS = 30          # default look-back window when no env override is given
REFETCH_DAYS = 7         # always re-fetch the last N days (late attribution)
POLL_TIMEOUT_SECONDS = 600  # hard cap on async report polling per account

action_attribution_windows = ["1d_view", "1d_click", "7d_click", "28d_click"]

# Breakdown group A — demographic (age x gender)
breakdowns_demographic = ["age", "gender"]
# Breakdown group B — placement. impression_device goes in breakdowns, NOT fields.
breakdowns_placement = ["publisher_platform", "platform_position", "impression_device"]

# Shared base fields. Breakdown dimensions must NOT be listed here — Meta rejects
# that with code=100. They come back automatically via the `breakdowns` param and
# export_all_data() exposes them as attributes.
fields_base = [
    "account_id", "campaign_id", "adset_id", "ad_id",
    "date_start", "date_stop", "impressions", "reach",
    "clicks", "spend", "unique_inline_link_clicks",
    "inline_link_clicks", "inline_post_engagement",
    "estimated_ad_recallers", "estimated_ad_recall_rate",
    "objective", "unique_clicks", "actions", "action_values",
    "outbound_clicks", "unique_actions", "unique_outbound_clicks",
    "video_p25_watched_actions", "video_p50_watched_actions",
    "video_p75_watched_actions", "video_p95_watched_actions",
    "results", "cost_per_result",
]

table_demographic = "insights_breakdowns_demographic"
table_log_demographic = "insights_breakdowns_demographic_log"
table_placement = "insights_breakdowns_placement"
table_log_placement = "insights_breakdowns_placement_log"


def list_days(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    days = []
    while start <= end:
        days.append(start.strftime("%Y-%m-%d"))
        start += timedelta(days=1)
    return days


def dict2json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def modify_object(object_data):
    for key in object_data.keys():
        if isinstance(object_data[key], (list, dict)):
            object_data[key] = dict2json(object_data[key])
    return object_data


def ensure_breakdown_tables(cur, connection):
    """Create the two breakdown tables + their idempotency logs if absent.

    Self-contained like intraday_insights.ensure_intraday_table, so the script
    can bootstrap a fresh database without a separate migration step.
    """
    metric_cols = """
        impressions TEXT, reach TEXT, clicks TEXT, spend TEXT,
        unique_inline_link_clicks TEXT, inline_link_clicks TEXT,
        inline_post_engagement TEXT, estimated_ad_recallers TEXT,
        estimated_ad_recall_rate TEXT, objective TEXT, unique_clicks TEXT,
        actions JSONB, action_values JSONB, outbound_clicks JSONB,
        unique_actions JSONB, unique_outbound_clicks JSONB,
        video_p25_watched_actions JSONB, video_p50_watched_actions JSONB,
        video_p75_watched_actions JSONB, video_p95_watched_actions JSONB,
        results JSONB, cost_per_result JSONB
    """
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table_demographic} (
            account_id TEXT, campaign_id TEXT, adset_id TEXT, ad_id TEXT,
            date_start DATE, date_stop DATE,
            age TEXT, gender TEXT,
            {metric_cols}
        )"""
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_ibd_account ON {table_demographic}(account_id, date_start)"
    )
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table_log_demographic} (
            account_id TEXT, date DATE, with_data BOOLEAN, recording_date TIMESTAMP
        )"""
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_ibd_log_account ON {table_log_demographic}(account_id, date)"
    )
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table_placement} (
            account_id TEXT, campaign_id TEXT, adset_id TEXT, ad_id TEXT,
            date_start DATE, date_stop DATE,
            publisher_platform TEXT, platform_position TEXT, impression_device TEXT,
            {metric_cols}
        )"""
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_ibp_account ON {table_placement}(account_id, date_start)"
    )
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table_log_placement} (
            account_id TEXT, date DATE, with_data BOOLEAN, recording_date TIMESTAMP
        )"""
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_ibp_log_account ON {table_log_placement}(account_id, date)"
    )
    connection.commit()


def store_rows(cur, table, rows):
    """Schema-safe multi-row insert: drops any key that isn't a real column."""
    if not rows:
        return
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table,),
    )
    valid = {r[0] for r in cur.fetchall()}
    for row in rows:
        filtered = {k: v for k, v in row.items() if k in valid}
        if not filtered:
            continue
        fields = ",".join(filtered.keys())
        values = ",".join(map(lambda x: f"%({x})s", filtered.keys()))
        cur.execute(f"INSERT INTO {table} ({fields}) VALUES ({values})", filtered)


def replace_account_day(cur, connection, table, table_log, account_id, day, rows, recording_date, with_data):
    """Atomically replace one (account_id, day) slice for a single breakdown pass.

    Call ONLY after a successful Meta fetch and in-memory row preparation. Deletes
    the old rows + old log row, inserts the fresh log row and any fetched rows,
    then commits — in one transaction. The previous snapshot is destroyed only
    once a valid replacement is ready; any failure rolls back and re-raises,
    leaving the old data intact. A zero-row Meta response is a valid fresh
    snapshot (replaces old rows, log row with_data=false).
    """
    try:
        cur.execute(f"DELETE FROM {table}     WHERE date_start = %s::date AND account_id = %s", (day, account_id))
        cur.execute(f"DELETE FROM {table_log} WHERE date       = %s::date AND account_id = %s", (day, account_id))
        cur.execute(f"INSERT INTO {table_log} VALUES (%s, %s, %s, %s)", (account_id, day, with_data, recording_date))
        store_rows(cur, table, rows)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def fetch_breakdown(account_id, day, fields, breakdowns):
    """Run one async insights report for an account/day/breakdown set.

    Returns the list of raw insight objects. Falls back to a synchronous call if
    the async job never starts within the poll timeout.
    """
    params = {
        "level": "ad",
        "time_range": {"since": day, "until": day},
        "action_attribution_windows": action_attribution_windows,
        "fields": fields,
        "breakdowns": breakdowns,
    }

    async_job = AdAccount("act_" + account_id).get_insights(params=params, is_async=True)
    async_job.api_get()
    percent = AdReportRun.Field.async_percent_completion
    status = AdReportRun.Field.async_status

    counter = 0
    poll_deadline = time.time() + POLL_TIMEOUT_SECONDS
    while (counter < 100) and ((async_job[percent] < 100) or (async_job[status] != "Job Completed")):
        if time.time() > poll_deadline:
            counter = 100  # trigger synchronous fallback
            break
        time.sleep(1)
        async_job.api_get()
        if (async_job[percent] == 0) and (async_job[status] == "Job Not Started"):
            counter += 1
    time.sleep(1)

    if counter >= 100:
        sync_job = AdAccount("act_" + account_id).get_insights(fields=fields, params=params, is_async=False)
        return list(sync_job)
    return list(async_job.get_result())


def run_pass(pass_name, fields, breakdowns, table, table_log, day, refetch_cutoff, id_list, cur, connection):
    """Process one breakdown pass (demographic or placement) for a single day.

    Returns the number of (account) tuples that failed permanently after retries.
    """
    print(f"[START] {pass_name} {day}", flush=True)

    if day >= refetch_cutoff:
        # Within attribution window — re-fetch unconditionally. No day-scope delete
        # here; deletion is deferred to replace_account_day() after a good fetch.
        accounts = list(id_list)
    else:
        cur.execute(
            f"SELECT account_id FROM {table_log} WHERE date = %s::date AND account_id = ANY(%s)",
            (day, list(id_list)),
        )
        already = {row[0] for row in cur.fetchall()}
        accounts = list(set(id_list) - already)

    retries = {}
    failures = 0

    for account_id in accounts:
        try:
            throttle = check_limit(account_id, access_token, graph_api_version)
            if rate_limit_exceeded(throttle):
                attempts = retries.get(account_id, 0)
                if attempts < MAX_RETRIES:
                    retries[account_id] = attempts + 1
                    print(f"[WARN] Throttle high for {account_id} {day} {pass_name}, sleep 30s "
                          f"(attempt {attempts + 1}/{MAX_RETRIES})", flush=True)
                    time.sleep(30)
                    accounts.append(account_id)
                else:
                    print(f"[ERROR] Throttle persistent for {account_id} {day} {pass_name} — giving up", flush=True)
                    failures += 1
                continue

            data = fetch_breakdown(account_id, day, fields, breakdowns)

            # Fetch succeeded — prepare rows, then hand off to the atomic replace.
            recording_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            with_data = len(data) > 0
            if with_data:
                rows = [obj.export_all_data() for obj in data]
                for row in rows:
                    modify_object(row)
            else:
                rows = []

            replace_account_day(cur, connection, table, table_log, account_id, day, rows, recording_date, with_data)
            print(f"[INFO] {pass_name} account={account_id} {day} rows={len(rows)}", flush=True)

        except FacebookRequestError as error:
            if is_token_error(error):
                print("[CRITICAL] Token expired during processing. Halting.", flush=True)
                sys.exit(2)
            if is_rate_limit_error(error):
                attempts = retries.get(account_id, 0)
                if attempts < MAX_RETRIES:
                    retries[account_id] = attempts + 1
                    print(f"[WARN] Rate-limit error for {account_id} {day} {pass_name}, sleep 20s "
                          f"(attempt {attempts + 1}/{MAX_RETRIES})", flush=True)
                    time.sleep(20)
                    accounts.append(account_id)
                else:
                    print(f"[ERROR] Rate-limit persistent for {account_id} {day} {pass_name} — giving up", flush=True)
                    failures += 1
            else:
                code = error._body["error"]["code"]
                print(f"[WARN] FacebookRequestError for {account_id} {day} {pass_name}: code={code}", flush=True)
        except (TypeError, ValueError, FacebookBadObjectError) as error:
            print(f"[WARN] SDK parse error for {account_id} {day} {pass_name}: {error} — skipping", flush=True)
        except (ConnectionError, OSError) as error:
            attempts = retries.get(account_id, 0)
            if attempts < MAX_RETRIES:
                retries[account_id] = attempts + 1
                print(f"[WARN] Network error for {account_id} {day} {pass_name}: {error} — sleep 20s "
                      f"(attempt {attempts + 1}/{MAX_RETRIES})", flush=True)
                time.sleep(20)
                accounts.append(account_id)
            else:
                print(f"[ERROR] Network error persistent for {account_id} {day} {pass_name} — giving up", flush=True)
                failures += 1

    print(f"[END] {pass_name} {day}", flush=True)
    return failures


def main():
    print(f"[START] insights_breakdowns_update\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    try:
        FacebookAdsApi.init(access_token=access_token, api_version="v" + graph_api_version)
    except FacebookRequestError as e:
        code = e._body["error"]["code"]
        print(f"[ERROR] FB API init failed (code={code}): {e}", flush=True)
        sys.exit(1)

    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=DELTA_DAYS)).strftime("%Y-%m-%d")
    refetch_cutoff = (datetime.utcnow() - timedelta(days=REFETCH_DAYS)).strftime("%Y-%m-%d")

    # Date range override (backfill batches).
    start_date = os.environ.get("INSIGHTS_START_DATE", start_date)
    yesterday = os.environ.get("INSIGHTS_END_DATE", yesterday)

    cur, connection = connection_to_database(host, port, database, user, password)
    ensure_breakdown_tables(cur, connection)

    me = User(fbid="me")
    id_list = list(me.get_ad_accounts())
    id_list = [x.export_all_data()["account_id"] for x in id_list]
    id_list = tuple(get_allowed_account_ids(id_list))

    days = sorted(list_days(start_date, yesterday))
    print(f"start uploading {len(days)} day(s)\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    persistent_failures = 0
    for day in days:
        persistent_failures += run_pass(
            "insights_breakdowns_demographic", fields_base, breakdowns_demographic,
            table_demographic, table_log_demographic, day, refetch_cutoff, id_list, cur, connection,
        )
        persistent_failures += run_pass(
            "insights_breakdowns_placement", fields_base, breakdowns_placement,
            table_placement, table_log_placement, day, refetch_cutoff, id_list, cur, connection,
        )

    # Optional materialized-view refresh — only if the project defines them.
    if os.environ.get("REFRESH_BREAKDOWN_MVS", "").lower() in ("1", "true", "yes"):
        try:
            cur.execute("REFRESH MATERIALIZED VIEW mv_bd_demo;")
            cur.execute("REFRESH MATERIALIZED VIEW mv_bd_plac;")
            connection.commit()
            print("[INFO] Materialized views refreshed", flush=True)
        except Exception as e:
            connection.rollback()
            print(f"[ERROR] Materialized view refresh failed: {e}", flush=True)
            sys.exit(1)

    print(f"[END] insights_breakdowns_update\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    if persistent_failures > 0:
        print(f"[ERROR] {persistent_failures} (account, day, pass) tuples failed permanently — exiting non-zero", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
