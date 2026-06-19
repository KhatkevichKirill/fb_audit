"""
insights — per-ad daily insights loader (no breakdowns), env-overridable.

Same shape as insights_update.ipynb but as a standalone, .env-driven script so it
can be driven in date-range batches by scripts/backfill/backfill_insights.sh.

Incremental: an (account, day) already recorded in insights_log is skipped. The
date range defaults to the last 30 days and can be overridden with
INSIGHTS_START_DATE / INSIGHTS_END_DATE.

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
from facebook_business.exceptions import FacebookRequestError

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

DELTA_DAYS = 30
POLL_TIMEOUT_SECONDS = 600

action_attribution_windows = ["1d_view", "1d_click", "7d_click", "28d_click"]

fields = [
    "account_id", "campaign_id", "adset_id", "ad_id", "date_start", "date_stop",
    "impressions", "reach", "clicks", "spend", "unique_inline_link_clicks",
    "inline_link_clicks", "inline_post_engagement", "estimated_ad_recallers",
    "estimated_ad_recall_rate", "objective", "unique_clicks", "actions",
    "action_values", "outbound_clicks", "unique_actions", "unique_outbound_clicks",
    "video_p25_watched_actions", "video_p50_watched_actions",
    "video_p75_watched_actions", "video_p95_watched_actions",
    "results", "cost_per_result",
]

table = "insights"
table_log = "insights_log"


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


def store_rows(cur, table_name, rows):
    """Schema-safe multi-row insert: drops any key that isn't a real column."""
    if not rows:
        return
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table_name,),
    )
    valid = {r[0] for r in cur.fetchall()}
    for row in rows:
        filtered = {k: v for k, v in row.items() if k in valid}
        if not filtered:
            continue
        cols = ",".join(filtered.keys())
        vals = ",".join(map(lambda x: f"%({x})s", filtered.keys()))
        cur.execute(f"INSERT INTO {table_name} ({cols}) VALUES ({vals})", filtered)


def ensure_tables(cur, connection):
    """Create insights_log + a baseline insights table if they don't exist.

    IF NOT EXISTS means a richer pre-existing schema (e.g. from the main pipeline)
    is left untouched; inserts are schema-safe regardless.
    """
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table_log} (
            account_id VARCHAR(50), date DATE, with_data BOOLEAN,
            recording_date TIMESTAMP WITHOUT TIME ZONE,
            PRIMARY KEY (account_id, date)
        )"""
    )
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table} (
            account_id VARCHAR(50), campaign_id VARCHAR(50), adset_id VARCHAR(50),
            ad_id VARCHAR(50), date_start DATE, date_stop DATE,
            impressions TEXT, reach TEXT, clicks TEXT, spend TEXT,
            unique_inline_link_clicks TEXT, inline_link_clicks TEXT,
            inline_post_engagement TEXT, estimated_ad_recallers TEXT,
            estimated_ad_recall_rate TEXT, objective TEXT, unique_clicks TEXT,
            actions JSONB, action_values JSONB, outbound_clicks JSONB,
            unique_actions JSONB, unique_outbound_clicks JSONB,
            video_p25_watched_actions JSONB, video_p50_watched_actions JSONB,
            video_p75_watched_actions JSONB, video_p95_watched_actions JSONB,
            results JSONB, cost_per_result JSONB,
            PRIMARY KEY (account_id, campaign_id, adset_id, ad_id, date_start),
            FOREIGN KEY (account_id, date_start)
                REFERENCES {table_log}(account_id, date) ON DELETE CASCADE
        )"""
    )
    connection.commit()


def fetch_insights(account_id, day):
    params = {
        "level": "ad",
        "time_range": {"since": day, "until": day},
        "action_attribution_windows": action_attribution_windows,
        "fields": fields,
    }
    async_job = AdAccount("act_" + account_id).get_insights(params=params, is_async=True)
    async_job.api_get()
    percent = AdReportRun.Field.async_percent_completion
    status = AdReportRun.Field.async_status

    counter = 0
    poll_deadline = time.time() + POLL_TIMEOUT_SECONDS
    while (counter < 100) and ((async_job[percent] < 100) or (async_job[status] != "Job Completed")):
        if time.time() > poll_deadline:
            counter = 100
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


def main():
    print(f"[START] insights\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    try:
        FacebookAdsApi.init(access_token=access_token, api_version="v" + graph_api_version)
    except FacebookRequestError as e:
        code = e._body["error"]["code"]
        print(f"[ERROR] FB API init failed (code={code}): {e}", flush=True)
        sys.exit(1)

    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=DELTA_DAYS)).strftime("%Y-%m-%d")
    start_date = os.environ.get("INSIGHTS_START_DATE", start_date)
    yesterday = os.environ.get("INSIGHTS_END_DATE", yesterday)

    cur, connection = connection_to_database(host, port, database, user, password)
    ensure_tables(cur, connection)

    me = User(fbid="me")
    id_list = list(me.get_ad_accounts())
    id_list = [x.export_all_data()["account_id"] for x in id_list]
    id_list = tuple(get_allowed_account_ids(id_list))

    days = sorted(list_days(start_date, yesterday))
    print(f"start uploading {len(days)} day(s)\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    for day in days:
        cur.execute(
            f"SELECT account_id FROM {table_log} WHERE date = %s::date AND account_id = ANY(%s)",
            (day, list(id_list)),
        )
        already = {row[0] for row in cur.fetchall()}
        accounts = list(set(id_list) - already)
        retries = {}

        for account_id in accounts:
            try:
                throttle = check_limit(account_id, access_token, graph_api_version)
                if rate_limit_exceeded(throttle):
                    attempts = retries.get(account_id, 0)
                    if attempts < MAX_RETRIES:
                        retries[account_id] = attempts + 1
                        print(f"[WARN] Throttle high for {account_id} {day}, sleep 30s "
                              f"(attempt {attempts + 1}/{MAX_RETRIES})", flush=True)
                        time.sleep(30)
                        accounts.append(account_id)
                    else:
                        print(f"[WARN] Throttle persistent for {account_id} {day} — skipping", flush=True)
                    continue

                data = fetch_insights(account_id, day)

                recording_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                with_data = len(data) > 0
                cur.execute(
                    f"INSERT INTO {table_log} VALUES (%s, %s, %s, %s)",
                    (account_id, day, with_data, recording_date),
                )
                if with_data:
                    rows = [obj.export_all_data() for obj in data]
                    for row in rows:
                        modify_object(row)
                    store_rows(cur, table, rows)
                connection.commit()
                print(f"[INFO] insights account={account_id} {day} rows={len(data)}", flush=True)

            except FacebookRequestError as error:
                if is_token_error(error):
                    print("[CRITICAL] Token expired during processing. Halting.", flush=True)
                    sys.exit(2)
                if is_rate_limit_error(error):
                    print(f"[WARN] Rate-limit error for {account_id} {day}, sleeping 20s", flush=True)
                    time.sleep(20)
                    accounts.append(account_id)
                else:
                    code = error._body["error"]["code"]
                    print(f"[WARN] FacebookRequestError for {account_id} {day}: code={code}", flush=True)
                connection.rollback()

    print(f"[END] insights\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)


if __name__ == "__main__":
    main()
